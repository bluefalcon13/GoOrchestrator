package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/model/openaimodel"
	"google.golang.org/adk/v2/server/adkrest"
	"google.golang.org/adk/v2/session"

	orchagent "go-orchestrator/agent"
	"go-orchestrator/engine"
	"go-orchestrator/llm"
	"go-orchestrator/rag"
	"go-orchestrator/tasks"
)

func main() {
	port := flag.String("port", "8080", "HTTP server port")
	lrmURL := flag.String("lrm-url", "http://localhost:8000", "Base URL for Large Reasoning Model (vLLM Gemma 4 26B)")
	workerURL := flag.String("worker-url", "http://localhost:8001", "Base URL for Leaf Worker (NPU Qwen 7B)")
	embedURL := flag.String("embed-url", "http://llama-embed:8080", "Base URL for Llama.cpp Embedding backend")
	rerankURL := flag.String("rerank-url", "http://llama-rerank:8080", "Base URL for Llama.cpp Reranker backend")
	qdrantURL := flag.String("qdrant-url", "http://qdrant:6333", "Base URL for Qdrant Vector Database")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("[ADK Orchestrator] Initializing local AI stack on port %s...", *port)

	// 1. Model Backend via ADK OpenAI Connector (Points to vLLM on :8000)
	lrmModel, err := openaimodel.NewModel(ctx, "default", &openaimodel.ClientConfig{
		BaseURL: *lrmURL + "/v1",
		API:     openaimodel.APIChatCompletions,
	})
	if err != nil {
		log.Fatalf("Failed to initialize ADK LRM model: %v", err)
	}

	// 2. Worker Router (Local XDNA 2 NPU + Dynamic Remote Workers)
	workerRouter := engine.NewWorkerRouter(llm.ModeHTTP, engine.WorkerNode{
		ID:          "npu-local",
		Name:        "AMD XDNA 2 NPU (Ryzen AI)",
		URL:         *workerURL,
		Type:        "npu",
		MaxContext:  8192,
		Concurrency: 1,
		Tags:        []string{"npu", "local", "fast"},
	})

	// 3. RAG Client (Zen 5 CPU Cluster)
	ragClient := rag.NewHTTPRAGClient(*embedURL, *rerankURL, *qdrantURL)

	// 4. Task Registry & Execution Engine (Kahn DAG Scheduler)
	registry := tasks.NewRegistry()
	codeGenH := tasks.NewCodeGenHandler(workerRouter)
	textExtractH := tasks.NewTextExtractHandler(workerRouter, ragClient)
	ragIndexH := tasks.NewRAGIndexHandler(ragClient)
	ragSearchH := tasks.NewRAGSearchHandler(ragClient)

	// Core handlers
	registry.Register("code_gen", codeGenH)
	registry.Register("code", codeGenH)
	registry.Register("codegen", codeGenH)
	registry.Register("code_generation", codeGenH)

	registry.Register("text_extract", textExtractH)
	registry.Register("extract", textExtractH)
	registry.Register("text", textExtractH)
	registry.Register("report", textExtractH)

	registry.Register("rag_index", ragIndexH)
	registry.Register("ingest", ragIndexH)
	registry.Register("index", ragIndexH)
	registry.Register("rag_ingest", ragIndexH)

	registry.Register("rag_search", ragSearchH)
	registry.Register("rag_query", ragSearchH)
	registry.Register("rag", ragSearchH)
	registry.Register("query", ragSearchH)
	registry.Register("search", ragSearchH)

	lrmClient := llm.NewClient(*lrmURL, llm.WithMode(llm.ModeHTTP))
	lrm := llm.NewLRMPersonaEngine(lrmClient)

	eventCh := make(chan any, 256)
	orch := engine.NewOrchestrator(lrm, workerRouter, registry, eventCh)

	go func() {
		if err := orch.RunEventLoop(ctx, nil); err != nil {
			log.Printf("[Engine] Event loop ended: %v", err)
		}
	}()

	// 5. ADK Tools & Agent
	oCtx := orchagent.OrchestratorContext{
		EventCh:      eventCh,
		WorkerRouter: workerRouter,
		RAGClient:    ragClient,
	}
	tools, err := orchagent.BuildTools(oCtx)
	if err != nil {
		log.Fatalf("Failed to build ADK tools: %v", err)
	}

	rootAgent, err := orchagent.NewOrchestratorAgent(lrmModel, tools)
	if err != nil {
		log.Fatalf("Failed to create root ADK agent: %v", err)
	}

	// 6. ADK REST & Streaming SSE Server (Replaces ~600 lines of custom API server)
	restServer, err := adkrest.NewServer(adkrest.ServerConfig{
		AgentLoader:     agent.NewSingleLoader(rootAgent),
		SessionService:  session.InMemoryService(),
		SSEWriteTimeout: 120 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create ADK REST server: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/api/", http.StripPrefix("/api", restServer))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","framework":"google-adk-v2"}`))
	})

	// MCP SSE Server for VS Code Continue
	mcpServer := orchagent.NewMCPServer(oCtx)
	mcpServer.RegisterRoutes(mux)

	// Worker fleet REST endpoints
	mux.HandleFunc("GET /api/v1/workers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(workerRouter.ListWorkers())
	})
	mux.HandleFunc("POST /api/v1/workers", func(w http.ResponseWriter, r *http.Request) {
		var node engine.WorkerNode
		if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := workerRouter.RegisterWorker(node); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(node)
	})

	server := &http.Server{
		Addr:              ":" + *port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		log.Println("[Main] Shutting down...")
		cancel()
		_ = server.Shutdown(context.Background())
	}()

	log.Printf("[ADK Orchestrator] Ready. Endpoints: /api/ (ADK REST/SSE), /healthz")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}
