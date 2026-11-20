package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"go-orchestrator/llm"
)

// MCPServer exposes the orchestrator's tools over the standard MCP SSE protocol
// for VS Code extensions (Cline / Continue), delegating all execution to the
// shared OrchestratorContext methods in tools.go.
type MCPServer struct {
	oCtx     OrchestratorContext
	mu       sync.RWMutex
	sessions map[string]chan string
}

// NewMCPServer creates a new MCP SSE server.
func NewMCPServer(oCtx OrchestratorContext) *MCPServer {
	return &MCPServer{
		oCtx:     oCtx,
		sessions: make(map[string]chan string),
	}
}

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id,omitempty"`
	Result  any    `json:"result,omitempty"`
	Error   any    `json:"error,omitempty"`
}

func (s *MCPServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/mcp/sse", s.handleSSE)
	mux.HandleFunc("/mcp/messages", s.handleMessages)
}

func (s *MCPServer) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE streaming unsupported", http.StatusInternalServerError)
		return
	}

	b := make([]byte, 16)
	_, _ = rand.Read(b)
	sessionID := hex.EncodeToString(b)

	ch := make(chan string, 64)
	s.mu.Lock()
	s.sessions[sessionID] = ch
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.sessions, sessionID)
		s.mu.Unlock()
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	endpointURI := fmt.Sprintf("/mcp/messages?sessionId=%s", sessionID)
	fmt.Fprintf(w, "event: endpoint\ndata: %s\n\n", endpointURI)
	flusher.Flush()
	log.Printf("[MCP] Session started: %s -> %s", sessionID, endpointURI)

	for {
		select {
		case <-r.Context().Done():
			log.Printf("[MCP] Session closed: %s", sessionID)
			return
		case msg, open := <-ch:
			if !open {
				return
			}
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

func (s *MCPServer) handleMessages(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	sessionID := r.URL.Query().Get("sessionId")
	s.mu.RLock()
	ch, exists := s.sessions[sessionID]
	s.mu.RUnlock()

	if !exists {
		http.Error(w, fmt.Sprintf("session %q not found", sessionID), http.StatusNotFound)
		return
	}

	var req jsonRPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[MCP] Error decoding JSON-RPC request: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("[MCP] Request: method=%s, id=%v, session=%s", req.Method, req.ID, sessionID)
	w.WriteHeader(http.StatusAccepted)

	go func() {
		resp := s.dispatch(context.Background(), req)
		if resp == nil {
			return
		}
		data, err := json.Marshal(resp)
		if err != nil {
			return
		}
		select {
		case ch <- string(data):
		case <-time.After(5 * time.Second):
			log.Printf("[MCP] Timeout sending response to session %s", sessionID)
		}
	}()
}

func (s *MCPServer) dispatch(ctx context.Context, req jsonRPCRequest) *jsonRPCResponse {
	switch req.Method {
	case "initialize":
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"protocolVersion": "2024-11-05",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "strix-orchestrator", "version": "2.0.0"},
			},
		}
	case "notifications/initialized":
		return nil
	case "ping":
		return &jsonRPCResponse{JSONRPC: "2.0", ID: req.ID, Result: map[string]any{}}
	case "tools/list":
		return &jsonRPCResponse{JSONRPC: "2.0", ID: req.ID, Result: map[string]any{"tools": s.toolDefinitions()}}
	case "tools/call":
		return s.handleToolCall(ctx, req)
	default:
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   map[string]any{"code": -32601, "message": fmt.Sprintf("method %q not found", req.Method)},
		}
	}
}

func (s *MCPServer) toolDefinitions() []map[string]any {
	return []map[string]any{
		{
			"name":        "strix_orchestrator_draft_plan",
			"description": "Submit and validate a proposed DAG plan without executing it. Verifies acyclicity and registers it as a draft awaiting human approval.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"title":     map[string]any{"type": "string", "description": "Descriptive title of the plan"},
					"objective": map[string]any{"type": "string", "description": "Directive describing what to build or investigate"},
					"epics": map[string]any{
						"type":        "array",
						"description": "List of Epics, Stories, and Tasks forming an acyclic DAG",
						"items": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"id":    map[string]any{"type": "string", "description": "Epic ID, e.g. EPIC_001"},
								"title": map[string]any{"type": "string", "description": "Epic title"},
								"stories": map[string]any{
									"type": "array",
									"items": map[string]any{
										"type": "object",
										"properties": map[string]any{
											"id":    map[string]any{"type": "string", "description": "Story ID, e.g. STORY_001"},
											"title": map[string]any{"type": "string", "description": "Story title"},
											"tasks": map[string]any{
												"type": "array",
												"items": map[string]any{
													"type": "object",
													"properties": map[string]any{
														"id":          map[string]any{"type": "string", "description": "Unique task ID (REQUIRED), e.g. TASK_001"},
														"title":       map[string]any{"type": "string", "description": "Task title"},
														"description": map[string]any{"type": "string", "description": "Task description"},
														"task_type":   map[string]any{"type": "string", "description": "Task type: code_gen, text_extract, rag_search, rag_index"},
														"depends_on":  map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Prerequisite task IDs"},
													},
													"required": []string{"id", "title"},
												},
											},
										},
										"required": []string{"id", "title", "tasks"},
									},
								},
							},
							"required": []string{"id", "title", "stories"},
						},
					},
				},
				"required": []string{"title", "objective", "epics"},
			},
		},
		{
			"name":        "strix_orchestrator_approve_plan",
			"description": "Approve a previously drafted plan to move it into active DAG execution across local Ryzen AI NPU.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"plan_id": map[string]any{"type": "string", "description": "Deterministic ID of the draft plan"},
				},
				"required": []string{"plan_id"},
			},
		},
		{
			"name":        "strix_orchestrator_get_execution_status",
			"description": "Inspect live execution progress, NPU worker scores, and retries for a plan.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"plan_id": map[string]any{"type": "string", "description": "Deterministic ID of the plan to inspect"},
				},
				"required": []string{"plan_id"},
			},
		},
		{
			"name":        "strix_orchestrator_list_workers",
			"description": "Inspect registered worker nodes (local NPU and remote GPUs like 9070XT), queue depth, and status.",
			"inputSchema": map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			"name":        "strix_orchestrator_rag_search",
			"description": "Search local documents, code, and logs using dense embeddings and cross-encoder reranking.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query":      map[string]any{"type": "string", "description": "Search query"},
					"collection": map[string]any{"type": "string", "description": "Qdrant collection name (default: default_corpus)"},
					"top_k":      map[string]any{"type": "integer", "description": "Number of results to return (default: 5)"},
				},
				"required": []string{"query"},
			},
		},
		{
			"name":        "strix_orchestrator_ingest_file",
			"description": "Polymorphic RAG indexer: index a file from disk by file_path (e.g. '/data/game.log') or raw text by content into local Qdrant. For large logs and documents, pass file_path to stream line-by-line with zero memory and zero context window bloat.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"file_path":  map[string]any{"type": "string", "description": "Local path to the file on disk (e.g. 'data/game.log', '/data/game.log')"},
					"path":       map[string]any{"type": "string", "description": "Alias for file_path"},
					"content":    map[string]any{"type": "string", "description": "Raw string content (optional; if omitted or pointing to a file, file_path is used)"},
					"collection": map[string]any{"type": "string", "description": "Qdrant collection name (default: default_corpus)"},
					"source":     map[string]any{"type": "string", "description": "Source identifier (default: file path or 'inline_upload')"},
					"chunk_size": map[string]any{"type": "integer", "description": "Character chunk size (default: 1000)"},
				},
			},
		},
	}
}

type toolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

func (s *MCPServer) handleToolCall(ctx context.Context, req jsonRPCRequest) *jsonRPCResponse {
	var params toolCallParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   map[string]any{"code": -32602, "message": "invalid params"},
		}
	}

	name := normalizeMCPToolName(params.Name)
	log.Printf("[MCP] Invoking tool %q (raw: %q) with args: %s", name, params.Name, string(params.Arguments))

	var outputText string
	var err error

	switch name {
	case "draft_plan":
		var planSpec llm.PlanSpec
		planSpec, err = llm.ParsePlanSpec(string(params.Arguments))
		if err == nil {
			outputText, err = s.oCtx.ExecuteDraftPlan(ctx, planSpec)
		}

	case "approve_plan":
		var in ApprovePlanArgs
		if err = json.Unmarshal(params.Arguments, &in); err == nil {
			outputText, err = s.oCtx.ExecuteApprovePlan(ctx, in)
		}

	case "get_execution_status", "get_plan_status":
		var in GetPlanStatusArgs
		if err = json.Unmarshal(params.Arguments, &in); err == nil {
			outputText, err = s.oCtx.ExecuteGetPlanStatus(ctx, in)
		}

	case "list_workers":
		outputText, err = s.oCtx.ExecuteListWorkers()

	case "rag_search":
		var in RAGSearchArgs
		if err = json.Unmarshal(params.Arguments, &in); err == nil {
			outputText, err = s.oCtx.ExecuteRAGSearch(ctx, in)
		}

	case "ingest_file", "index_text":
		var in IngestFileArgs
		if err = json.Unmarshal(params.Arguments, &in); err == nil {
			outputText, err = s.oCtx.ExecuteIngestFile(ctx, in)
		}

	default:
		outputText = fmt.Sprintf("Unknown tool %q", name)
	}

	if err != nil {
		outputText = fmt.Sprintf("Error: %v", err)
	}

	return &jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: map[string]any{
			"content": []map[string]any{
				{"type": "text", "text": outputText},
			},
		},
	}
}

func normalizeMCPToolName(raw string) string {
	// Handles Cline (go-orchestrator__strix_orchestrator_ingest_file), Continue (strix_orchestrator_ingest_file), etc.
	if idx := strings.Index(raw, "strix_orchestrator_"); idx != -1 {
		return raw[idx+len("strix_orchestrator_"):]
	}
	if idx := strings.Index(raw, "orchestrator_"); idx != -1 {
		return raw[idx+len("orchestrator_"):]
	}
	return raw
}
