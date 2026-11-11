package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go-orchestrator/api"
	"go-orchestrator/engine"
	"go-orchestrator/llm"
	"go-orchestrator/tasks"
)

func main() {
	port := flag.Int("port", 8080, "HTTP server port for the conversational API")
	blueprintPath := flag.String("blueprint", "", "Path to optional initial bootstrap blueprint JSON file")
	llmMode := flag.String("mode", "mock", "LLM backend mode: 'mock' (offline deterministic) or 'http' (remote /v1/chat/completions)")
	lrmURL := flag.String("lrm-url", "http://localhost:8000", "Base URL for Large Reasoning Model backend")
	workerURL := flag.String("worker-url", "http://localhost:8001", "Base URL for Small Task Model backend")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	parsedMode, err := llm.ParseMode(*llmMode)
	if err != nil {
		log.Fatalf("[Main] Invalid LLM mode: %v", err)
	}

	// Initialize the TWO underlying LLM backends via a unified interface:
	// 1. Large Reasoning Model (LRM) - one model wearing multiple "hats" (system prompts/personas)
	// 2. Small Task Model (Worker) - fast tactical execution model
	lrmClient := llm.NewClient(*lrmURL, llm.WithMode(parsedMode))
	workerClient := llm.NewClient(*workerURL, llm.WithMode(parsedMode))

	// Wrap the single LRM client with multi-hat persona capabilities
	// (Conversational, Planner, ProjectManager, Reviewer)
	lrm := llm.NewLRMPersonaEngine(lrmClient)

	// Register domain-agnostic task handlers powered by the Small Worker Model
	registry := tasks.NewRegistry()
	registry.Register("code_gen", tasks.NewCodeGenHandler(workerClient))
	registry.Register("text_extract", tasks.NewTextExtractHandler(workerClient))

	// Unified concurrent event bus
	eventCh := make(chan any, 256)

	// Map OS signals into engine events
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		for sig := range sigCh {
			eventCh <- engine.SigEvent{Signal: sig}
		}
	}()

	// Start HTTP Conversational Intake API
	srv := api.NewServer(*port, eventCh, lrm)
	go func() {
		if err := srv.Start(); err != nil {
			eventCh <- engine.FatalErrorEvent{Err: err}
		}
	}()

	// Optional bootstrap blueprint injection
	if *blueprintPath != "" {
		data, err := os.ReadFile(*blueprintPath)
		if err != nil {
			log.Fatalf("[Main] Failed to read bootstrap file %s: %v", *blueprintPath, err)
		}
		eventCh <- engine.SeedPlanEvent{RawJSON: string(data)}
	}

	// Ignite the single-writer actor event loop and block until graceful shutdown
	orch := engine.NewOrchestrator(lrm, workerClient, registry, eventCh)
	if err := orch.RunEventLoop(ctx, srv.Shutdown); err != nil {
		log.Fatalf("[Main] Engine exited with error: %v", err)
	}
	log.Println("[Main] Orchestrator shut down gracefully.")
}
