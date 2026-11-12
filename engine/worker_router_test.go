package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"go-orchestrator/llm"
)

func TestWorkerRouter_RegistrationAndList(t *testing.T) {
	router := NewWorkerRouter(llm.ModeMock)

	npu := WorkerNode{
		ID:          "npu-local",
		Name:        "AMD XDNA 2 NPU",
		URL:         "http://localhost:8001",
		Type:        "npu",
		MaxContext:  8192,
		Concurrency: 1,
		Tags:        []string{"npu", "local"},
	}

	gpu := WorkerNode{
		ID:          "desktop-9070xt",
		Name:        "Radeon RX 9070XT Worker",
		URL:         "http://192.168.50.100:8000",
		Type:        "gpu",
		MaxContext:  32768,
		Concurrency: 4,
		Tags:        []string{"gpu", "remote", "high-context"},
	}

	if err := router.RegisterWorker(npu); err != nil {
		t.Fatalf("failed to register npu: %v", err)
	}
	if err := router.RegisterWorker(gpu); err != nil {
		t.Fatalf("failed to register gpu: %v", err)
	}

	workers := router.ListWorkers()
	if len(workers) != 2 {
		t.Fatalf("expected 2 workers, got %d", len(workers))
	}

	if err := router.Heartbeat("npu-local"); err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	if err := router.UnregisterWorker("desktop-9070xt"); err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	workers = router.ListWorkers()
	if len(workers) != 1 || workers[0].ID != "npu-local" {
		t.Fatalf("expected 1 remaining worker (npu-local), got %v", workers)
	}
}

func TestWorkerRouter_ContextSizeRouting(t *testing.T) {
	router := NewWorkerRouter(llm.ModeMock)

	_ = router.RegisterWorker(WorkerNode{
		ID:          "npu-local",
		Name:        "AMD NPU",
		URL:         "http://localhost:8001",
		Type:        "npu",
		MaxContext:  8192,
		Concurrency: 1,
	})

	_ = router.RegisterWorker(WorkerNode{
		ID:          "desktop-9070xt",
		Name:        "Radeon 9070XT",
		URL:         "http://192.168.50.100:8000",
		Type:        "gpu",
		MaxContext:  32768,
		Concurrency: 4,
	})

	// 1. Small request (e.g. 500 chars -> ~166 tokens): should select NPU
	smallNode, err := router.SelectWorker(500)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if smallNode.ID != "npu-local" {
		t.Errorf("expected npu-local for small request, got %s", smallNode.ID)
	}
	router.releaseWorker(smallNode.ID)

	// 2. Large request exceeding 8192 (e.g. 15,000 tokens): should bypass NPU and route to 9070XT
	largeNode, err := router.SelectWorker(15000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if largeNode.ID != "desktop-9070xt" {
		t.Errorf("expected desktop-9070xt for large request, got %s", largeNode.ID)
	}
	router.releaseWorker(largeNode.ID)

	// 3. Monstrous request exceeding all workers (e.g. 50,000 tokens): should return ErrNoWorkersAvailable
	_, err = router.SelectWorker(50000)
	if err != ErrNoWorkersAvailable {
		t.Errorf("expected ErrNoWorkersAvailable for 50k tokens, got %v", err)
	}
}

func TestWorkerRouter_CompleteTransparentRouting(t *testing.T) {
	mockClient := llm.NewMockClient("mock-qwen", 0)
	router := NewWorkerRouter(llm.ModeMock)

	node := WorkerNode{
		ID:          "npu-local",
		Name:        "AMD NPU",
		URL:         "http://localhost:8001",
		Type:        "npu",
		MaxContext:  8192,
		Concurrency: 1,
	}
	_ = router.RegisterWorker(node)
	// Inject mock client directly for deterministic unit testing
	router.workers["npu-local"].SetClientForTest(mockClient)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := router.Complete(ctx, llm.CompletionRequest{
		Messages: []llm.Message{
			{Role: llm.RoleUser, Content: "Hello worker router"},
		},
	})
	if err != nil {
		t.Fatalf("Complete failed: %v", err)
	}
	if !strings.Contains(resp.Content, "mock") && resp.Content == "" {
		t.Errorf("expected non-empty completion response")
	}
}

