package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"go-orchestrator/llm"
)

var (
	ErrNoWorkersAvailable     = errors.New("no worker nodes available matching task requirements")
	ErrContextLengthExceeded = errors.New("context length exceeded worker maximum capacity (413)")
)

// WorkerNode represents an inference worker node (local XDNA 2 NPU, local iGPU, or remote GPU like a 9070XT).
type WorkerNode struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	URL         string    `json:"url"`
	Type        string    `json:"type"`        // "npu", "gpu", "cpu", "remote"
	MaxContext  int       `json:"max_context"` // 8192 for NPU, 32768 for remote GPU
	Concurrency int       `json:"concurrency"` // 1 for NPU serial queue, 4 for 9070XT
	ActiveJobs  int       `json:"active_jobs"`
	TotalJobs   int64     `json:"total_jobs"`
	TotalTokens int64     `json:"total_tokens"`
	Status      string    `json:"status"` // "online", "busy", "offline"
	Tags        []string  `json:"tags"`
	LastSeen    time.Time `json:"last_seen"`
	client      llm.LLMClient
}

// SetClientForTest injects a test client (e.g. mock).
func (w *WorkerNode) SetClientForTest(client llm.LLMClient) {
	w.client = client
}

// WorkerRouter manages a dynamic fleet of leaf worker models, providing
// intelligent load balancing, context-size-aware lane routing, and failover.
type WorkerRouter struct {
	mu           sync.RWMutex
	workers      map[string]*WorkerNode
	defaultMode  llm.Mode
	httpClient   *http.Client
	log          *slog.Logger
}

// NewWorkerRouter constructs a new WorkerRouter instance.
func NewWorkerRouter(defaultMode llm.Mode, defaultWorkers ...WorkerNode) *WorkerRouter {
	r := &WorkerRouter{
		workers:     make(map[string]*WorkerNode),
		defaultMode: defaultMode,
		httpClient:  &http.Client{Timeout: 120 * time.Second},
		log:         slog.Default(),
	}

	for _, w := range defaultWorkers {
		_ = r.RegisterWorker(w)
	}
	return r
}

// RegisterWorker registers or updates a worker node in the routing table.
func (r *WorkerRouter) RegisterWorker(node WorkerNode) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if node.ID == "" {
		node.ID = fmt.Sprintf("worker-%d", time.Now().UnixNano())
	}
	if node.MaxContext <= 0 {
		node.MaxContext = 8192
	}
	if node.Concurrency <= 0 {
		node.Concurrency = 1
	}
	if node.Status == "" {
		node.Status = "online"
	}
	node.LastSeen = time.Now()

	// Initialize LLM client for this worker
	if node.client == nil {
		node.client = llm.NewClient(node.URL, llm.WithMode(r.defaultMode))
	}

	r.workers[node.ID] = &node
	r.log.Info("registered worker node in routing table",
		"id", node.ID, "name", node.Name, "url", node.URL,
		"type", node.Type, "max_context", node.MaxContext, "concurrency", node.Concurrency)
	return nil
}

// UnregisterWorker removes a worker node from the routing table.
func (r *WorkerRouter) UnregisterWorker(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.workers[id]; !exists {
		return fmt.Errorf("worker %q not found", id)
	}
	delete(r.workers, id)
	r.log.Info("unregistered worker node", "id", id)
	return nil
}

// Heartbeat refreshes the last-seen timestamp and status of a worker.
func (r *WorkerRouter) Heartbeat(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	node, exists := r.workers[id]
	if !exists {
		return fmt.Errorf("worker %q not found", id)
	}
	node.LastSeen = time.Now()
	node.Status = "online"
	return nil
}

// ListWorkers returns a current snapshot of all registered workers.
func (r *WorkerRouter) ListWorkers() []WorkerNode {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]WorkerNode, 0, len(r.workers))
	for _, w := range r.workers {
		cp := *w
		cp.client = nil // Don't serialize interface
		list = append(list, cp)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].ID < list[j].ID
	})
	return list
}

// SelectWorker picks the optimal worker node for a given prompt length and tags.
func (r *WorkerRouter) SelectWorker(estimatedTokens int, requiredTags ...string) (*WorkerNode, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	var candidates []*WorkerNode

	for _, w := range r.workers {
		// Filter out stale workers (no heartbeat for > 5 minutes)
		if now.Sub(w.LastSeen) > 5*time.Minute && w.Type == "remote" {
			w.Status = "offline"
			continue
		}

		// Context capacity check
		if estimatedTokens > w.MaxContext {
			continue
		}

		// Tag matching check
		if len(requiredTags) > 0 && !hasAllTags(w.Tags, requiredTags) {
			continue
		}

		candidates = append(candidates, w)
	}

	if len(candidates) == 0 {
		return nil, ErrNoWorkersAvailable
	}

	// Priority heuristics:
	// 1. Prefer workers with available concurrency headroom over saturated ("busy") workers
	// 2. If small prompt (<= 8192) and NPU has capacity, prefer NPU (energy & bandwidth efficient)
	// 3. Otherwise prefer worker with lowest active jobs / least utilized
	sort.Slice(candidates, func(i, j int) bool {
		cA, cB := candidates[i], candidates[j]
		availA := cA.ActiveJobs < cA.Concurrency
		availB := cB.ActiveJobs < cB.Concurrency
		if availA != availB {
			return availA
		}
		if estimatedTokens <= 8192 {
			if cA.Type == "npu" && cB.Type != "npu" {
				return true
			}
			if cB.Type == "npu" && cA.Type != "npu" {
				return false
			}
		}
		// Least loaded
		loadA := float64(cA.ActiveJobs) / float64(cA.Concurrency)
		loadB := float64(cB.ActiveJobs) / float64(cB.Concurrency)
		return loadA < loadB
	})

	selected := candidates[0]
	selected.ActiveJobs++
	selected.TotalJobs++
	if selected.ActiveJobs >= selected.Concurrency {
		selected.Status = "busy"
	}
	return selected, nil
}

func (r *WorkerRouter) releaseWorker(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if node, exists := r.workers[id]; exists {
		node.ActiveJobs = max(0, node.ActiveJobs-1)
		if node.ActiveJobs < node.Concurrency {
			node.Status = "online"
		}
	}
}

func hasAllTags(available []string, required []string) bool {
	tagMap := make(map[string]bool, len(available))
	for _, t := range available {
		tagMap[t] = true
	}
	for _, req := range required {
		if !tagMap[req] {
			return false
		}
	}
	return true
}

func estimateTokens(prompt string) int {
	// Conservative estimate: ~3.5 chars per token for code/text
	return max(1, len(prompt)/3)
}

// ----------------------------------------------------------------------------
// LLMClient Implementation: Transparent routing for Tasks & Orchestrator
// ----------------------------------------------------------------------------

func (r *WorkerRouter) Complete(ctx context.Context, req llm.CompletionRequest) (llm.CompletionResponse, error) {
	// Calculate prompt length across messages
	var sb strings.Builder
	for _, m := range req.Messages {
		sb.WriteString(m.Content)
	}
	totalTokens := estimateTokens(sb.String())

	worker, err := r.SelectWorker(totalTokens)
	if err != nil {
		return llm.CompletionResponse{}, err
	}
	defer r.releaseWorker(worker.ID)

	r.log.Info("routing completion request to worker node",
		"worker_id", worker.ID, "type", worker.Type, "est_tokens", totalTokens)

	resp, err := worker.client.Complete(ctx, req)
	if err != nil {
		if strings.Contains(err.Error(), "413") || strings.Contains(err.Error(), "context_length_exceeded") {
			r.log.Warn("worker node rejected prompt: context length exceeded", "worker_id", worker.ID)
			return llm.CompletionResponse{}, ErrContextLengthExceeded
		}
		return llm.CompletionResponse{}, err
	}
	tokensUsed := int64(resp.Usage.TotalTokens)
	if tokensUsed <= 0 {
		tokensUsed = int64(totalTokens + estimateTokens(resp.Content))
	}
	r.mu.Lock()
	if node, exists := r.workers[worker.ID]; exists {
		node.TotalTokens += tokensUsed
	}
	r.mu.Unlock()
	return resp, nil
}

// DirectExecute sends a structured WorkerExecuteRequest directly to the worker's /execute endpoint.
func (r *WorkerRouter) DirectExecute(ctx context.Context, req map[string]any) (map[string]any, error) {
	promptStr, _ := req["prompt"].(string)
	if promptStr == "" {
		instr, _ := req["instruction"].(string)
		ctxStr, _ := req["context"].(string)
		promptStr = ctxStr + "\n" + instr
	}
	totalTokens := estimateTokens(promptStr)

	worker, err := r.SelectWorker(totalTokens)
	if err != nil {
		return nil, err
	}
	defer r.releaseWorker(worker.ID)

	payloadBytes, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	execURL := strings.TrimRight(worker.URL, "/") + "/execute"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, execURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 413 || resp.StatusCode == http.StatusRequestEntityTooLarge {
		return nil, ErrContextLengthExceeded
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("worker returned HTTP %d", resp.StatusCode)
	}

	var res map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return res, nil
}
