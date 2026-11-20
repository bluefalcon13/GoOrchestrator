package tasks

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
)

// TaskInput carries the targeted local execution context for an individual leaf task.
type TaskInput struct {
	TaskID          string            `json:"task_id"`
	Title           string            `json:"title"`
	Description     string            `json:"description"`
	TaskType        string            `json:"task_type"`
	Params          json.RawMessage   `json:"params,omitempty"`
	UpstreamOutputs map[string]string `json:"upstream_outputs,omitempty"` // taskID -> output content
	Feedback        []string          `json:"feedback,omitempty"`         // prior reviewer retry feedback
	Clarifications  []string          `json:"clarifications,omitempty"`   // prior user clarifications
	Attempt         int               `json:"attempt"`
}

// TaskOutput is the result of executing a task.
type TaskOutput struct {
	Content   string            `json:"content"`             // primary output string or serialized JSON
	Artifacts map[string]string `json:"artifacts,omitempty"` // optional generated files (path -> content)
	Metadata  map[string]any    `json:"metadata,omitempty"`  // handler-specific metadata
}

// TaskHandler defines the execution contract for domain-specific task types.
type TaskHandler interface {
	Execute(ctx context.Context, input TaskInput) (TaskOutput, error)
	Description() string
}

// Registry is a thread-safe registry of TaskHandler instances.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]TaskHandler
}

// NewRegistry constructs a new TaskHandler registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]TaskHandler),
	}
}

// Register registers a handler for a task type name.
func (r *Registry) Register(taskType string, handler TaskHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[taskType] = handler
}

// Get returns the handler registered for taskType, or falls back to semantic matching and default handler.
func (r *Registry) Get(taskType string) (TaskHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[taskType]
	if ok {
		return h, true
	}

	norm := strings.ToLower(strings.TrimSpace(taskType))
	if h, ok := r.handlers[norm]; ok {
		return h, true
	}

	if strings.Contains(norm, "ingest") || strings.Contains(norm, "index") {
		if h, ok := r.handlers["rag_index"]; ok {
			return h, true
		}
	}
	if strings.Contains(norm, "rag") || strings.Contains(norm, "search") || strings.Contains(norm, "query") {
		if h, ok := r.handlers["rag_search"]; ok {
			return h, true
		}
	}
	if strings.Contains(norm, "extract") || strings.Contains(norm, "text") || strings.Contains(norm, "grep") ||
		strings.Contains(norm, "verif") || strings.Contains(norm, "valid") || strings.Contains(norm, "report") {
		if h, ok := r.handlers["text_extract"]; ok {
			return h, true
		}
	}

	// Default fallback to code_gen (general worker execution)
	if h, ok := r.handlers["code_gen"]; ok {
		return h, true
	}
	return nil, false
}

// List returns a list of registered task type names.
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	keys := make([]string, 0, len(r.handlers))
	for k := range r.handlers {
		keys = append(keys, k)
	}
	return keys
}

// Capabilities returns task types and their descriptions for planning intake.
func (r *Registry) Capabilities() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	caps := make(map[string]string, len(r.handlers))
	for k, h := range r.handlers {
		caps[k] = h.Description()
	}
	return caps
}

