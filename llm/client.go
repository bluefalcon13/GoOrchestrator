// Package llm is the dual-tier model access layer.
//
// It exposes a single transport-agnostic LLMClient abstraction with two
// implementations (an OpenAI-compatible HTTP client and a deterministic,
// offline mock), plus the LRMPersonaEngine, which lets one Large Reasoning
// Model wear several strongly-typed "hats" (Conversational, Planner,
// ProjectManager, Reviewer), each with its own system prompt and structured
// output contract.
package llm

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// Role is the author role of a chat message.
type Role string

// Chat roles understood by OpenAI-compatible backends.
const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
)

// Message is one turn of a chat transcript.
type Message struct {
	Role    Role   `json:"role"`
	Content string `json:"content"`
}

// CompletionRequest is a backend-neutral chat completion request.
type CompletionRequest struct {
	// Model overrides the client's default model when non-empty.
	Model       string
	Messages    []Message
	Temperature float64
	MaxTokens   int
	// JSONMode asks the backend to constrain output to a JSON object.
	JSONMode bool
	// Tag identifies the logical caller (an LRM hat or a worker task handler).
	// It is never sent over the wire; it is used for logging and by the mock
	// backend to select a deterministic behaviour.
	Tag string
	// Metadata carries structured, non-prompt hints about the request (task
	// IDs, flags, raw fields). It is never sent over the wire. Real models
	// read the prompt; the deterministic mock reads Metadata so that it does
	// not have to parse free-form prompt text.
	Metadata map[string]string
}

// Usage reports token accounting for a completion.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// CompletionResponse is a backend-neutral chat completion result.
type CompletionResponse struct {
	Content      string
	Model        string
	FinishReason string
	Usage        Usage
	Latency      time.Duration
}

// LLMClient is the single abstraction every model consumer depends on.
// Implementations must be safe for concurrent use by multiple goroutines.
type LLMClient interface {
	Complete(ctx context.Context, req CompletionRequest) (CompletionResponse, error)
}

// Mode selects the LLMClient implementation returned by NewClient.
type Mode string

// Supported client modes.
const (
	// ModeMock returns a deterministic offline simulator (the default) so the
	// binary runs out of the box without any model server.
	ModeMock Mode = "mock"
	// ModeHTTP talks to an OpenAI-compatible /v1/chat/completions endpoint.
	ModeHTTP Mode = "http"
)

// ParseMode converts a flag value into a Mode.
func ParseMode(s string) (Mode, error) {
	switch Mode(strings.ToLower(strings.TrimSpace(s))) {
	case ModeMock:
		return ModeMock, nil
	case ModeHTTP:
		return ModeHTTP, nil
	default:
		return "", fmt.Errorf("unknown llm mode %q (want %q or %q)", s, ModeMock, ModeHTTP)
	}
}

// ClientOption customises NewClient.
type ClientOption func(*clientConfig)

type clientConfig struct {
	mode        Mode
	model       string
	apiKey      string
	timeout     time.Duration
	maxRetries  int
	httpClient  *http.Client
	mockLatency time.Duration
	logger      *slog.Logger
}

// WithMode selects the backend implementation (default ModeMock).
func WithMode(m Mode) ClientOption { return func(c *clientConfig) { c.mode = m } }

// WithModel sets the default model name sent to the backend.
func WithModel(model string) ClientOption { return func(c *clientConfig) { c.model = model } }

// WithAPIKey sets a bearer token for HTTP backends.
func WithAPIKey(key string) ClientOption { return func(c *clientConfig) { c.apiKey = key } }

// WithTimeout sets the per-HTTP-request timeout.
func WithTimeout(d time.Duration) ClientOption { return func(c *clientConfig) { c.timeout = d } }

// WithMaxRetries sets how many times transient HTTP failures are retried.
func WithMaxRetries(n int) ClientOption { return func(c *clientConfig) { c.maxRetries = n } }

// WithHTTPClient injects a custom *http.Client (overrides WithTimeout).
func WithHTTPClient(hc *http.Client) ClientOption { return func(c *clientConfig) { c.httpClient = hc } }

// WithMockLatency sets the simulated latency of the mock backend.
func WithMockLatency(d time.Duration) ClientOption { return func(c *clientConfig) { c.mockLatency = d } }

// WithLogger sets the client logger.
func WithLogger(l *slog.Logger) ClientOption { return func(c *clientConfig) { c.logger = l } }

// NewClient builds an LLMClient for the backend at baseURL. Without options
// it returns the deterministic mock so the orchestrator runs offline.
func NewClient(baseURL string, opts ...ClientOption) LLMClient {
	cfg := clientConfig{
		mode:        ModeMock,
		model:       "default",
		timeout:     300 * time.Second,
		maxRetries:  3,
		mockLatency: 150 * time.Millisecond,
		logger:      slog.Default(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.mode == ModeHTTP {
		return newHTTPClient(baseURL, cfg)
	}
	return NewMockClient("mock-"+cfg.model, cfg.mockLatency)
}

