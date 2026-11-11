package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

const maxResponseBytes = 8 << 20

// APIError is a non-2xx response from an HTTP backend.
type APIError struct {
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("llm backend returned HTTP %d: %s", e.StatusCode, e.Body)
}

// Retryable reports whether the failure is transient.
func (e *APIError) Retryable() bool {
	return e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500
}

// httpClient speaks the OpenAI-compatible chat completions protocol, which is
// served by vLLM, llama.cpp, Ollama, TGI, LM Studio, OpenAI and others.
type httpClient struct {
	endpoint   string
	model      string
	apiKey     string
	maxRetries int
	hc         *http.Client
	log        *slog.Logger
}

func newHTTPClient(baseURL string, cfg clientConfig) *httpClient {
	hc := cfg.httpClient
	if hc == nil {
		hc = &http.Client{Timeout: cfg.timeout}
	}
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	endpoint := base + "/v1/chat/completions"
	if strings.HasSuffix(base, "/v1") {
		endpoint = base + "/chat/completions"
	}
	return &httpClient{
		endpoint:   endpoint,
		model:      cfg.model,
		apiKey:     cfg.apiKey,
		maxRetries: max(cfg.maxRetries, 0),
		hc:         hc,
		log:        cfg.logger,
	}
}

type chatCompletionRequest struct {
	Model          string          `json:"model"`
	Messages       []Message       `json:"messages"`
	Temperature    float64         `json:"temperature"`
	MaxTokens      int             `json:"max_tokens,omitempty"`
	ResponseFormat *responseFormat `json:"response_format,omitempty"`
}

type responseFormat struct {
	Type string `json:"type"`
}

type chatCompletionResponse struct {
	Model   string `json:"model"`
	Choices []struct {
		Message      Message `json:"message"`
		FinishReason string  `json:"finish_reason"`
	} `json:"choices"`
	Usage Usage `json:"usage"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// Complete implements LLMClient with bounded exponential-backoff retries on
// transient failures (transport errors, HTTP 429 and 5xx).
func (c *httpClient) Complete(ctx context.Context, req CompletionRequest) (CompletionResponse, error) {
	model := req.Model
	if model == "" {
		model = c.model
	}
	body := chatCompletionRequest{
		Model:       model,
		Messages:    req.Messages,
		Temperature: req.Temperature,
		MaxTokens:   req.MaxTokens,
	}
	if req.JSONMode {
		body.ResponseFormat = &responseFormat{Type: "json_object"}
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return CompletionResponse{}, fmt.Errorf("encode completion request: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<(attempt-1)) * 500 * time.Millisecond
			c.log.Warn("retrying llm request", "tag", req.Tag, "attempt", attempt+1, "backoff", backoff, "err", lastErr)
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return CompletionResponse{}, ctx.Err()
			case <-timer.C:
			}
		}
		resp, retryable, err := c.do(ctx, payload)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !retryable || ctx.Err() != nil {
			break
		}
	}
	return CompletionResponse{}, fmt.Errorf("llm request to %s (tag %s): %w", c.endpoint, req.Tag, lastErr)
}

func (c *httpClient) do(ctx context.Context, payload []byte) (CompletionResponse, bool, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return CompletionResponse{}, false, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	if c.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	start := time.Now()
	resp, err := c.hc.Do(httpReq)
	if err != nil {
		return CompletionResponse{}, ctx.Err() == nil, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return CompletionResponse{}, true, fmt.Errorf("read response body: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		apiErr := &APIError{StatusCode: resp.StatusCode, Body: truncate(string(raw), 512)}
		return CompletionResponse{}, apiErr.Retryable(), apiErr
	}

	var parsed chatCompletionResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return CompletionResponse{}, false, fmt.Errorf("decode response body: %w", err)
	}
	if parsed.Error != nil {
		return CompletionResponse{}, false, errors.New("backend error: " + parsed.Error.Message)
	}
	if len(parsed.Choices) == 0 {
		return CompletionResponse{}, true, errors.New("backend response contained no choices")
	}
	choice := parsed.Choices[0]
	return CompletionResponse{
		Content:      choice.Message.Content,
		Model:        parsed.Model,
		FinishReason: choice.FinishReason,
		Usage:        parsed.Usage,
		Latency:      time.Since(start),
	}, false, nil
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

