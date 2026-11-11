package tasks

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"go-orchestrator/llm"
)

func TestCodeGenHandler_Execute(t *testing.T) {
	mock := llm.NewMockClient("mock-test", 0)
	handler := NewCodeGenHandler(mock)

	input := TaskInput{
		TaskID:      "T1",
		Title:       "TokenBucket",
		Description: "Implement a token bucket rate limiter in Go",
		TaskType:    "code_gen",
		Params:      json.RawMessage(`{"language":"go","package":"limiter","filename":"limiter.go"}`),
		UpstreamOutputs: map[string]string{
			"T0": "Interface: type Limiter interface { Allow() bool }",
		},
		Attempt: 1,
	}

	out, err := handler.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(out.Artifacts) == 0 {
		t.Errorf("expected at least one generated artifact file")
	}

	code, exists := out.Artifacts["limiter.go"]
	if !exists {
		t.Errorf("expected limiter.go artifact to exist")
	}
	if !strings.Contains(code, "package limiter") {
		t.Errorf("expected generated code to contain 'package limiter', got:\n%s", code)
	}
}

func TestTextExtractHandler_Execute(t *testing.T) {
	mock := llm.NewMockClient("mock-test", 0)
	handler := NewTextExtractHandler(mock)

	doc := `The primary goal is to ensure high throughput and low latency.
Constraint: must not exceed 100MB of memory allocation.
The system will deliver a concurrent pipeline with automated retries.
A major risk is network partitions causing flaky connections.`

	input := TaskInput{
		TaskID:      "T2",
		Title:       "Extract Requirements",
		Description: "Extract goals and risks from the specification",
		TaskType:    "text_extract",
		Params:      json.RawMessage(`{"fields":["goals","constraints","risks"],"chunk_size":200}`),
		UpstreamOutputs: map[string]string{
			"T1": doc,
		},
		Attempt: 1,
	}

	out, err := handler.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(out.Content, "insights") {
		t.Errorf("expected JSON output containing insights, got: %s", out.Content)
	}

	var res struct {
		Insights []struct {
			Field string `json:"field"`
			Value string `json:"value"`
		} `json:"insights"`
	}
	if err := json.Unmarshal([]byte(out.Content), &res); err != nil {
		t.Fatalf("failed to decode extraction JSON: %v", err)
	}
	if len(res.Insights) == 0 {
		t.Errorf("expected extracted insights, got 0")
	}
}

