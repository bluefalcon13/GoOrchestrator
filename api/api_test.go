package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go-orchestrator/engine"
	"go-orchestrator/llm"
	"go-orchestrator/tasks"
)

func setupTestAPIServer(t *testing.T) (*Server, http.Handler, chan any, context.CancelFunc) {
	mock := llm.NewMockClient("mock-test", 0)
	lrm := llm.NewLRMPersonaEngine(mock)

	registry := tasks.NewRegistry()
	registry.Register("code_gen", tasks.NewCodeGenHandler(mock))
	registry.Register("text_extract", tasks.NewTextExtractHandler(mock))

	eventCh := make(chan any, 100)
	orch := engine.NewOrchestrator(lrm, mock, registry, eventCh)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = orch.RunEventLoop(ctx, nil)
	}()

	srv := NewServer(0, eventCh, lrm)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/chat", srv.handleChat)
	mux.HandleFunc("GET /api/v1/plans", srv.handleListPlans)
	mux.HandleFunc("GET /api/v1/plans/{planID}", srv.handleGetPlan)
	mux.HandleFunc("POST /api/v1/plans/{planID}/approve", srv.handleApprovePlan)
	mux.HandleFunc("POST /api/v1/plans/{planID}/tasks/{taskID}/clarify", srv.handleClarifyTask)
	mux.HandleFunc("GET /openapi.yaml", srv.handleOpenAPI)

	return srv, mux, eventCh, cancel
}

func TestAPI_OpenAPI(t *testing.T) {
	_, handler, _, cancel := setupTestAPIServer(t)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "openapi: 3.1.0") {
		t.Errorf("expected OpenAPI 3.1 header, got:\n%s", w.Body.String()[:100])
	}
}

func TestAPI_Chat_AnswerAndPlanDraft(t *testing.T) {
	_, handler, _, cancel := setupTestAPIServer(t)
	defer cancel()

	// 1. Question about system state
	chatBody, _ := json.Marshal(map[string]string{
		"message": "What is currently running?",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/chat", bytes.NewReader(chatBody))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}
	var chatResp chatResponse
	if err := json.Unmarshal(w.Body.Bytes(), &chatResp); err != nil {
		t.Fatalf("decode chat response: %v", err)
	}
	if chatResp.Intent != llm.IntentAnswer {
		t.Errorf("expected intent answer, got %s", chatResp.Intent)
	}

	// 2. Directive to build something (triggers plan drafting)
	planDirectiveBody, _ := json.Marshal(map[string]string{
		"message": "build an LRU cache in Go with TTL expiration",
	})
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/chat", bytes.NewReader(planDirectiveBody))
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w2.Code, w2.Body.String())
	}
	var planResp chatResponse
	if err := json.Unmarshal(w2.Body.Bytes(), &planResp); err != nil {
		t.Fatalf("decode chat response: %v", err)
	}
	if planResp.Intent != llm.IntentDraftPlan {
		t.Errorf("expected intent draft_plan, got %s", planResp.Intent)
	}
	if planResp.DraftPlan == nil {
		t.Fatalf("expected DraftPlan in response, got nil")
	}
	if len(planResp.DraftPlan.Epics) == 0 {
		t.Fatalf("expected epics in drafted plan")
	}
}

func TestAPI_ApprovePlanAndQueryStatus(t *testing.T) {
	_, handler, _, cancel := setupTestAPIServer(t)
	defer cancel()

	planSpec := llm.PlanSpec{
		ID:        "plan-api-1",
		Title:     "API Test Plan",
		Objective: "Test execution over HTTP API",
		Epics: []llm.EpicSpec{
			{
				ID: "E1", Title: "Epic 1",
				Stories: []llm.StorySpec{
					{
						ID: "E1.S1", Title: "Story 1",
						Tasks: []llm.TaskSpec{
							{
								ID:          "T1",
								Title:       "Task 1",
								Description: "Simple task",
								TaskType:    "code_gen",
								Params:      json.RawMessage(`{"language":"go","package":"p"}`),
							},
						},
					},
				},
			},
		},
	}

	body, _ := json.Marshal(planSpec)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/plans/plan-api-1/approve", bytes.NewReader(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for approve, got %d: %s", w.Code, w.Body.String())
	}

	// Poll GET /api/v1/plans/plan-api-1
	var finalPlan engine.PlanNode
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		getReq := httptest.NewRequest(http.MethodGet, "/api/v1/plans/plan-api-1", nil)
		getW := httptest.NewRecorder()
		handler.ServeHTTP(getW, getReq)

		if getW.Code == http.StatusOK {
			_ = json.Unmarshal(getW.Body.Bytes(), &finalPlan)
			if finalPlan.Status == engine.PlanStatusCompleted {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	if finalPlan.Status != engine.PlanStatusCompleted {
		t.Errorf("expected plan status Completed, got %s", finalPlan.Status)
	}
}

func TestAPI_ClarifyEndpoint(t *testing.T) {
	_, handler, _, cancel := setupTestAPIServer(t)
	defer cancel()

	planSpec := llm.PlanSpec{
		ID:        "plan-api-ambig",
		Title:     "Ambig Test Plan",
		Objective: "Test clarify endpoint",
		Epics: []llm.EpicSpec{
			{
				ID: "E1", Title: "Epic 1",
				Stories: []llm.StorySpec{
					{
						ID: "E1.S1", Title: "Story 1",
						Tasks: []llm.TaskSpec{
							{
								ID:          "T1",
								Title:       "Ambiguous Task",
								Description: "This description is ambiguous",
								TaskType:    "code_gen",
								Params:      json.RawMessage(`{"language":"go","package":"p"}`),
							},
						},
					},
				},
			},
		},
	}

	body, _ := json.Marshal(planSpec)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/plans/plan-api-ambig/approve", bytes.NewReader(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}

	// Wait for T1 to pause in AwaitingClarification
	deadline := time.Now().Add(2 * time.Second)
	paused := false
	for time.Now().Before(deadline) {
		getReq := httptest.NewRequest(http.MethodGet, "/api/v1/plans/plan-api-ambig", nil)
		getW := httptest.NewRecorder()
		handler.ServeHTTP(getW, getReq)

		var p engine.PlanNode
		if getW.Code == http.StatusOK {
			_ = json.Unmarshal(getW.Body.Bytes(), &p)
			if len(p.Epics) > 0 && len(p.Epics[0].Stories[0].Tasks) > 0 {
				if p.Epics[0].Stories[0].Tasks[0].Status == engine.StatusAwaitingClarification {
					paused = true
					break
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !paused {
		t.Fatalf("task never paused in AwaitingClarification")
	}

	// Call POST /api/v1/plans/plan-api-ambig/tasks/T1/clarify
	clarifyPayload, _ := json.Marshal(map[string]string{
		"answer": "Here is the exact behaviour: use FIFO eviction with 1000 items capacity.",
	})
	clarifyReq := httptest.NewRequest(http.MethodPost, "/api/v1/plans/plan-api-ambig/tasks/T1/clarify", bytes.NewReader(clarifyPayload))
	clarifyW := httptest.NewRecorder()
	handler.ServeHTTP(clarifyW, clarifyReq)

	if clarifyW.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for clarify, got %d: %s", clarifyW.Code, clarifyW.Body.String())
	}

	// Verify plan reaches Completed
	completed := false
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		getReq := httptest.NewRequest(http.MethodGet, "/api/v1/plans/plan-api-ambig", nil)
		getW := httptest.NewRecorder()
		handler.ServeHTTP(getW, getReq)

		var p engine.PlanNode
		if getW.Code == http.StatusOK {
			_ = json.Unmarshal(getW.Body.Bytes(), &p)
			if p.Status == engine.PlanStatusCompleted {
				completed = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !completed {
		t.Errorf("plan did not reach Completed after clarification")
	}
}

