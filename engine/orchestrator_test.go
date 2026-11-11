package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go-orchestrator/llm"
	"go-orchestrator/tasks"
)

func setupTestEngine(t *testing.T) (*Orchestrator, chan any, context.CancelFunc) {
	// Mock client with zero latency for fast deterministic unit tests
	mock := llm.NewMockClient("mock-test", 0)
	lrm := llm.NewLRMPersonaEngine(mock)

	registry := tasks.NewRegistry()
	registry.Register("code_gen", tasks.NewCodeGenHandler(mock))
	registry.Register("text_extract", tasks.NewTextExtractHandler(mock))

	eventCh := make(chan any, 100)
	orch := NewOrchestrator(lrm, mock, registry, eventCh)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = orch.RunEventLoop(ctx, nil)
	}()

	return orch, eventCh, cancel
}

func pollPlan(t *testing.T, eventCh chan any, planID string, timeout time.Duration, condition func(*PlanNode) bool) *PlanNode {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		replyCh := make(chan GetPlanResult, 1)
		eventCh <- GetPlanQuery{PlanID: planID, ReplyCh: replyCh}
		res := <-replyCh
		if res.Found && condition(res.Plan) {
			return res.Plan
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for plan condition on %s", planID)
	return nil
}

func TestOrchestrator_EndToEndSuccess(t *testing.T) {
	_, eventCh, cancel := setupTestEngine(t)
	defer cancel()

	planSpec := llm.PlanSpec{
		ID:        "plan-e2e",
		Title:     "Build Rate Limiter",
		Objective: "Implement in-memory rate limiter",
		Epics: []llm.EpicSpec{
			{
				ID: "E1", Title: "Core Engineering",
				Stories: []llm.StorySpec{
					{
						ID: "E1.S1", Title: "Rate Limiter Core",
						Tasks: []llm.TaskSpec{
							{
								ID:          "T1",
								Title:       "Define rate limiter types",
								Description: "Define Limiter struct and options in Go",
								TaskType:    "code_gen",
								Params:      json.RawMessage(`{"language":"go","package":"limiter","filename":"limiter.go"}`),
							},
							{
								ID:          "T2",
								Title:       "Extract rate limiter requirements",
								Description: "Extract constraints from docs",
								TaskType:    "text_extract",
								DependsOn:   []string{"T1"},
								Params:      json.RawMessage(`{"fields":["goals","constraints"]}`),
							},
						},
					},
				},
			},
		},
	}

	planBytes, _ := json.Marshal(planSpec)
	replyCh := make(chan CommandResult, 1)
	eventCh <- SeedPlanEvent{PlanID: planSpec.ID, RawJSON: string(planBytes), ReplyCh: replyCh}
	res := <-replyCh
	if !res.Success {
		t.Fatalf("failed to seed plan: %v", res.Err)
	}

	finalPlan := pollPlan(t, eventCh, planSpec.ID, 3*time.Second, func(p *PlanNode) bool {
		return p.Status == PlanStatusCompleted
	})

	if finalPlan.Status != PlanStatusCompleted {
		t.Errorf("expected plan status Completed, got %s", finalPlan.Status)
	}
	if finalPlan.Epics[0].Status != PlanStatusCompleted {
		t.Errorf("expected epic status Completed, got %s", finalPlan.Epics[0].Status)
	}
	if finalPlan.Epics[0].Stories[0].Status != PlanStatusCompleted {
		t.Errorf("expected story status Completed, got %s", finalPlan.Epics[0].Stories[0].Status)
	}
}

func TestOrchestrator_TacticalRetry(t *testing.T) {
	_, eventCh, cancel := setupTestEngine(t)
	defer cancel()

	// "flaky" triggers defective first output in mock, which is rejected by reviewer,
	// feedback is appended, and second attempt succeeds.
	planSpec := llm.PlanSpec{
		ID:        "plan-flaky",
		Title:     "Flaky Task Test",
		Objective: "Test retry mechanism",
		Epics: []llm.EpicSpec{
			{
				ID: "E1", Title: "Epic",
				Stories: []llm.StorySpec{
					{
						ID: "E1.S1", Title: "Story",
						Tasks: []llm.TaskSpec{
							{
								ID:          "T1",
								Title:       "Build flaky component",
								Description: "This is a flaky implementation requiring retry",
								TaskType:    "code_gen",
								Params:      json.RawMessage(`{"language":"go","package":"core"}`),
							},
						},
					},
				},
			},
		},
	}

	planBytes, _ := json.Marshal(planSpec)
	replyCh := make(chan CommandResult, 1)
	eventCh <- SeedPlanEvent{PlanID: planSpec.ID, RawJSON: string(planBytes), ReplyCh: replyCh}
	res := <-replyCh
	if !res.Success {
		t.Fatalf("failed to seed plan: %v", res.Err)
	}

	finalPlan := pollPlan(t, eventCh, planSpec.ID, 3*time.Second, func(p *PlanNode) bool {
		return p.Status == PlanStatusCompleted
	})

	task := finalPlan.Epics[0].Stories[0].Tasks[0]
	if task.Attempts < 2 {
		t.Errorf("expected task to have at least 2 attempts due to retry, got %d", task.Attempts)
	}
	if len(task.FeedbackHistory) == 0 {
		t.Errorf("expected task to record feedback history on retry")
	}
}

func TestOrchestrator_StrategicEscalation_Split(t *testing.T) {
	_, eventCh, cancel := setupTestEngine(t)
	defer cancel()

	// "monolithic" triggers repeated review failures; max retries exceeded triggers
	// Project Manager triage which selects ActionSplit. Subtasks are spliced and executed.
	planSpec := llm.PlanSpec{
		ID:        "plan-split",
		Title:     "Monolithic Task Test",
		Objective: "Test PM decomposition",
		Epics: []llm.EpicSpec{
			{
				ID: "E1", Title: "Epic",
				Stories: []llm.StorySpec{
					{
						ID: "E1.S1", Title: "Story",
						Tasks: []llm.TaskSpec{
							{
								ID:          "T1",
								Title:       "Monolithic task",
								Description: "This is a monolithic system that needs splitting",
								TaskType:    "code_gen",
								Params:      json.RawMessage(`{"language":"go","package":"monolith"}`),
							},
						},
					},
				},
			},
		},
	}

	planBytes, _ := json.Marshal(planSpec)
	replyCh := make(chan CommandResult, 1)
	eventCh <- SeedPlanEvent{PlanID: planSpec.ID, RawJSON: string(planBytes), ReplyCh: replyCh}
	res := <-replyCh
	if !res.Success {
		t.Fatalf("failed to seed plan: %v", res.Err)
	}

	finalPlan := pollPlan(t, eventCh, planSpec.ID, 5*time.Second, func(p *PlanNode) bool {
		return p.Status == PlanStatusCompleted
	})

	story := finalPlan.Epics[0].Stories[0]
	if len(story.Tasks) <= 1 {
		t.Errorf("expected subtasks to be spliced into story, found %d tasks", len(story.Tasks))
	}
}

func TestOrchestrator_StrategicEscalation_ClarificationAndResume(t *testing.T) {
	_, eventCh, cancel := setupTestEngine(t)
	defer cancel()

	// "ambiguous" triggers Reviewer NeedsClarification -> Project Manager ActionClarify.
	// Task pauses in AwaitingClarification, dependent task T2 is Blocked.
	// We submit clarification via ClarificationResolvedEvent -> T1 resumes and completes,
	// T2 unblocks and completes.
	planSpec := llm.PlanSpec{
		ID:        "plan-clarify",
		Title:     "Ambiguous Task Test",
		Objective: "Test clarification flow",
		Epics: []llm.EpicSpec{
			{
				ID: "E1", Title: "Epic",
				Stories: []llm.StorySpec{
					{
						ID: "E1.S1", Title: "Story",
						Tasks: []llm.TaskSpec{
							{
								ID:          "T1",
								Title:       "Ambiguous task",
								Description: "This description is ambiguous and lacks specs",
								TaskType:    "code_gen",
								Params:      json.RawMessage(`{"language":"go","package":"ambig"}`),
							},
							{
								ID:          "T2",
								Title:       "Dependent task",
								Description: "Depends on T1",
								TaskType:    "text_extract",
								DependsOn:   []string{"T1"},
							},
						},
					},
				},
			},
		},
	}

	planBytes, _ := json.Marshal(planSpec)
	replyCh := make(chan CommandResult, 1)
	eventCh <- SeedPlanEvent{PlanID: planSpec.ID, RawJSON: string(planBytes), ReplyCh: replyCh}
	res := <-replyCh
	if !res.Success {
		t.Fatalf("failed to seed plan: %v", res.Err)
	}

	// 1. Wait for T1 to pause in AwaitingClarification
	pausedPlan := pollPlan(t, eventCh, planSpec.ID, 3*time.Second, func(p *PlanNode) bool {
		t1 := p.Epics[0].Stories[0].Tasks[0]
		return t1.Status == StatusAwaitingClarification
	})

	t1 := pausedPlan.Epics[0].Stories[0].Tasks[0]
	t2 := pausedPlan.Epics[0].Stories[0].Tasks[1]
	if t1.Status != StatusAwaitingClarification {
		t.Fatalf("expected T1 to be AwaitingClarification, got %s", t1.Status)
	}
	if t2.Status != StatusBlocked {
		t.Fatalf("expected T2 to be Blocked, got %s", t2.Status)
	}
	if t1.PendingQuestion == "" {
		t.Fatalf("expected T1 to have a pending clarifying question")
	}

	// 2. Submit clarification
	clarifyReply := make(chan CommandResult, 1)
	eventCh <- ClarificationResolvedEvent{
		PlanID: planSpec.ID,
		TaskID: "T1",
		Interpretation: llm.ClarificationInterpretation{
			AmendedDescription: "Implement a thread-safe token bucket limiter using sync.Mutex.",
			Acknowledgement:    "Clarification accepted.",
		},
		Answer:  "Use sync.Mutex and replenish tokens every second.",
		ReplyCh: clarifyReply,
	}

	cRes := <-clarifyReply
	if !cRes.Success {
		t.Fatalf("failed to resolve clarification: %v", cRes.Message)
	}

	// 3. Verify entire plan runs to Completed
	resumedPlan := pollPlan(t, eventCh, planSpec.ID, 4*time.Second, func(p *PlanNode) bool {
		return p.Status == PlanStatusCompleted
	})

	if resumedPlan.Status != PlanStatusCompleted {
		t.Errorf("expected plan to reach Completed after clarification, got %s", resumedPlan.Status)
	}
}

