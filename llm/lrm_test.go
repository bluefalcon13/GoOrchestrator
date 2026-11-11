package llm

import (
	"context"
	"strings"
	"testing"
)

func TestLRMPersonaEngine_MockFlow(t *testing.T) {
	mock := NewMockClient("mock-unit", 0)
	lrm := NewLRMPersonaEngine(mock)
	ctx := context.Background()

	// 1. Conversational Hat - general query
	reply, err := lrm.Converse(ctx, ConversationInput{
		Message:      "What is the system status?",
		StateSummary: "Plan ID: p1 | Status: Running\n  Task T1: Build [Running]",
	})
	if err != nil {
		t.Fatalf("conversational failed: %v", err)
	}
	if reply.Intent != IntentAnswer || !strings.Contains(reply.Reply, "Plan ID: p1") {
		t.Errorf("unexpected conversational reply: %+v", reply)
	}

	// 2. Conversational Hat - directive detection
	replyDir, err := lrm.Converse(ctx, ConversationInput{
		Message: "build a distributed key-value store in Go",
	})
	if err != nil {
		t.Fatalf("conversational directive failed: %v", err)
	}
	if replyDir.Intent != IntentDraftPlan || replyDir.Directive == "" {
		t.Errorf("expected draft_plan intent, got %+v", replyDir)
	}

	// 3. Planner Hat
	plan, err := lrm.DraftPlan(ctx, PlanningInput{
		Directive: replyDir.Directive,
		Capabilities: []Capability{
			{TaskType: "code_gen", Description: "Code generator"},
			{TaskType: "text_extract", Description: "Text extractor"},
		},
	})
	if err != nil {
		t.Fatalf("planner failed: %v", err)
	}
	if len(plan.Epics) == 0 || plan.TaskCount() == 0 {
		t.Fatalf("expected non-empty plan, got %d tasks", plan.TaskCount())
	}

	// 4. Reviewer Hat
	taskSpec := plan.Epics[0].Stories[0].Tasks[0]
	verdict, err := lrm.Review(ctx, ReviewInput{
		PlanObjective: plan.Objective,
		Task:          taskSpec,
		Attempt:       1,
		MaxAttempts:   3,
		Output:        `{"files":[{"path":"types.go","content":"package app"}]}`,
	})
	if err != nil {
		t.Fatalf("reviewer failed: %v", err)
	}
	if !verdict.Approved {
		t.Errorf("expected approved verdict, got %+v", verdict)
	}

	// 5. Project Manager Hat (triage)
	remediation, err := lrm.Triage(ctx, TriageInput{
		PlanObjective:   plan.Objective,
		Task:            taskSpec,
		Reason:          "Exhausted 3 attempts",
		Attempts:        3,
		MaxAttempts:     3,
		CanSplit:        true,
		ExistingTaskIDs: []string{taskSpec.ID},
	})
	if err != nil {
		t.Fatalf("triage failed: %v", err)
	}
	if remediation.Action != ActionSplit && remediation.Action != ActionClarify {
		t.Errorf("unexpected remediation action: %s", remediation.Action)
	}

	// 6. Conversational Hat (clarification interpretation)
	clarification, err := lrm.InterpretClarification(ctx, ClarificationInput{
		Task:        taskSpec,
		Explanation: "Need explicit eviction policy",
		Question:    "What eviction policy?",
		Answer:      "Use LRU eviction with a mutex.",
	})
	if err != nil {
		t.Fatalf("clarification failed: %v", err)
	}
	if !strings.Contains(clarification.AmendedDescription, "LRU") {
		t.Errorf("expected amended description to contain clarification, got: %s", clarification.AmendedDescription)
	}
}

