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

func TestParsePlanSpec_LeGarbage(t *testing.T) {
	raw := `{
    "title": "Star Citizen Ghost State Investigation",
    "objective": "Perform a forensic investigation of a 1.5M line log file to identify the causal link between server-side instability, orphaned mission states, and the 'Ghost State' account corruption.",
    "epics": [
        {
            "title": "Signal Extraction",
            "objective": "Extract high-density forensic markers from the raw log data.",
            "stories": [
                {
                    "title": "Error and Mission Signatures",
                    "tasks": [
                        {
                            "name": "T1: Error Signatures",
                            "description": "Parse log for [Error] and [Critical] tags, focusing on Subsumption, DGS, and Mission services. Output to errors.json"
                        },
                        {
                            "name": "T2: Mission Lifecycle",
                            "description": "Extract <EndMission>, <MissionEnded>, and MISSION_OBJECTIVE_STATE transitions. Output to mission_lifecycle.json"
                        }
                    ]
                },
                {
                    "title": "Account and Stability Signatures",
                    "tasks": [
                        {
                            "name": "T3: ObjectDataBank Errors",
                            "description": "Surgical extraction of 'Missing Player ObjectDataBank' errors. Output to object_data_bank_errors.json"
                        },
                        {
                            "name": "T4: Server Instability",
                            "description": "Parse for <ServerCrashRecovery> and <ReplicationLayer> heartbeat losses. Output to server_instability.json"
                        }
                    ]
                }
            ]
        },
        {
            "title": "Correlation and Synthesis",
            "objective": "Analyze temporal relationships between extracted signals.",
            "stories": [
                {
                    "title": "Forensic Analysis",
                    "tasks": [
                        {
                            "name": "T5: Correlation Engine",
                            "description": "Python script to perform millisecond-accurate temporal join of T1, T2, T3, and T4. Output: correlation_results.json",
                            "depends_on": [
                                "T1",
                                "T2",
                                "T3",
                                "T4"
                            ]
                        },
                        {
                            "name": "T6: Final Forensic Report",
                            "description": "Generate a Markdown report detailing the causal chain of the desync."
                        }
                    ]
                }
            ]
        }
    ]
}`
	spec, err := ParsePlanSpec(raw)
	if err != nil {
		t.Fatalf("ParsePlanSpec failed: %v", err)
	}
	if spec.TaskCount() != 6 {
		t.Fatalf("expected 6 tasks, got %d", spec.TaskCount())
	}
	if spec.Epics[0].Stories[0].Tasks[0].ID != "T1" {
		t.Errorf("expected task ID 'T1', got %q", spec.Epics[0].Stories[0].Tasks[0].ID)
	}
	if spec.Epics[1].Stories[0].Tasks[0].ID != "T5" {
		t.Errorf("expected task ID 'T5', got %q", spec.Epics[1].Stories[0].Tasks[0].ID)
	}
	if spec.Epics[0].ID == "" || spec.Epics[0].Stories[0].ID == "" {
		t.Errorf("expected auto-generated IDs for epics and stories, got Epic=%q Story=%q",
			spec.Epics[0].ID, spec.Epics[0].Stories[0].ID)
	}
}

