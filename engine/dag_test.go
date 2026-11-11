package engine

import (
	"context"
	"strings"
	"testing"

	"go-orchestrator/llm"
	"go-orchestrator/tasks"
)

type dummyHandler struct{}

func (d *dummyHandler) Execute(ctx context.Context, input tasks.TaskInput) (tasks.TaskOutput, error) {
	return tasks.TaskOutput{Content: "dummy"}, nil
}
func (d *dummyHandler) Description() string { return "dummy" }

func TestValidatePlanSpec_ValidDAG(t *testing.T) {
	reg := tasks.NewRegistry()
	reg.Register("code_gen", &dummyHandler{})
	reg.Register("text_extract", &dummyHandler{})

	spec := llm.PlanSpec{
		Title: "Test Plan",
		Epics: []llm.EpicSpec{
			{
				ID: "E1", Title: "Epic 1",
				Stories: []llm.StorySpec{
					{
						ID: "E1.S1", Title: "Story 1",
						Tasks: []llm.TaskSpec{
							{ID: "T1", Title: "Task 1", TaskType: "text_extract"},
							{ID: "T2", Title: "Task 2", TaskType: "code_gen", DependsOn: []string{"T1"}},
							{ID: "T3", Title: "Task 3", TaskType: "code_gen", DependsOn: []string{"T1"}},
							{ID: "T4", Title: "Task 4", TaskType: "code_gen", DependsOn: []string{"T2", "T3"}},
						},
					},
				},
			},
		},
	}

	sorted, err := ValidatePlanSpec(spec, reg)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	if len(sorted) != 4 {
		t.Fatalf("expected 4 sorted tasks, got %d", len(sorted))
	}
	if sorted[0] != "T1" {
		t.Errorf("expected T1 first, got %s", sorted[0])
	}
	if sorted[3] != "T4" {
		t.Errorf("expected T4 last, got %s", sorted[3])
	}
}

func TestValidatePlanSpec_CycleDetection(t *testing.T) {
	reg := tasks.NewRegistry()
	reg.Register("code_gen", &dummyHandler{})

	spec := llm.PlanSpec{
		Title: "Cyclic Plan",
		Epics: []llm.EpicSpec{
			{
				ID: "E1", Title: "Epic 1",
				Stories: []llm.StorySpec{
					{
						ID: "E1.S1", Title: "Story 1",
						Tasks: []llm.TaskSpec{
							{ID: "T1", Title: "Task 1", TaskType: "code_gen", DependsOn: []string{"T2"}},
							{ID: "T2", Title: "Task 2", TaskType: "code_gen", DependsOn: []string{"T1"}},
						},
					},
				},
			},
		},
	}

	_, err := ValidatePlanSpec(spec, reg)
	if err == nil {
		t.Fatal("expected cycle error, got nil")
	}
	if !strings.Contains(err.Error(), "cycle detected") {
		t.Errorf("expected cycle detection message, got %v", err)
	}
}

func TestValidatePlanSpec_MissingDependency(t *testing.T) {
	reg := tasks.NewRegistry()
	reg.Register("code_gen", &dummyHandler{})

	spec := llm.PlanSpec{
		Title: "Missing Dep Plan",
		Epics: []llm.EpicSpec{
			{
				ID: "E1", Title: "Epic 1",
				Stories: []llm.StorySpec{
					{
						ID: "E1.S1", Title: "Story 1",
						Tasks: []llm.TaskSpec{
							{ID: "T1", Title: "Task 1", TaskType: "code_gen", DependsOn: []string{"T_NONEXISTENT"}},
						},
					},
				},
			},
		},
	}

	_, err := ValidatePlanSpec(spec, reg)
	if err == nil {
		t.Fatal("expected missing dep error, got nil")
	}
	if !strings.Contains(err.Error(), "non-existent task") {
		t.Errorf("expected non-existent task error, got %v", err)
	}
}

func TestValidatePlanSpec_UnregisteredHandler(t *testing.T) {
	reg := tasks.NewRegistry()

	spec := llm.PlanSpec{
		Title: "Unregistered Handler Plan",
		Epics: []llm.EpicSpec{
			{
				ID: "E1", Title: "Epic 1",
				Stories: []llm.StorySpec{
					{
						ID: "E1.S1", Title: "Story 1",
						Tasks: []llm.TaskSpec{
							{ID: "T1", Title: "Task 1", TaskType: "unknown_task_type"},
						},
					},
				},
			},
		},
	}

	_, err := ValidatePlanSpec(spec, reg)
	if err == nil {
		t.Fatal("expected unregistered handler error, got nil")
	}
	if !strings.Contains(err.Error(), "unregistered task_type") {
		t.Errorf("expected unregistered task_type error, got %v", err)
	}
}

