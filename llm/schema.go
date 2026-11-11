package llm

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ---------------------------------------------------------------------------
// Planner hat output: hierarchical plan of Epics -> Stories -> Tasks whose leaf
// tasks form a DAG through explicit DependsOn task IDs.
// ---------------------------------------------------------------------------

// TaskSpec is a leaf, executable unit of work.
type TaskSpec struct {
	ID                 string          `json:"id"`
	Title              string          `json:"title"`
	Description        string          `json:"description"`
	TaskType           string          `json:"task_type"`
	DependsOn          []string        `json:"depends_on,omitempty"`
	AcceptanceCriteria []string        `json:"acceptance_criteria,omitempty"`
	Params             json.RawMessage `json:"params,omitempty"`
}

// StorySpec groups related tasks delivering one user-visible increment.
type StorySpec struct {
	ID          string     `json:"id"`
	Title       string     `json:"title"`
	Description string     `json:"description,omitempty"`
	Tasks       []TaskSpec `json:"tasks"`
}

// EpicSpec groups stories delivering one major capability.
type EpicSpec struct {
	ID          string      `json:"id"`
	Title       string      `json:"title"`
	Description string      `json:"description,omitempty"`
	Stories     []StorySpec `json:"stories"`
}

// PlanSpec is the complete hierarchical plan document.
type PlanSpec struct {
	ID        string     `json:"id,omitempty"`
	Title     string     `json:"title"`
	Objective string     `json:"objective,omitempty"`
	Epics     []EpicSpec `json:"epics"`
}

// ParsePlanSpec decodes a plan document, tolerating surrounding prose/fences.
func ParsePlanSpec(raw string) (PlanSpec, error) {
	spec, err := DecodeJSON[PlanSpec](raw)
	if err != nil {
		return PlanSpec{}, fmt.Errorf("malformed plan JSON: %w", err)
	}
	return spec, nil
}

// TaskCount returns the number of leaf tasks in the plan.
func (p PlanSpec) TaskCount() int {
	n := 0
	for _, e := range p.Epics {
		for _, s := range e.Stories {
			n += len(s.Tasks)
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// Reviewer hat output.
// ---------------------------------------------------------------------------

// ReviewVerdict is the Reviewer hat's structured judgement of a task output.
type ReviewVerdict struct {
	Approved           bool    `json:"approved"`
	Feedback           string  `json:"feedback"`
	NeedsClarification bool    `json:"needs_clarification"`
	Score              float64 `json:"score"`
}

func (v *ReviewVerdict) normalize() {
	v.Score = min(max(v.Score, 0), 1)
	v.Feedback = strings.TrimSpace(v.Feedback)
	if v.NeedsClarification {
		v.Approved = false
	}
	if !v.Approved && v.Feedback == "" {
		v.Feedback = "The output was rejected without specific feedback; re-check every acceptance criterion."
	}
}

// ---------------------------------------------------------------------------
// Project Manager hat output.
// ---------------------------------------------------------------------------

// RemediationAction is the Project Manager's triage decision.
type RemediationAction string

// Remediation actions.
const (
	// ActionSplit re-decomposes the task into simpler sub-tasks spliced into the DAG.
	ActionSplit RemediationAction = "split"
	// ActionClarify pauses the branch and asks the user a targeted question.
	ActionClarify RemediationAction = "clarify"
	// ActionFail marks the task as terminally failed (dependents become blocked).
	ActionFail RemediationAction = "fail"
)

// Remediation is the Project Manager hat's structured triage decision.
type Remediation struct {
	Action    RemediationAction `json:"action"`
	Rationale string            `json:"rationale"`
	// SubTasks is populated for ActionSplit. IDs are local to the proposal;
	// DependsOn may reference sibling sub-task IDs or existing plan task IDs.
	SubTasks []TaskSpec `json:"sub_tasks,omitempty"`
	// Explanation is a human-readable account of what failed (ActionClarify/ActionFail).
	Explanation string `json:"explanation,omitempty"`
	// Question is the targeted clarifying question for the user (ActionClarify).
	Question string `json:"question,omitempty"`
}

// ---------------------------------------------------------------------------
// Conversational hat outputs.
// ---------------------------------------------------------------------------

// Intent classifies a user chat message.
type Intent string

// Chat intents.
const (
	IntentAnswer    Intent = "answer"
	IntentDraftPlan Intent = "draft_plan"
)

// ConversationReply is the Conversational hat's structured chat response.
type ConversationReply struct {
	Reply     string `json:"reply"`
	Intent    Intent `json:"intent"`
	Directive string `json:"directive,omitempty"`
}

// ClarificationInterpretation folds a user's free-form clarification into a
// precise amendment of the paused task.
type ClarificationInterpretation struct {
	AmendedDescription           string   `json:"amended_description"`
	AdditionalAcceptanceCriteria []string `json:"additional_acceptance_criteria,omitempty"`
	Acknowledgement              string   `json:"acknowledgement"`
}

// ---------------------------------------------------------------------------
// Hat inputs.
// ---------------------------------------------------------------------------

// Capability describes a registered task type available to the Planner.
type Capability struct {
	TaskType    string `json:"task_type"`
	Description string `json:"description"`
}

// ConversationInput is the Conversational hat's input.
type ConversationInput struct {
	Message      string
	History      []Message
	StateSummary string
	FocusPlanID  string
}

// PlanningInput is the Planner hat's input. PreviousDraft and
// ValidationProblems are set on repair rounds after Go-side DAG validation
// rejected an earlier draft.
type PlanningInput struct {
	Directive          string
	Capabilities       []Capability
	StateSummary       string
	PreviousDraft      *PlanSpec
	ValidationProblems []string
}

// ReviewInput is the Reviewer hat's input.
type ReviewInput struct {
	PlanObjective  string
	EpicTitle      string
	StoryTitle     string
	Task           TaskSpec
	Attempt        int
	MaxAttempts    int
	Output         string
	PriorFeedback  []string
	Clarifications []string
}

// TriageInput is the Project Manager hat's input.
type TriageInput struct {
	PlanObjective   string
	EpicTitle       string
	StoryTitle      string
	Task            TaskSpec
	Reason          string
	Attempts        int
	MaxAttempts     int
	Feedback        []string
	LastOutput      string
	LastError       string
	CanSplit        bool
	Capabilities    []Capability
	ExistingTaskIDs []string
	Clarifications  []string
}

// ClarificationInput is the input for interpreting a user's clarification.
type ClarificationInput struct {
	Task        TaskSpec
	Explanation string
	Question    string
	Answer      string
	Feedback    []string
}

