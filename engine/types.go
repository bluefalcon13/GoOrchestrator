package engine

import (
	"encoding/json"
	"os"
	"time"

	"go-orchestrator/llm"
	"go-orchestrator/tasks"
)

// TaskStatus represents the lifecycle state of a leaf task node.
type TaskStatus string

const (
	StatusPending               TaskStatus = "Pending"
	StatusReady                 TaskStatus = "Ready"
	StatusRunning               TaskStatus = "Running"
	StatusReviewing             TaskStatus = "Reviewing"
	StatusAwaitingClarification TaskStatus = "AwaitingClarification"
	StatusCompleted             TaskStatus = "Completed"
	StatusFailed                TaskStatus = "Failed"
	StatusBlocked               TaskStatus = "Blocked"
)

// PlanStatus represents the overall state of an Epic, Story, or Plan.
type PlanStatus string

const (
	PlanStatusDraft                 PlanStatus = "Draft"
	PlanStatusPending               PlanStatus = "Pending"
	PlanStatusReady                 PlanStatus = "Ready"
	PlanStatusRunning               PlanStatus = "Running"
	PlanStatusAwaitingClarification PlanStatus = "AwaitingClarification"
	PlanStatusCompleted             PlanStatus = "Completed"
	PlanStatusFailed                PlanStatus = "Failed"
	PlanStatusBlocked               PlanStatus = "Blocked"
)

// TaskNode is the in-memory representation of a leaf task during execution.
type TaskNode struct {
	ID                 string            `json:"id"`
	Title              string            `json:"title"`
	Description        string            `json:"description"`
	TaskType           string            `json:"task_type"`
	DependsOn          []string          `json:"depends_on"`
	AcceptanceCriteria []string          `json:"acceptance_criteria"`
	Params             json.RawMessage   `json:"params,omitempty"`
	Status             TaskStatus        `json:"status"`
	Attempts           int               `json:"attempts"`
	MaxAttempts        int               `json:"max_attempts"`
	Output             string            `json:"output,omitempty"`
	Artifacts          map[string]string `json:"artifacts,omitempty"`
	FeedbackHistory    []string          `json:"feedback_history,omitempty"`
	Clarifications     []string          `json:"clarifications,omitempty"`
	PendingExplanation string            `json:"pending_explanation,omitempty"`
	PendingQuestion    string            `json:"pending_question,omitempty"`
	LastError          string            `json:"last_error,omitempty"`
	LastScore          float64           `json:"last_score,omitempty"`
	IsJoinNode         bool              `json:"is_join_node,omitempty"`
	SubTaskIDs         []string          `json:"sub_task_ids,omitempty"`
	CreatedAt          time.Time         `json:"created_at"`
	UpdatedAt          time.Time         `json:"updated_at"`
}

// StoryNode groups related tasks in the active plan.
type StoryNode struct {
	ID          string      `json:"id"`
	Title       string      `json:"title"`
	Description string      `json:"description,omitempty"`
	Status      PlanStatus  `json:"status"`
	Tasks       []*TaskNode `json:"tasks"`
}

// EpicNode groups related stories in the active plan.
type EpicNode struct {
	ID          string       `json:"id"`
	Title       string       `json:"title"`
	Description string       `json:"description,omitempty"`
	Status      PlanStatus   `json:"status"`
	Stories     []*StoryNode `json:"stories"`
}

// PlanNode is the top-level execution tree for an orchestration run.
type PlanNode struct {
	ID        string      `json:"id"`
	Title     string      `json:"title"`
	Objective string      `json:"objective"`
	Status    PlanStatus  `json:"status"`
	Epics     []*EpicNode `json:"epics"`
	CreatedAt time.Time   `json:"created_at"`
	UpdatedAt time.Time   `json:"updated_at"`
}

// ---------------------------------------------------------------------------
// Engine Event Definitions
// ---------------------------------------------------------------------------

// SigEvent carries OS termination signals onto the event bus.
type SigEvent struct {
	Signal os.Signal
}

// FatalErrorEvent reports an unrecoverable component error (e.g. HTTP server listen failure).
type FatalErrorEvent struct {
	Err error
}

// CommandResult is a synchronous command outcome passed over reply channels.
type CommandResult struct {
	Success bool
	Message string
	Data    any
	Err     error
}

// SeedPlanEvent requests parsing, validation, and scheduling of a raw plan JSON.
type SeedPlanEvent struct {
	PlanID  string
	RawJSON string
	ReplyCh chan CommandResult
}

// TaskExecutionResultEvent carries the output of an asynchronous worker execution.
type TaskExecutionResultEvent struct {
	PlanID  string
	TaskID  string
	Output  tasks.TaskOutput
	Err     error
	Attempt int
}

// TaskReviewResultEvent carries the verdict from the LRM Reviewer hat.
type TaskReviewResultEvent struct {
	PlanID  string
	TaskID  string
	Verdict llm.ReviewVerdict
	Err     error
	Attempt int
}

// TaskEscalatedEvent signals that a task has failed review retries or requires clarification.
type TaskEscalatedEvent struct {
	PlanID   string
	TaskID   string
	Reason   string
	Attempts int
}

// RemediationResultEvent carries the decision from the LRM Project Manager hat.
type RemediationResultEvent struct {
	PlanID      string
	TaskID      string
	Remediation llm.Remediation
	Err         error
}

// ClarificationResolvedEvent applies user-provided clarification back onto a paused task.
type ClarificationResolvedEvent struct {
	PlanID         string
	TaskID         string
	Interpretation llm.ClarificationInterpretation
	Answer         string
	ReplyCh        chan CommandResult
}

// ---------------------------------------------------------------------------
// API Query / Command Events (Bridged over eventCh)
// ---------------------------------------------------------------------------

// GetPlanQuery queries the state of a specific plan by ID.
type GetPlanQuery struct {
	PlanID  string
	ReplyCh chan GetPlanResult
}

// GetPlanResult returns the requested plan snapshot.
type GetPlanResult struct {
	Plan  *PlanNode
	Found bool
}

// ListPlansQuery queries all plans in memory.
type ListPlansQuery struct {
	ReplyCh chan []*PlanNode
}

// GetSystemStateQuery queries a concise text summary of system activity.
type GetSystemStateQuery struct {
	ReplyCh chan string
}

// StoreDraftPlanCommand saves an unapproved draft plan produced by the conversational/planner hats.
type StoreDraftPlanCommand struct {
	Plan    llm.PlanSpec
	ReplyCh chan CommandResult
}

