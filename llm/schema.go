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

func (t *TaskSpec) UnmarshalJSON(data []byte) error {
	type Alias TaskSpec
	aux := struct {
		*Alias
		TaskID     string `json:"task_id"`
		TaskName   string `json:"task_name"`
		Name       string `json:"name"`
		Identifier string `json:"identifier"`
	}{
		Alias: (*Alias)(t),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if t.ID == "" {
		if aux.TaskID != "" {
			t.ID = aux.TaskID
		} else if aux.Identifier != "" {
			t.ID = aux.Identifier
		}
	}
	if t.Title == "" {
		if aux.Name != "" {
			t.Title = aux.Name
		} else if aux.TaskName != "" {
			t.Title = aux.TaskName
		}
	}
	return nil
}

func (s *StorySpec) UnmarshalJSON(data []byte) error {
	type Alias StorySpec
	aux := struct {
		*Alias
		StoryID string `json:"story_id"`
		Name    string `json:"name"`
	}{
		Alias: (*Alias)(s),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if s.ID == "" && aux.StoryID != "" {
		s.ID = aux.StoryID
	}
	if s.Title == "" && aux.Name != "" {
		s.Title = aux.Name
	}
	return nil
}

func (e *EpicSpec) UnmarshalJSON(data []byte) error {
	type Alias EpicSpec
	aux := struct {
		*Alias
		EpicID string `json:"epic_id"`
		Name   string `json:"name"`
	}{
		Alias: (*Alias)(e),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if e.ID == "" && aux.EpicID != "" {
		e.ID = aux.EpicID
	}
	if e.Title == "" && aux.Name != "" {
		e.Title = aux.Name
	}
	return nil
}

func (p *PlanSpec) UnmarshalJSON(data []byte) error {
	type Alias PlanSpec
	aux := struct {
		*Alias
		PlanID   string `json:"plan_id"`
		Name     string `json:"name"`
		PlanName string `json:"plan_name"`
	}{
		Alias: (*Alias)(p),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if p.ID == "" && aux.PlanID != "" {
		p.ID = aux.PlanID
	}
	if p.Title == "" {
		if aux.Name != "" {
			p.Title = aux.Name
		} else if aux.PlanName != "" {
			p.Title = aux.PlanName
		}
	}
	return nil
}

// ParsePlanSpec decodes a plan document, tolerating surrounding prose/fences,
// wrapper objects (e.g. {"plan": {...}}), and direct tasks under epics.
func ParsePlanSpec(raw string) (PlanSpec, error) {
	cleanJSON, err := ExtractJSON(raw)
	if err != nil {
		return PlanSpec{}, fmt.Errorf("malformed plan JSON: %w", err)
	}

	var rawMap map[string]any
	if err := json.Unmarshal([]byte(cleanJSON), &rawMap); err == nil {
		// Unwrap {"plan": ...}, {"plan_spec": ...}, {"spec": ...}, etc.
		for _, wrapper := range []string{"plan", "plan_spec", "spec", "workflow", "data"} {
			if sub, ok := rawMap[wrapper].(map[string]any); ok {
				rawMap = sub
				break
			}
		}
		// Normalize title
		if _, hasTitle := rawMap["title"]; !hasTitle {
			if name, ok := rawMap["name"].(string); ok {
				rawMap["title"] = name
			} else if planName, ok := rawMap["plan_name"].(string); ok {
				rawMap["title"] = planName
			}
		}
		// Normalize epics, stories, and tasks
		taskSeq := 1
		if epicsRaw, ok := rawMap["epics"].([]any); ok {
			for i, eRaw := range epicsRaw {
				if eMap, ok := eRaw.(map[string]any); ok {
					if _, hasID := eMap["id"]; !hasID {
						eMap["id"] = fmt.Sprintf("E%d", i+1)
					}
					stories, _ := eMap["stories"].([]any)
					tasks, hasTasks := eMap["tasks"].([]any)
					if len(stories) == 0 && hasTasks && len(tasks) > 0 {
						eMap["stories"] = []any{
							map[string]any{
								"id":    fmt.Sprintf("E%d.S1", i+1),
								"title": fmt.Sprintf("%v Story", eMap["title"]),
								"tasks": tasks,
							},
						}
						stories, _ = eMap["stories"].([]any)
					}

					for j, sRaw := range stories {
						if sMap, ok := sRaw.(map[string]any); ok {
							if _, hasID := sMap["id"]; !hasID {
								sMap["id"] = fmt.Sprintf("E%d.S%d", i+1, j+1)
							}
							if sTasks, ok := sMap["tasks"].([]any); ok {
								for _, tRaw := range sTasks {
									if tMap, ok := tRaw.(map[string]any); ok {
										// Normalize name -> id & title
										nameStr, _ := tMap["name"].(string)
										titleStr, _ := tMap["title"].(string)
										if titleStr == "" && nameStr != "" {
											tMap["title"] = nameStr
										}
										idStr, _ := tMap["id"].(string)
										if idStr == "" {
											candidate := nameStr
											if candidate == "" {
												candidate = titleStr
											}
											// If candidate starts with "T1:", "T2 -", etc.
											if strings.Contains(candidate, ":") {
												candidate = strings.TrimSpace(strings.Split(candidate, ":")[0])
											} else if strings.Contains(candidate, "-") {
												candidate = strings.TrimSpace(strings.Split(candidate, "-")[0])
											} else if candidate == "" {
												candidate = fmt.Sprintf("T%d", taskSeq)
											}
											tMap["id"] = candidate
										}
										taskSeq++
									}
								}
							}
						}
					}
				}
			}
		} else if tasksRaw, ok := rawMap["tasks"].([]any); ok && len(tasksRaw) > 0 {
			rawMap["epics"] = []any{
				map[string]any{
					"id":    "epic-1",
					"title": rawMap["title"],
					"stories": []any{
						map[string]any{
							"id":    "story-1",
							"title": rawMap["title"],
							"tasks": tasksRaw,
						},
					},
				},
			}
		}
		reBytes, _ := json.Marshal(rawMap)
		cleanJSON = string(reBytes)
	}

	spec, err := DecodeJSON[PlanSpec](cleanJSON)
	if err != nil {
		return PlanSpec{}, fmt.Errorf("malformed plan JSON: %w", err)
	}

	// Normalize task types if missing or approximate
	for i := range spec.Epics {
		for j := range spec.Epics[i].Stories {
			for k := range spec.Epics[i].Stories[j].Tasks {
				t := &spec.Epics[i].Stories[j].Tasks[k]
				tt := strings.ToLower(strings.TrimSpace(t.TaskType))
				switch tt {
				case "", "code", "coding", "code_generation", "codegen":
					t.TaskType = "code_gen"
				case "extract", "text", "text_extraction", "report", "markdown":
					t.TaskType = "text_extract"
				case "rag_query", "rag", "query", "search", "rag-query", "rag-search":
					t.TaskType = "rag_search"
				case "ingest", "index", "rag_ingest", "rag-index", "indexing":
					t.TaskType = "rag_index"
				}
			}
		}
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

