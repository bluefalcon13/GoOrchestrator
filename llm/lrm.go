package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

// LRMPersonaEngine wraps a single Large Reasoning Model client and lets it
// wear different "hats": each call selects a Persona (system prompt +
// sampling profile) and a structured output contract, decodes the JSON
// response into a strongly-typed Go value, validates it, and runs a bounded
// self-repair loop when the model violates the contract.
//
// LRMPersonaEngine is stateless apart from its immutable configuration and is
// safe for concurrent use.
type LRMPersonaEngine struct {
	client         LLMClient
	personas       map[Hat]Persona
	model          string
	repairAttempts int
	log            *slog.Logger
}

// LRMOption customises an LRMPersonaEngine.
type LRMOption func(*LRMPersonaEngine)

// WithPersona overrides the profile of one hat.
func WithPersona(p Persona) LRMOption {
	return func(e *LRMPersonaEngine) { e.personas[p.Hat] = p }
}

// WithLRMModel sets the model name sent with every LRM request.
func WithLRMModel(model string) LRMOption { return func(e *LRMPersonaEngine) { e.model = model } }

// WithRepairAttempts sets how many times a contract violation is fed back to
// the model for self-repair (default 2).
func WithRepairAttempts(n int) LRMOption {
	return func(e *LRMPersonaEngine) { e.repairAttempts = max(n, 0) }
}

// WithLRMLogger sets the logger.
func WithLRMLogger(l *slog.Logger) LRMOption { return func(e *LRMPersonaEngine) { e.log = l } }

// NewLRMPersonaEngine wraps client with multi-hat persona capabilities.
func NewLRMPersonaEngine(client LLMClient, opts ...LRMOption) *LRMPersonaEngine {
	e := &LRMPersonaEngine{
		client:         client,
		personas:       DefaultPersonas(),
		repairAttempts: 2,
		log:            slog.Default(),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Persona returns the active profile for a hat.
func (e *LRMPersonaEngine) Persona(h Hat) Persona { return e.personas[h] }

type hatCall struct {
	hat      Hat
	tag      string
	contract string
	prompt   string
	history  []Message
	meta     map[string]string
}

// wear executes one structured call under a given hat.
func wear[T any](ctx context.Context, e *LRMPersonaEngine, call hatCall, validate func(*T) error) (T, error) {
	var zero T
	persona, ok := e.personas[call.hat]
	if !ok {
		return zero, fmt.Errorf("no persona registered for hat %q", call.hat)
	}
	msgs := make([]Message, 0, len(call.history)+2)
	msgs = append(msgs, Message{Role: RoleSystem, Content: persona.systemMessage(call.contract)})
	msgs = append(msgs, call.history...)
	msgs = append(msgs, Message{Role: RoleUser, Content: call.prompt})

	start := time.Now()
	for attempt := 0; ; attempt++ {
		resp, err := e.client.Complete(ctx, CompletionRequest{
			Model:       e.model,
			Messages:    msgs,
			Temperature: persona.Temperature,
			MaxTokens:   persona.MaxTokens,
			JSONMode:    true,
			Tag:         call.tag,
			Metadata:    call.meta,
		})
		if err != nil {
			return zero, fmt.Errorf("%s hat: %w", call.hat, err)
		}
		out, perr := DecodeJSON[T](resp.Content)
		if perr == nil && validate != nil {
			perr = validate(&out)
		}
		if perr == nil {
			e.log.Debug("lrm hat call complete", "hat", call.hat, "attempts", attempt+1,
				"tokens", resp.Usage.TotalTokens, "elapsed", time.Since(start))
			return out, nil
		}
		if attempt >= e.repairAttempts {
			return zero, fmt.Errorf("%s hat: invalid structured output after %d attempt(s): %w", call.hat, attempt+1, perr)
		}
		e.log.Warn("lrm contract violation, requesting repair", "hat", call.hat, "attempt", attempt+1, "err", perr)
		msgs = append(msgs,
			Message{Role: RoleAssistant, Content: resp.Content},
			Message{Role: RoleUser, Content: fmt.Sprintf(
				"Your previous response was rejected: %v. Respond again with ONLY a single valid JSON object that satisfies the output contract.", perr)},
		)
	}
}

// ---------------------------------------------------------------------------
// Conversational hat
// ---------------------------------------------------------------------------

// Converse answers a user chat message (or classifies it as a new directive).
func (e *LRMPersonaEngine) Converse(ctx context.Context, in ConversationInput) (ConversationReply, error) {
	var b strings.Builder
	b.WriteString("## System state snapshot\n")
	b.WriteString(orNone(in.StateSummary))
	if in.FocusPlanID != "" {
		fmt.Fprintf(&b, "\n\nThe user is currently focused on plan %q.", in.FocusPlanID)
	}
	b.WriteString("\n\n## User message\n")
	b.WriteString(in.Message)

	return wear(ctx, e, hatCall{
		hat:      HatConversational,
		tag:      TagConversational,
		contract: contractConversation,
		prompt:   b.String(),
		history:  in.History,
		meta:     map[string]string{MetaMessage: in.Message, MetaStateSummary: in.StateSummary},
	}, func(r *ConversationReply) error {
		r.Reply = strings.TrimSpace(r.Reply)
		r.Directive = strings.TrimSpace(r.Directive)
		switch r.Intent {
		case "":
			r.Intent = IntentAnswer
		case IntentAnswer, IntentDraftPlan:
		default:
			return fmt.Errorf("intent must be %q or %q, got %q", IntentAnswer, IntentDraftPlan, r.Intent)
		}
		if r.Intent == IntentAnswer && r.Reply == "" {
			return errors.New("reply must not be empty")
		}
		if r.Intent == IntentDraftPlan && r.Directive == "" {
			r.Directive = strings.TrimSpace(in.Message)
		}
		return nil
	})
}

// InterpretClarification (Conversational hat) folds a user's answer into an
// amended, self-contained task specification.
func (e *LRMPersonaEngine) InterpretClarification(ctx context.Context, in ClarificationInput) (ClarificationInterpretation, error) {
	var b strings.Builder
	b.WriteString("Operation: clarification.\n\n## Paused task\n")
	writeTaskSpec(&b, in.Task)
	fmt.Fprintf(&b, "\n## What failed\n%s\n\n## Question asked\n%s\n", orNone(in.Explanation), orNone(in.Question))
	writeList(&b, "Reviewer feedback so far", in.Feedback)
	fmt.Fprintf(&b, "\n## User's answer\n%s\n", in.Answer)

	return wear(ctx, e, hatCall{
		hat:      HatConversational,
		tag:      TagClarification,
		contract: contractClarification,
		prompt:   b.String(),
		meta: map[string]string{
			MetaTaskID: in.Task.ID, MetaTitle: in.Task.Title, MetaDescription: in.Task.Description,
			MetaQuestion: in.Question, MetaAnswer: in.Answer,
		},
	}, func(r *ClarificationInterpretation) error {
		r.AmendedDescription = strings.TrimSpace(r.AmendedDescription)
		if r.AmendedDescription == "" {
			return errors.New("amended_description must not be empty")
		}
		if strings.TrimSpace(r.Acknowledgement) == "" {
			r.Acknowledgement = "Your clarification was applied and the task has been resumed."
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// Planner hat
// ---------------------------------------------------------------------------

// DraftPlan converts a directive into a hierarchical Epic/Story/Task plan.
func (e *LRMPersonaEngine) DraftPlan(ctx context.Context, in PlanningInput) (PlanSpec, error) {
	var b strings.Builder
	b.WriteString("## Registered task types (task_type must be one of these)\n")
	types := make([]string, 0, len(in.Capabilities))
	for _, c := range in.Capabilities {
		fmt.Fprintf(&b, "- %s: %s\n", c.TaskType, c.Description)
		types = append(types, c.TaskType)
	}
	if len(in.Capabilities) == 0 {
		b.WriteString("(none registered)\n")
	}
	if in.StateSummary != "" {
		fmt.Fprintf(&b, "\n## Existing system state (for context only)\n%s\n", truncate(in.StateSummary, 3000))
	}
	fmt.Fprintf(&b, "\n## Directive\n%s\n", in.Directive)
	if in.PreviousDraft != nil {
		prev, _ := json.MarshalIndent(in.PreviousDraft, "", "  ")
		fmt.Fprintf(&b, "\n## Previous draft (rejected by structural validation)\n%s\n", truncate(string(prev), 12000))
		writeList(&b, "Validation problems to fix", in.ValidationProblems)
	}

	return wear(ctx, e, hatCall{
		hat:      HatPlanner,
		tag:      TagPlanner,
		contract: contractPlan,
		prompt:   b.String(),
		meta: map[string]string{
			MetaDirective: in.Directive,
			MetaTaskTypes: strings.Join(types, ","),
		},
	}, func(p *PlanSpec) error {
		if len(p.Epics) == 0 || p.TaskCount() == 0 {
			return errors.New("plan must contain at least one epic with at least one task")
		}
		if strings.TrimSpace(p.Title) == "" {
			p.Title = truncate(in.Directive, 80)
		}
		if strings.TrimSpace(p.Objective) == "" {
			p.Objective = in.Directive
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// Project Manager hat
// ---------------------------------------------------------------------------

// Triage decides how to remediate an escalated task.
func (e *LRMPersonaEngine) Triage(ctx context.Context, in TriageInput) (Remediation, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "## Plan objective\n%s\n\n## Location\nEpic: %s\nStory: %s\n\n## Escalated task\n",
		orNone(in.PlanObjective), orNone(in.EpicTitle), orNone(in.StoryTitle))
	writeTaskSpec(&b, in.Task)
	fmt.Fprintf(&b, "\n## Escalation\nReason: %s\nAttempts used: %d of %d\n", in.Reason, in.Attempts, in.MaxAttempts)
	if in.LastError != "" {
		fmt.Fprintf(&b, "Last execution error: %s\n", in.LastError)
	}
	writeList(&b, "Reviewer feedback history", in.Feedback)
	writeList(&b, "Clarifications already provided by the user", in.Clarifications)
	if in.LastOutput != "" {
		fmt.Fprintf(&b, "\n## Last worker output (truncated)\n%s\n", truncate(in.LastOutput, 6000))
	}
	b.WriteString("\n## Registered task types\n")
	for _, c := range in.Capabilities {
		fmt.Fprintf(&b, "- %s: %s\n", c.TaskType, c.Description)
	}
	fmt.Fprintf(&b, "\n## Existing plan task IDs\n%s\n", strings.Join(in.ExistingTaskIDs, ", "))
	if in.CanSplit {
		b.WriteString("\nSplitting IS permitted for this task.\n")
	} else {
		b.WriteString("\nSplitting is NOT permitted for this task (maximum decomposition depth reached). Choose clarify or fail.\n")
	}

	return wear(ctx, e, hatCall{
		hat:      HatProjectManager,
		tag:      TagProjectManager,
		contract: contractRemediation,
		prompt:   b.String(),
		meta: map[string]string{
			MetaTaskID: in.Task.ID, MetaTitle: in.Task.Title, MetaDescription: in.Task.Description,
			MetaTaskType: in.Task.TaskType, MetaReason: in.Reason, MetaCanSplit: strconv.FormatBool(in.CanSplit),
			MetaFeedback: lastOf(in.Feedback), MetaAttempt: strconv.Itoa(in.Attempts),
		},
	}, func(r *Remediation) error {
		r.Action = RemediationAction(strings.ToLower(strings.TrimSpace(string(r.Action))))
		switch r.Action {
		case ActionSplit:
			if !in.CanSplit {
				return errors.New("action split is not permitted for this task; choose clarify or fail")
			}
			if len(r.SubTasks) < 1 {
				return errors.New("action split requires at least one sub_task")
			}
		case ActionClarify:
			if strings.TrimSpace(r.Question) == "" {
				return errors.New("action clarify requires a non-empty question")
			}
		case ActionFail:
		default:
			return fmt.Errorf("action must be split, clarify or fail; got %q", r.Action)
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// Reviewer hat
// ---------------------------------------------------------------------------

// Review validates a task output against its acceptance criteria.
func (e *LRMPersonaEngine) Review(ctx context.Context, in ReviewInput) (ReviewVerdict, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "## Plan objective\n%s\n\n## Location\nEpic: %s\nStory: %s\n\n## Task under review (attempt %d of %d)\n",
		orNone(in.PlanObjective), orNone(in.EpicTitle), orNone(in.StoryTitle), in.Attempt, in.MaxAttempts)
	writeTaskSpec(&b, in.Task)
	writeList(&b, "Feedback given on previous attempts", in.PriorFeedback)
	writeList(&b, "User clarifications", in.Clarifications)
	fmt.Fprintf(&b, "\n## Worker output\n%s\n", orNone(in.Output))

	return wear(ctx, e, hatCall{
		hat:      HatReviewer,
		tag:      TagReviewer,
		contract: contractReview,
		prompt:   b.String(),
		meta: map[string]string{
			MetaTaskID: in.Task.ID, MetaTitle: in.Task.Title, MetaDescription: in.Task.Description,
			MetaOutput: in.Output, MetaAttempt: strconv.Itoa(in.Attempt),
			MetaHasClarification: strconv.FormatBool(len(in.Clarifications) > 0),
		},
	}, func(v *ReviewVerdict) error {
		v.normalize()
		return nil
	})
}

// ---------------------------------------------------------------------------
// prompt helpers
// ---------------------------------------------------------------------------

func writeTaskSpec(b *strings.Builder, t TaskSpec) {
	fmt.Fprintf(b, "ID: %s\nTitle: %s\nType: %s\nDescription:\n%s\n", t.ID, t.Title, t.TaskType, orNone(t.Description))
	writeList(b, "Acceptance criteria", t.AcceptanceCriteria)
	if len(t.Params) > 0 && string(t.Params) != "null" {
		fmt.Fprintf(b, "Params: %s\n", truncate(string(t.Params), 2000))
	}
}

func writeList(b *strings.Builder, title string, items []string) {
	if len(items) == 0 {
		return
	}
	fmt.Fprintf(b, "\n%s:\n", title)
	for _, it := range items {
		fmt.Fprintf(b, "- %s\n", it)
	}
}

func orNone(s string) string {
	if strings.TrimSpace(s) == "" {
		return "(none)"
	}
	return s
}

func lastOf(items []string) string {
	if len(items) == 0 {
		return ""
	}
	return items[len(items)-1]
}

