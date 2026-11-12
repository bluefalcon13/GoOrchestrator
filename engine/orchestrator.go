package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"go-orchestrator/llm"
	"go-orchestrator/tasks"
)

// Orchestrator coordinates the single-writer actor event loop, DAG scheduler,
// worker dispatch, and multi-tier LRM review/escalation loop.
type Orchestrator struct {
	lrm          *llm.LRMPersonaEngine
	workerClient llm.LLMClient
	registry     *tasks.Registry
	eventCh      chan any
	plans        map[string]*PlanNode
	draftPlans   map[string]llm.PlanSpec
	maxRetries   int
	log          *slog.Logger
	wg           sync.WaitGroup
}

// NewOrchestrator creates an Orchestrator instance.
func NewOrchestrator(
	lrm *llm.LRMPersonaEngine,
	workerClient llm.LLMClient,
	registry *tasks.Registry,
	eventCh chan any,
) *Orchestrator {
	return &Orchestrator{
		lrm:          lrm,
		workerClient: workerClient,
		registry:     registry,
		eventCh:      eventCh,
		plans:        make(map[string]*PlanNode),
		draftPlans:   make(map[string]llm.PlanSpec),
		maxRetries:   3,
		log:          slog.Default(),
	}
}

// RunEventLoop is the single-writer actor event loop. It blocks until a SigEvent
// or cancellation signal is received, then initiates graceful shutdown.
func (o *Orchestrator) RunEventLoop(ctx context.Context, srvShutdown func(context.Context) error) error {
	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	defer cancelWorkers()

	o.log.Info("actor event loop ignited, waiting for events...")

	for {
		select {
		case <-ctx.Done():
			o.log.Info("root context canceled, initiating graceful shutdown")
			return o.shutdown(cancelWorkers, srvShutdown)

		case event, ok := <-o.eventCh:
			if !ok {
				o.log.Info("event channel closed, shutting down")
				return o.shutdown(cancelWorkers, srvShutdown)
			}

			switch ev := event.(type) {
			case SigEvent:
				o.log.Info("received termination signal", "signal", ev.Signal)
				return o.shutdown(cancelWorkers, srvShutdown)

			case FatalErrorEvent:
				o.log.Error("fatal component error received", "err", ev.Err)
				return o.shutdown(cancelWorkers, srvShutdown)

			case SeedPlanEvent:
				o.handleSeedPlan(workerCtx, ev)

			case TaskExecutionResultEvent:
				o.handleTaskExecutionResult(workerCtx, ev)

			case TaskReviewResultEvent:
				o.handleTaskReviewResult(workerCtx, ev)

			case TaskEscalatedEvent:
				o.handleTaskEscalated(workerCtx, ev)

			case RemediationResultEvent:
				o.handleRemediationResult(workerCtx, ev)

			case ClarificationResolvedEvent:
				o.handleClarificationResolved(workerCtx, ev)

			case GetPlanQuery:
				plan, exists := o.plans[ev.PlanID]
				if !exists {
					if draft, ok := o.draftPlans[ev.PlanID]; ok {
						draftNode := o.createPlanNode(draft)
						draftNode.Status = PlanStatusDraft
						plan = draftNode
						exists = true
					}
				}
				ev.ReplyCh <- GetPlanResult{Plan: clonePlan(plan), Found: exists}

			case ListPlansQuery:
				list := make([]*PlanNode, 0, len(o.plans)+len(o.draftPlans))
				for _, p := range o.plans {
					list = append(list, clonePlan(p))
				}
				for _, d := range o.draftPlans {
					draftNode := o.createPlanNode(d)
					draftNode.Status = PlanStatusDraft
					list = append(list, draftNode)
				}
				sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt.Before(list[j].CreatedAt) })
				ev.ReplyCh <- list

			case GetSystemStateQuery:
				ev.ReplyCh <- o.formatSystemStateSummary()

			case GetDraftPlansQuery:
				drafts := make([]llm.PlanSpec, 0, len(o.draftPlans))
				for _, d := range o.draftPlans {
					drafts = append(drafts, d)
				}
				ev.ReplyCh <- drafts

			case StoreDraftPlanCommand:
				o.handleStoreDraftPlan(ev)

			case PausePlanCommand:
				o.handlePausePlan(ev)

			case ResumePlanCommand:
				o.handleResumePlan(ctx, ev)

			default:
				o.log.Warn("unhandled event type", "type", fmt.Sprintf("%T", event))
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Graceful Shutdown
// ---------------------------------------------------------------------------

func (o *Orchestrator) shutdown(cancelWorkers context.CancelFunc, srvShutdown func(context.Context) error) error {
	o.log.Info("commencing graceful shutdown sequence")

	// 1. Cancel in-flight workers
	cancelWorkers()

	// 2. Shut down HTTP server
	if srvShutdown != nil {
		shutdownCtx, srvCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer srvCancel()
		if err := srvShutdown(shutdownCtx); err != nil {
			o.log.Error("error during HTTP server shutdown", "err", err)
		}
	}

	// 3. Drain active worker goroutines with bounded timeout
	drainCh := make(chan struct{})
	go func() {
		o.wg.Wait()
		close(drainCh)
	}()

	select {
	case <-drainCh:
		o.log.Info("all background worker tasks drained cleanly")
	case <-time.After(5 * time.Second):
		o.log.Warn("worker drain timeout reached, forcing exit")
	}

	return nil
}

// ---------------------------------------------------------------------------
// Plan Ingestion and Seeding
// ---------------------------------------------------------------------------

func (o *Orchestrator) handleSeedPlan(ctx context.Context, ev SeedPlanEvent) {
	var spec llm.PlanSpec
	var err error

	if strings.TrimSpace(ev.RawJSON) != "" {
		spec, err = llm.ParsePlanSpec(ev.RawJSON)
		if err != nil {
			o.log.Error("failed to parse plan JSON", "err", err)
			if ev.ReplyCh != nil {
				ev.ReplyCh <- CommandResult{Success: false, Message: "failed to parse plan JSON", Err: err}
			}
			return
		}
	} else if ev.PlanID != "" {
		draft, ok := o.draftPlans[ev.PlanID]
		if !ok {
			// Fuzzy auto-healing: prefix/substring match or single draft fallback
			for dID, d := range o.draftPlans {
				if strings.HasPrefix(dID, ev.PlanID) || strings.HasPrefix(ev.PlanID, dID) || strings.Contains(dID, ev.PlanID) {
					draft = d
					ok = true
					ev.PlanID = dID
					break
				}
			}
			if !ok && len(o.draftPlans) == 1 {
				for dID, d := range o.draftPlans {
					draft = d
					ok = true
					ev.PlanID = dID
					break
				}
			}
		}
		if !ok {
			o.log.Error("draft plan not found in registry", "plan_id", ev.PlanID)
			if ev.ReplyCh != nil {
				ev.ReplyCh <- CommandResult{Success: false, Message: fmt.Sprintf("draft plan %q not found", ev.PlanID), Err: fmt.Errorf("draft plan not found")}
			}
			return
		}
		spec = draft
	} else if len(o.draftPlans) == 1 {
		// PlanID was omitted, auto-resolve single pending draft
		for dID, d := range o.draftPlans {
			spec = d
			ev.PlanID = dID
			break
		}
	} else {
		o.log.Error("seed plan requires either RawJSON or PlanID")
		if ev.ReplyCh != nil {
			ev.ReplyCh <- CommandResult{Success: false, Message: "missing plan specification", Err: fmt.Errorf("neither RawJSON nor PlanID provided")}
		}
		return
	}

	if ev.PlanID != "" {
		spec.ID = ev.PlanID
	}
	if spec.ID == "" {
		spec.ID = GeneratePlanSlug(spec.Title)
	}

	// Run Go-side structural DAG validation
	if _, err := ValidatePlanSpec(spec, o.registry); err != nil {
		o.log.Error("plan failed structural DAG validation", "plan_id", spec.ID, "err", err)
		if ev.ReplyCh != nil {
			ev.ReplyCh <- CommandResult{Success: false, Message: "structural DAG validation failed", Err: err}
		}
		return
	}

	// Convert to active execution tree
	planNode := o.createPlanNode(spec)
	o.plans[planNode.ID] = planNode
	delete(o.draftPlans, planNode.ID)

	o.log.Info("plan accepted into active execution", "plan_id", planNode.ID, "title", planNode.Title)
	if ev.ReplyCh != nil {
		ev.ReplyCh <- CommandResult{Success: true, Message: "plan accepted into execution", Data: planNode.ID}
	}

	// Evaluate DAG and dispatch initial ready tasks
	o.evaluateAndDispatch(ctx, planNode)
}

// GeneratePlanSlug creates a normalized plan ID slug from its title.
func GeneratePlanSlug(title string) string {
	clean := strings.ToLower(title)
	for _, word := range []string{"forensic", "analysis", "plan:", "investigation", "root cause", "pipeline", "specialized"} {
		clean = strings.ReplaceAll(clean, word, "")
	}

	var sb strings.Builder
	sb.WriteString("plan-")
	lastDash := true
	count := 0
	for _, r := range clean {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			sb.WriteRune(r)
			lastDash = false
			count++
			if count >= 30 {
				break
			}
		} else if !lastDash {
			sb.WriteRune('-')
			lastDash = true
		}
	}
	res := strings.Trim(sb.String(), "-")
	if res == "plan" || res == "" {
		res = "plan-workflow"
	}

	hash := sha256.Sum256([]byte(fmt.Sprintf("%s-%d", title, time.Now().UnixNano())))
	suffix := hex.EncodeToString(hash[:2]) // 4 hex chars
	return fmt.Sprintf("%s-%s", res, suffix)
}

func (o *Orchestrator) handleStoreDraftPlan(ev StoreDraftPlanCommand) {
	spec := ev.Plan
	if spec.ID == "" {
		spec.ID = GeneratePlanSlug(spec.Title)
	}
	o.draftPlans[spec.ID] = spec
	ev.ReplyCh <- CommandResult{Success: true, Message: "draft plan saved", Data: spec}
}

func (o *Orchestrator) handlePausePlan(ev PausePlanCommand) {
	var target *PlanNode
	if ev.PlanID != "" {
		target = o.plans[ev.PlanID]
	} else {
		for _, p := range o.plans {
			if p.Status == PlanStatusRunning || p.Status == PlanStatusReady {
				target = p
				break
			}
		}
	}

	if target == nil {
		if ev.ReplyCh != nil {
			ev.ReplyCh <- CommandResult{Success: false, Message: "no active plan found to pause", Err: fmt.Errorf("plan not found")}
		}
		return
	}

	target.Status = PlanStatusPaused
	target.UpdatedAt = time.Now()
	o.log.Info("plan execution paused", "plan_id", target.ID)
	if ev.ReplyCh != nil {
		ev.ReplyCh <- CommandResult{Success: true, Message: fmt.Sprintf("plan %s paused", target.ID), Data: target.ID}
	}
}

func (o *Orchestrator) handleResumePlan(ctx context.Context, ev ResumePlanCommand) {
	var target *PlanNode
	if ev.PlanID != "" {
		target = o.plans[ev.PlanID]
	} else {
		for _, p := range o.plans {
			if p.Status == PlanStatusPaused {
				target = p
				break
			}
		}
	}

	if target == nil {
		if ev.ReplyCh != nil {
			ev.ReplyCh <- CommandResult{Success: false, Message: "no paused plan found to resume", Err: fmt.Errorf("plan not found")}
		}
		return
	}

	target.Status = PlanStatusRunning
	target.UpdatedAt = time.Now()
	o.log.Info("plan execution resumed", "plan_id", target.ID)
	if ev.ReplyCh != nil {
		ev.ReplyCh <- CommandResult{Success: true, Message: fmt.Sprintf("plan %s resumed", target.ID), Data: target.ID}
	}
	o.evaluateAndDispatch(ctx, target)
}

func (o *Orchestrator) createPlanNode(spec llm.PlanSpec) *PlanNode {
	now := time.Now()
	p := &PlanNode{
		ID:        spec.ID,
		Title:     spec.Title,
		Objective: spec.Objective,
		Status:    PlanStatusReady,
		CreatedAt: now,
		UpdatedAt: now,
	}

	for _, e := range spec.Epics {
		epicNode := &EpicNode{
			ID:          e.ID,
			Title:       e.Title,
			Description: e.Description,
			Status:      PlanStatusReady,
		}
		for _, s := range e.Stories {
			storyNode := &StoryNode{
				ID:          s.ID,
				Title:       s.Title,
				Description: s.Description,
				Status:      PlanStatusReady,
			}
			for _, t := range s.Tasks {
				taskNode := &TaskNode{
					ID:                 t.ID,
					Title:              t.Title,
					Description:        t.Description,
					TaskType:           t.TaskType,
					DependsOn:          append([]string(nil), t.DependsOn...),
					AcceptanceCriteria: append([]string(nil), t.AcceptanceCriteria...),
					Params:             t.Params,
					Status:             StatusPending,
					MaxAttempts:        o.maxRetries,
					Artifacts:          make(map[string]string),
					CreatedAt:          now,
					UpdatedAt:          now,
				}
				storyNode.Tasks = append(storyNode.Tasks, taskNode)
			}
			epicNode.Stories = append(epicNode.Stories, storyNode)
		}
		p.Epics = append(p.Epics, epicNode)
	}
	return p
}

// ---------------------------------------------------------------------------
// DAG Evaluation and Asynchronous Task Dispatch
// ---------------------------------------------------------------------------

func (o *Orchestrator) evaluateAndDispatch(ctx context.Context, plan *PlanNode) {
	if plan.Status == PlanStatusPaused {
		o.log.Info("plan is paused, skipping task dispatch", "plan_id", plan.ID)
		return
	}

	allTasks := o.flattenTasks(plan)
	taskMap := make(map[string]*TaskNode, len(allTasks))
	for _, t := range allTasks {
		taskMap[t.ID] = t
	}

	var toDispatch []*TaskNode

	for _, task := range allTasks {
		if task.Status != StatusPending {
			continue
		}

		// Check if all upstream dependencies are Completed
		depsSatisfied := true
		for _, depID := range task.DependsOn {
			depTask, exists := taskMap[depID]
			if !exists || depTask.Status != StatusCompleted {
				depsSatisfied = false
				break
			}
		}

		if depsSatisfied {
			if task.IsJoinNode {
				// Join nodes complete automatically once their child sub-tasks finish
				task.Status = StatusCompleted
				task.UpdatedAt = time.Now()
				o.log.Info("join node completed automatically", "plan_id", plan.ID, "task_id", task.ID)
			} else {
				task.Status = StatusReady
				task.UpdatedAt = time.Now()
				toDispatch = append(toDispatch, task)
			}
		}
	}

	o.rollupPlan(plan)

	for _, task := range toDispatch {
		o.dispatchTask(ctx, plan, task, taskMap)
	}
}

func (o *Orchestrator) dispatchTask(
	ctx context.Context,
	plan *PlanNode,
	task *TaskNode,
	taskMap map[string]*TaskNode,
) {
	handler, ok := o.registry.Get(task.TaskType)
	if !ok {
		o.log.Error("no handler found for task type", "task_id", task.ID, "type", task.TaskType)
		task.Status = StatusFailed
		task.LastError = fmt.Sprintf("missing handler for %s", task.TaskType)
		o.rollupPlan(plan)
		return
	}

	task.Status = StatusRunning
	task.Attempts++
	task.UpdatedAt = time.Now()
	o.rollupPlan(plan)

	// Collect outputs of direct upstream dependencies
	upstreamOutputs := make(map[string]string)
	for _, depID := range task.DependsOn {
		if dep, exists := taskMap[depID]; exists {
			upstreamOutputs[depID] = dep.Output
		}
	}

	input := tasks.TaskInput{
		TaskID:          task.ID,
		Title:           task.Title,
		Description:     task.Description,
		TaskType:        task.TaskType,
		Params:          task.Params,
		UpstreamOutputs: upstreamOutputs,
		Feedback:        append([]string(nil), task.FeedbackHistory...),
		Clarifications:  append([]string(nil), task.Clarifications...),
		Attempt:         task.Attempts,
	}

	planID := plan.ID
	taskID := task.ID
	attempt := task.Attempts

	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		o.log.Info("dispatching task to worker", "plan_id", planID, "task_id", taskID, "attempt", attempt)

		output, err := handler.Execute(ctx, input)
		if ctx.Err() != nil {
			return // Context canceled during shutdown
		}

		o.eventCh <- TaskExecutionResultEvent{
			PlanID:  planID,
			TaskID:  taskID,
			Output:  output,
			Err:     err,
			Attempt: attempt,
		}
	}()
}

// ---------------------------------------------------------------------------
// Execution, Review, and Escalation Handlers
// ---------------------------------------------------------------------------

func (o *Orchestrator) handleTaskExecutionResult(ctx context.Context, ev TaskExecutionResultEvent) {
	plan, ok := o.plans[ev.PlanID]
	if !ok {
		return
	}
	task := o.findTask(plan, ev.TaskID)
	if task == nil || task.Status != StatusRunning {
		return
	}

	if ev.Err != nil {
		o.log.Warn("task execution yielded error", "plan_id", ev.PlanID, "task_id", ev.TaskID, "err", ev.Err)
		task.LastError = ev.Err.Error()
		if task.Attempts < task.MaxAttempts {
			// Tactical retry
			task.Status = StatusPending
			o.evaluateAndDispatch(ctx, plan)
		} else {
			// Strategic escalation to Project Manager
			o.eventCh <- TaskEscalatedEvent{
				PlanID:   ev.PlanID,
				TaskID:   ev.TaskID,
				Reason:   fmt.Sprintf("Worker execution error: %v", ev.Err),
				Attempts: task.Attempts,
			}
		}
		return
	}

	// Execution succeeded: store output and dispatch for Review
	task.Output = ev.Output.Content
	task.Artifacts = ev.Output.Artifacts
	task.Status = StatusReviewing
	task.UpdatedAt = time.Now()
	o.rollupPlan(plan)

	// Async Reviewer Hat call
	epic, story := o.findTaskParents(plan, task.ID)
	reviewIn := llm.ReviewInput{
		PlanObjective:  plan.Objective,
		EpicTitle:      epic.Title,
		StoryTitle:     story.Title,
		Task:           o.taskToSpec(task),
		Attempt:        task.Attempts,
		MaxAttempts:    task.MaxAttempts,
		Output:         task.Output,
		PriorFeedback:  append([]string(nil), task.FeedbackHistory...),
		Clarifications: append([]string(nil), task.Clarifications...),
	}

	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		verdict, err := o.lrm.Review(ctx, reviewIn)
		if ctx.Err() != nil {
			return
		}
		o.eventCh <- TaskReviewResultEvent{
			PlanID:  ev.PlanID,
			TaskID:  ev.TaskID,
			Verdict: verdict,
			Err:     err,
			Attempt: ev.Attempt,
		}
	}()
}

func (o *Orchestrator) handleTaskReviewResult(ctx context.Context, ev TaskReviewResultEvent) {
	plan, ok := o.plans[ev.PlanID]
	if !ok {
		return
	}
	task := o.findTask(plan, ev.TaskID)
	if task == nil || task.Status != StatusReviewing {
		return
	}

	if ev.Err != nil {
		o.log.Error("reviewer hat encountered error", "plan_id", ev.PlanID, "task_id", ev.TaskID, "err", ev.Err)
		task.LastError = "Review error: " + ev.Err.Error()
		task.Status = StatusPending
		o.evaluateAndDispatch(ctx, plan)
		return
	}

	task.LastScore = ev.Verdict.Score

	if ev.Verdict.Approved {
		o.log.Info("task passed review", "plan_id", ev.PlanID, "task_id", ev.TaskID, "score", ev.Verdict.Score)
		task.Status = StatusCompleted
		task.UpdatedAt = time.Now()
		o.evaluateAndDispatch(ctx, plan)
		return
	}

	// Rejected by Reviewer
	o.log.Warn("task rejected by reviewer", "plan_id", ev.PlanID, "task_id", ev.TaskID,
		"needs_clarification", ev.Verdict.NeedsClarification, "feedback", ev.Verdict.Feedback)

	if ev.Verdict.Feedback != "" {
		task.FeedbackHistory = append(task.FeedbackHistory, ev.Verdict.Feedback)
	}

	if ev.Verdict.NeedsClarification {
		// Needs Clarification immediately triggers strategic escalation
		o.eventCh <- TaskEscalatedEvent{
			PlanID:   ev.PlanID,
			TaskID:   ev.TaskID,
			Reason:   "needs_clarification",
			Attempts: task.Attempts,
		}
		return
	}

	// Tactical Retry vs Strategic Escalation
	if task.Attempts < task.MaxAttempts {
		o.log.Info("scheduling tactical retry", "plan_id", ev.PlanID, "task_id", ev.TaskID, "next_attempt", task.Attempts+1)
		task.Status = StatusPending
		o.evaluateAndDispatch(ctx, plan)
	} else {
		o.log.Warn("max retries exhausted, escalating to Project Manager", "plan_id", ev.PlanID, "task_id", ev.TaskID)
		o.eventCh <- TaskEscalatedEvent{
			PlanID:   ev.PlanID,
			TaskID:   ev.TaskID,
			Reason:   fmt.Sprintf("Exhausted %d attempts. Last feedback: %s", task.Attempts, ev.Verdict.Feedback),
			Attempts: task.Attempts,
		}
	}
}

func (o *Orchestrator) handleTaskEscalated(ctx context.Context, ev TaskEscalatedEvent) {
	plan, ok := o.plans[ev.PlanID]
	if !ok {
		return
	}
	task := o.findTask(plan, ev.TaskID)
	if task == nil {
		return
	}

	epic, story := o.findTaskParents(plan, task.ID)
	allTasks := o.flattenTasks(plan)
	existingIDs := make([]string, 0, len(allTasks))
	for _, t := range allTasks {
		existingIDs = append(existingIDs, t.ID)
	}

	caps := o.registry.Capabilities()
	capList := make([]llm.Capability, 0, len(caps))
	for k, desc := range caps {
		capList = append(capList, llm.Capability{TaskType: k, Description: desc})
	}

	canSplit := !strings.Contains(task.ID, ".s") // Don't split infinitely deep sub-tasks

	triageIn := llm.TriageInput{
		PlanObjective:   plan.Objective,
		EpicTitle:       epic.Title,
		StoryTitle:      story.Title,
		Task:            o.taskToSpec(task),
		Reason:          ev.Reason,
		Attempts:        ev.Attempts,
		MaxAttempts:     task.MaxAttempts,
		Feedback:        append([]string(nil), task.FeedbackHistory...),
		LastOutput:      task.Output,
		LastError:       task.LastError,
		CanSplit:        canSplit,
		Capabilities:    capList,
		ExistingTaskIDs: existingIDs,
		Clarifications:  append([]string(nil), task.Clarifications...),
	}

	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		remediation, err := o.lrm.Triage(ctx, triageIn)
		if ctx.Err() != nil {
			return
		}
		o.eventCh <- RemediationResultEvent{
			PlanID:      ev.PlanID,
			TaskID:      ev.TaskID,
			Remediation: remediation,
			Err:         err,
		}
	}()
}

func (o *Orchestrator) handleRemediationResult(ctx context.Context, ev RemediationResultEvent) {
	plan, ok := o.plans[ev.PlanID]
	if !ok {
		return
	}
	task := o.findTask(plan, ev.TaskID)
	if task == nil {
		return
	}

	if ev.Err != nil {
		o.log.Error("project manager triage error, pausing for clarification", "err", ev.Err)
		task.Status = StatusAwaitingClarification
		task.PendingQuestion = "The task repeatedly failed and automated triage failed. Please provide guidance."
		o.blockTransitiveDependents(plan, task.ID)
		o.rollupPlan(plan)
		return
	}

	o.log.Info("project manager returned remediation", "action", ev.Remediation.Action, "rationale", ev.Remediation.Rationale)

	switch ev.Remediation.Action {
	case llm.ActionSplit:
		if err := o.spliceSubTasks(plan, task, ev.Remediation.SubTasks); err != nil {
			o.log.Warn("sub-task splicing failed, falling back to clarification", "err", err)
			task.Status = StatusAwaitingClarification
			task.PendingExplanation = "Failed to decompose task: " + err.Error()
			task.PendingQuestion = "Please provide clarifying instructions for this task."
			o.blockTransitiveDependents(plan, task.ID)
		} else {
			o.evaluateAndDispatch(ctx, plan)
		}

	case llm.ActionClarify:
		task.Status = StatusAwaitingClarification
		task.PendingExplanation = ev.Remediation.Explanation
		task.PendingQuestion = ev.Remediation.Question
		task.UpdatedAt = time.Now()
		o.blockTransitiveDependents(plan, task.ID)
		o.rollupPlan(plan)

	case llm.ActionFail:
		task.Status = StatusFailed
		task.LastError = ev.Remediation.Explanation
		task.UpdatedAt = time.Now()
		o.blockTransitiveDependents(plan, task.ID)
		o.rollupPlan(plan)
	}
}

func (o *Orchestrator) handleClarificationResolved(ctx context.Context, ev ClarificationResolvedEvent) {
	plan, ok := o.plans[ev.PlanID]
	if !ok {
		if ev.ReplyCh != nil {
			ev.ReplyCh <- CommandResult{Success: false, Message: "plan not found"}
		}
		return
	}
	task := o.findTask(plan, ev.TaskID)
	if task == nil {
		if ev.ReplyCh != nil {
			ev.ReplyCh <- CommandResult{Success: false, Message: "task not found"}
		}
		return
	}

	o.log.Info("applying clarification to paused task", "plan_id", ev.PlanID, "task_id", ev.TaskID)

	task.Description = ev.Interpretation.AmendedDescription
	task.AcceptanceCriteria = append(task.AcceptanceCriteria, ev.Interpretation.AdditionalAcceptanceCriteria...)
	task.Clarifications = append(task.Clarifications, ev.Answer)
	task.PendingExplanation = ""
	task.PendingQuestion = ""
	task.Attempts = 0
	task.Status = StatusPending
	task.UpdatedAt = time.Now()

	// Unblock downstream dependents so they can execute once this task completes
	downstreamMap, _ := BuildAdjacencyLists(plan)
	dependents := FindTransitiveDependents(task.ID, downstreamMap)
	for _, depID := range dependents {
		if dep := o.findTask(plan, depID); dep != nil && dep.Status == StatusBlocked {
			dep.Status = StatusPending
			dep.UpdatedAt = time.Now()
		}
	}

	o.evaluateAndDispatch(ctx, plan)

	if ev.ReplyCh != nil {
		ev.ReplyCh <- CommandResult{
			Success: true,
			Message: ev.Interpretation.Acknowledgement,
			Data:    task,
		}
	}
}

// ---------------------------------------------------------------------------
// Dynamic Sub-Task Splicing
// ---------------------------------------------------------------------------

func (o *Orchestrator) spliceSubTasks(plan *PlanNode, task *TaskNode, subTasks []llm.TaskSpec) error {
	_, story := o.findTaskParents(plan, task.ID)
	if story == nil {
		return fmt.Errorf("parent story for task %s not found", task.ID)
	}

	idPrefix := task.ID + "."
	localMap := make(map[string]string)
	for _, sub := range subTasks {
		localMap[sub.ID] = idPrefix + sub.ID
	}

	now := time.Now()
	var newNodes []*TaskNode
	terminalSubIDs := make([]string, 0, len(subTasks))
	isTargetedBySibling := make(map[string]bool)

	for _, sub := range subTasks {
		newID := localMap[sub.ID]
		var mappedDeps []string
		for _, d := range sub.DependsOn {
			if target, ok := localMap[d]; ok {
				mappedDeps = append(mappedDeps, target)
				isTargetedBySibling[target] = true
			} else {
				mappedDeps = append(mappedDeps, d)
			}
		}
		if len(mappedDeps) == 0 {
			// Inherit original task's upstream dependencies
			mappedDeps = append([]string(nil), task.DependsOn...)
		}

		node := &TaskNode{
			ID:                 newID,
			Title:              sub.Title,
			Description:        sub.Description,
			TaskType:           sub.TaskType,
			DependsOn:          mappedDeps,
			AcceptanceCriteria: append([]string(nil), sub.AcceptanceCriteria...),
			Params:             sub.Params,
			Status:             StatusPending,
			MaxAttempts:        o.maxRetries,
			Artifacts:          make(map[string]string),
			CreatedAt:          now,
			UpdatedAt:          now,
		}
		newNodes = append(newNodes, node)
	}

	for _, n := range newNodes {
		if !isTargetedBySibling[n.ID] {
			terminalSubIDs = append(terminalSubIDs, n.ID)
		}
	}
	if len(terminalSubIDs) == 0 {
		for _, n := range newNodes {
			terminalSubIDs = append(terminalSubIDs, n.ID)
		}
	}

	// Splice new nodes into story
	story.Tasks = append(story.Tasks, newNodes...)

	// Configure original task as join node
	task.IsJoinNode = true
	task.SubTaskIDs = make([]string, 0, len(newNodes))
	for _, n := range newNodes {
		task.SubTaskIDs = append(task.SubTaskIDs, n.ID)
	}
	task.DependsOn = terminalSubIDs
	task.Status = StatusPending
	task.UpdatedAt = now

	// Validate resulting DAG
	spec := o.planToSpec(plan)
	if _, err := ValidatePlanSpec(spec, o.registry); err != nil {
		// Roll back on validation failure
		story.Tasks = story.Tasks[:len(story.Tasks)-len(newNodes)]
		task.IsJoinNode = false
		task.SubTaskIDs = nil
		return fmt.Errorf("spliced plan DAG invalid: %w", err)
	}

	o.log.Info("successfully spliced sub-tasks into DAG", "original_task", task.ID, "sub_tasks_count", len(newNodes))
	return nil
}

func (o *Orchestrator) blockTransitiveDependents(plan *PlanNode, rootTaskID string) {
	downstreamMap, _ := BuildAdjacencyLists(plan)
	dependents := FindTransitiveDependents(rootTaskID, downstreamMap)
	now := time.Now()
	for _, depID := range dependents {
		if dep := o.findTask(plan, depID); dep != nil {
			dep.Status = StatusBlocked
			dep.UpdatedAt = now
		}
	}
}

// ---------------------------------------------------------------------------
// Status Rollups and Helpers
// ---------------------------------------------------------------------------

func (o *Orchestrator) rollupPlan(plan *PlanNode) {
	if plan.Status == PlanStatusPaused {
		plan.UpdatedAt = time.Now()
		return
	}

	for _, epic := range plan.Epics {
		for _, story := range epic.Stories {
			story.Status = computeAggregateStatus(storyTasksToStatuses(story.Tasks))
		}
		epic.Status = computeAggregateStatus(storyNodesToStatuses(epic.Stories))
	}
	plan.Status = computeAggregateStatus(epicNodesToStatuses(plan.Epics))
	plan.UpdatedAt = time.Now()
}

func computeAggregateStatus(statuses []string) PlanStatus {
	if len(statuses) == 0 {
		return PlanStatusPending
	}
	allCompleted := true
	anyFailed := false
	anyAwaiting := false
	anyRunning := false
	anyReady := false
	allBlocked := true

	for _, s := range statuses {
		if s != string(StatusCompleted) {
			allCompleted = false
		}
		if s == string(StatusFailed) {
			anyFailed = true
		}
		if s == string(StatusAwaitingClarification) {
			anyAwaiting = true
		}
		if s == string(StatusRunning) || s == string(StatusReviewing) {
			anyRunning = true
		}
		if s == string(StatusReady) {
			anyReady = true
		}
		if s != string(StatusBlocked) {
			allBlocked = false
		}
	}

	switch {
	case allCompleted:
		return PlanStatusCompleted
	case anyFailed:
		return PlanStatusFailed
	case anyAwaiting:
		return PlanStatusAwaitingClarification
	case anyRunning:
		return PlanStatusRunning
	case anyReady:
		return PlanStatusReady
	case allBlocked:
		return PlanStatusBlocked
	default:
		return PlanStatusPending
	}
}

func storyTasksToStatuses(tasks []*TaskNode) []string {
	res := make([]string, len(tasks))
	for i, t := range tasks {
		res[i] = string(t.Status)
	}
	return res
}

func storyNodesToStatuses(stories []*StoryNode) []string {
	res := make([]string, len(stories))
	for i, s := range stories {
		res[i] = string(s.Status)
	}
	return res
}

func epicNodesToStatuses(epics []*EpicNode) []string {
	res := make([]string, len(epics))
	for i, e := range epics {
		res[i] = string(e.Status)
	}
	return res
}

func (o *Orchestrator) flattenTasks(plan *PlanNode) []*TaskNode {
	var list []*TaskNode
	for _, e := range plan.Epics {
		for _, s := range e.Stories {
			list = append(list, s.Tasks...)
		}
	}
	return list
}

func (o *Orchestrator) findTask(plan *PlanNode, taskID string) *TaskNode {
	for _, e := range plan.Epics {
		for _, s := range e.Stories {
			for _, t := range s.Tasks {
				if t.ID == taskID {
					return t
				}
			}
		}
	}
	return nil
}

func (o *Orchestrator) findTaskParents(plan *PlanNode, taskID string) (*EpicNode, *StoryNode) {
	for _, e := range plan.Epics {
		for _, s := range e.Stories {
			for _, t := range s.Tasks {
				if t.ID == taskID {
					return e, s
				}
			}
		}
	}
	return nil, nil
}

func (o *Orchestrator) taskToSpec(t *TaskNode) llm.TaskSpec {
	return llm.TaskSpec{
		ID:                 t.ID,
		Title:              t.Title,
		Description:        t.Description,
		TaskType:           t.TaskType,
		DependsOn:          append([]string(nil), t.DependsOn...),
		AcceptanceCriteria: append([]string(nil), t.AcceptanceCriteria...),
		Params:             t.Params,
	}
}

func (o *Orchestrator) planToSpec(p *PlanNode) llm.PlanSpec {
	spec := llm.PlanSpec{
		ID:        p.ID,
		Title:     p.Title,
		Objective: p.Objective,
	}
	for _, e := range p.Epics {
		epicSpec := llm.EpicSpec{
			ID:          e.ID,
			Title:       e.Title,
			Description: e.Description,
		}
		for _, s := range e.Stories {
			storySpec := llm.StorySpec{
				ID:          s.ID,
				Title:       s.Title,
				Description: s.Description,
			}
			for _, t := range s.Tasks {
				storySpec.Tasks = append(storySpec.Tasks, o.taskToSpec(t))
			}
			epicSpec.Stories = append(epicSpec.Stories, storySpec)
		}
		spec.Epics = append(spec.Epics, epicSpec)
	}
	return spec
}

func (o *Orchestrator) formatSystemStateSummary() string {
	if len(o.plans) == 0 && len(o.draftPlans) == 0 {
		return "No plans currently tracked in the engine."
	}
	var b strings.Builder
	for _, p := range o.plans {
		b.WriteString(fmt.Sprintf("Plan ID: %s | Title: %s | Status: %s\n", p.ID, p.Title, p.Status))
		for _, e := range p.Epics {
			b.WriteString(fmt.Sprintf("  Epic %s: %s [%s]\n", e.ID, e.Title, e.Status))
			for _, s := range e.Stories {
				b.WriteString(fmt.Sprintf("    Story %s: %s [%s]\n", s.ID, s.Title, s.Status))
				for _, t := range s.Tasks {
					b.WriteString(fmt.Sprintf("      Task %s: %s (%s) [%s] attempts=%d\n", t.ID, t.Title, t.TaskType, t.Status, t.Attempts))
					if t.Status == StatusAwaitingClarification {
						b.WriteString(fmt.Sprintf("        >>> AWAITING CLARIFICATION <<<\n        Explanation: %s\n        Question: %s\n",
							t.PendingExplanation, t.PendingQuestion))
					}
					if t.LastError != "" {
						b.WriteString(fmt.Sprintf("        Error: %s\n", t.LastError))
					}
				}
			}
		}
	}
	for id, d := range o.draftPlans {
		b.WriteString(fmt.Sprintf("Draft Plan ID: %s | Title: %s [Status: Draft]\n", id, d.Title))
	}
	return b.String()
}

func clonePlan(p *PlanNode) *PlanNode {
	if p == nil {
		return nil
	}
	data, _ := json.Marshal(p)
	var cloned PlanNode
	_ = json.Unmarshal(data, &cloned)
	return &cloned
}

