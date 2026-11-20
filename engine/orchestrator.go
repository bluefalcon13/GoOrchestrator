package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/agent/workflowagent"
	"google.golang.org/adk/v2/runner"
	"google.golang.org/adk/v2/session"
	"google.golang.org/adk/v2/workflow"
	"google.golang.org/genai"

	"go-orchestrator/llm"
	"go-orchestrator/tasks"
)

// Orchestrator coordinates plan lifecycle, DAG execution via google.golang.org/adk/v2/workflow,
// worker dispatch, and multi-tier LRM review/escalation.
type Orchestrator struct {
	mu           sync.RWMutex
	lrm          *llm.LRMPersonaEngine
	workerClient llm.LLMClient
	registry     *tasks.Registry
	eventCh      chan any
	plans        map[string]*PlanNode
	draftPlans   map[string]llm.PlanSpec
	planCancels  map[string]context.CancelFunc
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
	drafts := make(map[string]llm.PlanSpec)
	if data, err := os.ReadFile("/data/draft_plans.json"); err == nil {
		_ = json.Unmarshal(data, &drafts)
	}

	return &Orchestrator{
		lrm:          lrm,
		workerClient: workerClient,
		registry:     registry,
		eventCh:      eventCh,
		plans:        make(map[string]*PlanNode),
		draftPlans:   drafts,
		planCancels:  make(map[string]context.CancelFunc),
		maxRetries:   3,
		log:          slog.Default(),
	}
}

// RunEventLoop processes external control/query commands while ADK workflow
// runners execute active plan DAGs asynchronously.
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

			case ClarificationResolvedEvent:
				o.handleClarificationResolved(workerCtx, ev)

			case GetPlanQuery:
				o.mu.RLock()
				plan, exists := o.plans[ev.PlanID]
				if !exists {
					if draft, ok := o.draftPlans[ev.PlanID]; ok {
						draftNode := o.createPlanNode(draft)
						draftNode.Status = PlanStatusDraft
						plan = draftNode
						exists = true
					}
				}
				cloned := clonePlan(plan)
				o.mu.RUnlock()
				ev.ReplyCh <- GetPlanResult{Plan: cloned, Found: exists}

			case ListPlansQuery:
				o.mu.RLock()
				list := make([]*PlanNode, 0, len(o.plans)+len(o.draftPlans))
				for _, p := range o.plans {
					list = append(list, clonePlan(p))
				}
				for _, d := range o.draftPlans {
					draftNode := o.createPlanNode(d)
					draftNode.Status = PlanStatusDraft
					list = append(list, draftNode)
				}
				o.mu.RUnlock()
				sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt.Before(list[j].CreatedAt) })
				ev.ReplyCh <- list

			case GetSystemStateQuery:
				o.mu.RLock()
				summary := o.formatSystemStateSummaryLocked()
				o.mu.RUnlock()
				ev.ReplyCh <- summary

			case GetDraftPlansQuery:
				o.mu.RLock()
				drafts := make([]llm.PlanSpec, 0, len(o.draftPlans))
				for _, d := range o.draftPlans {
					drafts = append(drafts, d)
				}
				o.mu.RUnlock()
				ev.ReplyCh <- drafts

			case StoreDraftPlanCommand:
				o.handleStoreDraftPlan(ev)

			case PausePlanCommand:
				o.handlePausePlan(ev)

			case ResumePlanCommand:
				o.handleResumePlan(workerCtx, ev)

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

	cancelWorkers()

	if srvShutdown != nil {
		shutdownCtx, srvCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer srvCancel()
		if err := srvShutdown(shutdownCtx); err != nil {
			o.log.Error("error during HTTP server shutdown", "err", err)
		}
	}

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
// Plan Ingestion, Draft Persistence, Pause & Resume
// ---------------------------------------------------------------------------

func (o *Orchestrator) persistDraftPlansLocked() {
	if data, err := json.Marshal(o.draftPlans); err == nil {
		_ = os.WriteFile("/data/draft_plans.json", data, 0644)
	}
}

func (o *Orchestrator) handleSeedPlan(workerCtx context.Context, ev SeedPlanEvent) {
	var spec llm.PlanSpec
	var err error

	o.mu.Lock()
	if strings.TrimSpace(ev.RawJSON) != "" {
		o.mu.Unlock()
		spec, err = llm.ParsePlanSpec(ev.RawJSON)
		if err != nil {
			o.log.Error("failed to parse plan JSON", "err", err)
			if ev.ReplyCh != nil {
				ev.ReplyCh <- CommandResult{Success: false, Message: "failed to parse plan JSON", Err: err}
			}
			return
		}
		o.mu.Lock()
	} else if ev.PlanID != "" {
		draft, ok := o.draftPlans[ev.PlanID]
		if !ok {
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
			o.mu.Unlock()
			o.log.Error("draft plan not found in registry", "plan_id", ev.PlanID)
			if ev.ReplyCh != nil {
				ev.ReplyCh <- CommandResult{Success: false, Message: fmt.Sprintf("draft plan %q not found", ev.PlanID), Err: fmt.Errorf("draft plan not found")}
			}
			return
		}
		spec = draft
	} else if len(o.draftPlans) == 1 {
		for dID, d := range o.draftPlans {
			spec = d
			ev.PlanID = dID
			break
		}
	} else {
		o.mu.Unlock()
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

	if _, err := ValidatePlanSpec(spec, o.registry); err != nil {
		o.mu.Unlock()
		o.log.Error("plan failed structural DAG validation", "plan_id", spec.ID, "err", err)
		if ev.ReplyCh != nil {
			ev.ReplyCh <- CommandResult{Success: false, Message: "structural DAG validation failed", Err: err}
		}
		return
	}

	planNode := o.createPlanNode(spec)
	o.plans[planNode.ID] = planNode
	if _, existed := o.draftPlans[planNode.ID]; existed {
		delete(o.draftPlans, planNode.ID)
		o.persistDraftPlansLocked()
	}
	o.mu.Unlock()

	o.log.Info("plan accepted into active execution", "plan_id", planNode.ID, "title", planNode.Title)
	if ev.ReplyCh != nil {
		ev.ReplyCh <- CommandResult{Success: true, Message: "plan accepted into execution", Data: planNode.ID}
	}

	o.launchPlanWorkflow(workerCtx, planNode.ID)
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
	suffix := hex.EncodeToString(hash[:2])
	return fmt.Sprintf("%s-%s", res, suffix)
}

func (o *Orchestrator) handleStoreDraftPlan(ev StoreDraftPlanCommand) {
	o.mu.Lock()
	spec := ev.Plan
	if spec.ID == "" {
		spec.ID = GeneratePlanSlug(spec.Title)
	}
	o.draftPlans[spec.ID] = spec
	o.persistDraftPlansLocked()
	o.mu.Unlock()
	ev.ReplyCh <- CommandResult{Success: true, Message: "draft plan saved", Data: spec}
}

func (o *Orchestrator) handlePausePlan(ev PausePlanCommand) {
	o.mu.Lock()
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
		o.mu.Unlock()
		if ev.ReplyCh != nil {
			ev.ReplyCh <- CommandResult{Success: false, Message: "no active plan found to pause", Err: fmt.Errorf("plan not found")}
		}
		return
	}

	target.Status = PlanStatusPaused
	target.UpdatedAt = time.Now()
	if cancel, ok := o.planCancels[target.ID]; ok {
		cancel()
		delete(o.planCancels, target.ID)
	}
	planID := target.ID
	o.mu.Unlock()

	o.log.Info("plan execution paused", "plan_id", planID)
	if ev.ReplyCh != nil {
		ev.ReplyCh <- CommandResult{Success: true, Message: fmt.Sprintf("plan %s paused", planID), Data: planID}
	}
}

func (o *Orchestrator) handleResumePlan(workerCtx context.Context, ev ResumePlanCommand) {
	o.mu.Lock()
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
		o.mu.Unlock()
		if ev.ReplyCh != nil {
			ev.ReplyCh <- CommandResult{Success: false, Message: "no paused plan found to resume", Err: fmt.Errorf("plan not found")}
		}
		return
	}

	target.Status = PlanStatusRunning
	target.UpdatedAt = time.Now()
	planID := target.ID
	o.mu.Unlock()

	o.log.Info("plan execution resumed", "plan_id", planID)
	if ev.ReplyCh != nil {
		ev.ReplyCh <- CommandResult{Success: true, Message: fmt.Sprintf("plan %s resumed", planID), Data: planID}
	}
	o.launchPlanWorkflow(workerCtx, planID)
}

func (o *Orchestrator) handleClarificationResolved(workerCtx context.Context, ev ClarificationResolvedEvent) {
	o.mu.Lock()
	plan, ok := o.plans[ev.PlanID]
	if !ok {
		o.mu.Unlock()
		if ev.ReplyCh != nil {
			ev.ReplyCh <- CommandResult{Success: false, Message: "plan not found"}
		}
		return
	}
	task := o.findTaskLocked(plan, ev.TaskID)
	if task == nil {
		o.mu.Unlock()
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

	downstreamMap, _ := BuildAdjacencyLists(plan)
	dependents := FindTransitiveDependents(task.ID, downstreamMap)
	for _, depID := range dependents {
		if dep := o.findTaskLocked(plan, depID); dep != nil && dep.Status == StatusBlocked {
			dep.Status = StatusPending
			dep.UpdatedAt = time.Now()
		}
	}
	o.rollupPlanLocked(plan)
	taskCopy := *task
	o.mu.Unlock()

	if ev.ReplyCh != nil {
		ev.ReplyCh <- CommandResult{
			Success: true,
			Message: ev.Interpretation.Acknowledgement,
			Data:    &taskCopy,
		}
	}

	o.launchPlanWorkflow(workerCtx, ev.PlanID)
}

// ---------------------------------------------------------------------------
// ADK Workflow DAG Compilation & Execution
// ---------------------------------------------------------------------------

var errTaskHalted = errors.New("task halted pending clarification or terminal failure")

func (o *Orchestrator) launchPlanWorkflow(workerCtx context.Context, planID string) {
	o.mu.Lock()
	plan, ok := o.plans[planID]
	if !ok || plan.Status == PlanStatusPaused {
		o.mu.Unlock()
		return
	}
	if prevCancel, exists := o.planCancels[planID]; exists {
		prevCancel()
	}
	planCtx, cancel := context.WithCancel(workerCtx)
	o.planCancels[planID] = cancel

	wfAgent, err := o.buildPlanWorkflowAgentLocked(plan)
	o.mu.Unlock()

	if err != nil {
		o.log.Error("failed to compile ADK workflow for plan", "plan_id", planID, "err", err)
		return
	}
	if wfAgent == nil {
		return // All tasks already completed
	}

	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		defer func() {
			o.mu.Lock()
			delete(o.planCancels, planID)
			if p, exists := o.plans[planID]; exists {
				o.rollupPlanLocked(p)
			}
			o.mu.Unlock()
			cancel()
		}()

		sessSvc := session.InMemoryService()
		r, err := runner.New(runner.Config{
			AppName:        "strix_workflow",
			Agent:          wfAgent,
			SessionService: sessSvc,
		})
		if err != nil {
			o.log.Error("failed to create ADK workflow runner", "plan_id", planID, "err", err)
			return
		}

		sessResp, err := sessSvc.Create(planCtx, &session.CreateRequest{
			AppName: "strix_workflow",
			UserID:  "orchestrator",
		})
		if err != nil {
			o.log.Error("failed to create ADK workflow session", "plan_id", planID, "err", err)
			return
		}

		trigger := genai.NewContentFromText("execute plan "+planID, genai.RoleUser)
		for _, runErr := range r.Run(planCtx, "orchestrator", sessResp.Session.ID(), trigger, agent.RunConfig{}) {
			if runErr != nil && !errors.Is(runErr, errTaskHalted) && !errors.Is(runErr, context.Canceled) {
				o.log.Warn("ADK workflow execution yielded error", "plan_id", planID, "err", runErr)
			}
		}
	}()
}

// buildPlanWorkflowAgentLocked compiles all non-completed tasks of a PlanNode
// into a google.golang.org/adk/v2/workflow graph wrapped as a workflowagent.
func (o *Orchestrator) buildPlanWorkflowAgentLocked(plan *PlanNode) (agent.Agent, error) {
	allTasks := o.flattenTasksLocked(plan)
	nodeMap := make(map[string]workflow.Node)

	for _, t := range allTasks {
		if t.Status == StatusCompleted || t.Status == StatusBlocked || t.Status == StatusAwaitingClarification || t.Status == StatusFailed {
			continue
		}
		taskID := t.ID
		planID := plan.ID
		nodeMap[taskID] = workflow.NewDynamicNode[any, string](
			taskID,
			func(ctx agent.Context, _ any, _ func(*session.Event) error) (string, error) {
				return o.executeTaskNode(ctx, planID, taskID)
			},
			workflow.NodeConfig{},
		)
	}

	if len(nodeMap) == 0 {
		return nil, nil
	}

	eb := workflow.NewEdgeBuilder()
	for _, t := range allTasks {
		node, included := nodeMap[t.ID]
		if !included {
			continue
		}
		wiredUpstream := false
		for _, depID := range t.DependsOn {
			if depNode, depInGraph := nodeMap[depID]; depInGraph {
				eb.Add(depNode, node)
				wiredUpstream = true
			}
		}
		if !wiredUpstream {
			eb.Add(workflow.Start, node)
		}
	}

	return workflowagent.New(workflowagent.Config{
		Name:        sanitizeWorkflowName(plan.ID),
		Description: plan.Title,
		Edges:       eb.Build(),
	})
}

func sanitizeWorkflowName(id string) string {
	var sb strings.Builder
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('_')
		}
	}
	res := sb.String()
	if res == "" || (res[0] >= '0' && res[0] <= '9') {
		return "wf_" + res
	}
	return res
}

// executeTaskNode runs a single leaf task inside an ADK workflow DynamicNode:
// Execute -> LRM Review -> Tactical Retry -> Project Manager Triage (Split via workflow.RunNode / Clarify / Fail).
func (o *Orchestrator) executeTaskNode(ctx agent.Context, planID, taskID string) (string, error) {
	o.mu.Lock()
	plan, ok := o.plans[planID]
	if !ok || plan.Status == PlanStatusPaused {
		o.mu.Unlock()
		return "", errTaskHalted
	}
	task := o.findTaskLocked(plan, taskID)
	if task == nil {
		o.mu.Unlock()
		return "", fmt.Errorf("task %s not found", taskID)
	}
	if task.Status == StatusCompleted {
		out := task.Output
		o.mu.Unlock()
		return out, nil
	}
	if task.Status == StatusBlocked || task.Status == StatusAwaitingClarification || task.Status == StatusFailed {
		o.mu.Unlock()
		return "", errTaskHalted
	}

	handler, hasHandler := o.registry.Get(task.TaskType)
	if !hasHandler {
		task.Status = StatusFailed
		task.LastError = fmt.Sprintf("missing handler for %s", task.TaskType)
		o.blockTransitiveDependentsLocked(plan, task.ID)
		o.rollupPlanLocked(plan)
		o.mu.Unlock()
		return "", errTaskHalted
	}
	maxAttempts := task.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = o.maxRetries
	}
	o.mu.Unlock()

	var escalateReason string

	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}

		// 1. Prepare TaskInput and mark Running
		o.mu.Lock()
		if plan.Status == PlanStatusPaused {
			o.mu.Unlock()
			return "", errTaskHalted
		}
		task.Status = StatusRunning
		task.Attempts++
		attempt := task.Attempts
		task.UpdatedAt = time.Now()
		o.rollupPlanLocked(plan)

		upstreamOutputs := make(map[string]string)
		for _, depID := range task.DependsOn {
			if dep := o.findTaskLocked(plan, depID); dep != nil {
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
			Attempt:         attempt,
		}
		o.mu.Unlock()

		o.log.Info("dispatching task to worker", "plan_id", planID, "task_id", taskID, "attempt", attempt)
		output, execErr := handler.Execute(ctx, input)
		if ctx.Err() != nil {
			return "", ctx.Err()
		}

		if execErr != nil {
			o.log.Warn("task execution yielded error", "plan_id", planID, "task_id", taskID, "err", execErr)
			o.mu.Lock()
			task.LastError = execErr.Error()
			task.UpdatedAt = time.Now()
			o.mu.Unlock()

			if attempt < maxAttempts {
				continue
			}
			escalateReason = fmt.Sprintf("Worker execution error: %v", execErr)
			break
		}

		// 2. Store output and invoke Reviewer Hat
		o.mu.Lock()
		task.Output = output.Content
		task.Artifacts = output.Artifacts
		task.Status = StatusReviewing
		task.UpdatedAt = time.Now()
		o.rollupPlanLocked(plan)

		epic, story := o.findTaskParentsLocked(plan, task.ID)
		reviewIn := llm.ReviewInput{
			PlanObjective:  plan.Objective,
			EpicTitle:      epic.Title,
			StoryTitle:     story.Title,
			Task:           o.taskToSpec(task),
			Attempt:        attempt,
			MaxAttempts:    maxAttempts,
			Output:         task.Output,
			PriorFeedback:  append([]string(nil), task.FeedbackHistory...),
			Clarifications: append([]string(nil), task.Clarifications...),
		}
		o.mu.Unlock()

		verdict, revErr := o.lrm.Review(ctx, reviewIn)
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if revErr != nil {
			o.log.Error("reviewer hat encountered error", "plan_id", planID, "task_id", taskID, "err", revErr)
			o.mu.Lock()
			task.LastError = "Review error: " + revErr.Error()
			o.mu.Unlock()
			if attempt < maxAttempts {
				continue
			}
			escalateReason = fmt.Sprintf("Reviewer error: %v", revErr)
			break
		}

		o.mu.Lock()
		task.LastScore = verdict.Score
		if verdict.Approved {
			task.Status = StatusCompleted
			task.UpdatedAt = time.Now()
			o.rollupPlanLocked(plan)
			finalOut := task.Output
			o.mu.Unlock()
			o.log.Info("task passed review", "plan_id", planID, "task_id", taskID, "score", verdict.Score)
			return finalOut, nil
		}

		if verdict.Feedback != "" {
			task.FeedbackHistory = append(task.FeedbackHistory, verdict.Feedback)
		}
		task.UpdatedAt = time.Now()
		o.mu.Unlock()

		o.log.Warn("task rejected by reviewer", "plan_id", planID, "task_id", taskID,
			"needs_clarification", verdict.NeedsClarification, "feedback", verdict.Feedback)

		if verdict.NeedsClarification {
			escalateReason = "needs_clarification"
			break
		}
		if attempt < maxAttempts {
			o.log.Info("scheduling tactical retry", "plan_id", planID, "task_id", taskID, "next_attempt", attempt+1)
			continue
		}
		o.log.Warn("max retries exhausted, escalating to Project Manager", "plan_id", planID, "task_id", taskID)
		escalateReason = fmt.Sprintf("Exhausted %d attempts. Last feedback: %s", attempt, verdict.Feedback)
		break
	}

	// 3. Strategic Escalation: Project Manager Triage (directly in-node, no channel self-send)
	return o.triageAndRemediate(ctx, plan, task, escalateReason)
}

func (o *Orchestrator) triageAndRemediate(ctx agent.Context, plan *PlanNode, task *TaskNode, reason string) (string, error) {
	o.mu.RLock()
	epic, story := o.findTaskParentsLocked(plan, task.ID)
	allTasks := o.flattenTasksLocked(plan)
	existingIDs := make([]string, 0, len(allTasks))
	for _, t := range allTasks {
		existingIDs = append(existingIDs, t.ID)
	}
	caps := o.registry.Capabilities()
	capList := make([]llm.Capability, 0, len(caps))
	for k, desc := range caps {
		capList = append(capList, llm.Capability{TaskType: k, Description: desc})
	}
	triageIn := llm.TriageInput{
		PlanObjective:   plan.Objective,
		EpicTitle:       epic.Title,
		StoryTitle:      story.Title,
		Task:            o.taskToSpec(task),
		Reason:          reason,
		Attempts:        task.Attempts,
		MaxAttempts:     task.MaxAttempts,
		Feedback:        append([]string(nil), task.FeedbackHistory...),
		LastOutput:      task.Output,
		LastError:       task.LastError,
		CanSplit:        !strings.Contains(task.ID, ".s"),
		Capabilities:    capList,
		ExistingTaskIDs: existingIDs,
		Clarifications:  append([]string(nil), task.Clarifications...),
	}
	o.mu.RUnlock()

	remediation, err := o.lrm.Triage(ctx, triageIn)
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	if err != nil {
		o.log.Error("project manager triage error, pausing for clarification", "err", err)
		o.mu.Lock()
		task.Status = StatusAwaitingClarification
		task.PendingQuestion = "The task repeatedly failed and automated triage failed. Please provide guidance."
		o.blockTransitiveDependentsLocked(plan, task.ID)
		o.rollupPlanLocked(plan)
		o.mu.Unlock()
		return "", errTaskHalted
	}

	o.log.Info("project manager returned remediation", "action", remediation.Action, "rationale", remediation.Rationale)

	switch remediation.Action {
	case llm.ActionSplit:
		subNodes, err := o.spliceSubTasks(plan, task, remediation.SubTasks)
		if err != nil {
			o.log.Warn("sub-task splicing failed, falling back to clarification", "err", err)
			o.mu.Lock()
			task.Status = StatusAwaitingClarification
			task.PendingExplanation = "Failed to decompose task: " + err.Error()
			task.PendingQuestion = "Please provide clarifying instructions for this task."
			o.blockTransitiveDependentsLocked(plan, task.ID)
			o.rollupPlanLocked(plan)
			o.mu.Unlock()
			return "", errTaskHalted
		}

		// Execute spliced sub-tasks in topological order via ADK workflow.RunNode
		var combinedOut strings.Builder
		for _, sub := range subNodes {
			subID := sub.ID
			childNode := workflow.NewDynamicNode[any, string](
				subID,
				func(childCtx agent.Context, _ any, _ func(*session.Event) error) (string, error) {
					return o.executeTaskNode(childCtx, plan.ID, subID)
				},
				workflow.NodeConfig{},
			)
			subOut, runErr := workflow.RunNode[string](ctx, childNode, nil)
			if runErr != nil {
				return "", runErr
			}
			if combinedOut.Len() > 0 {
				combinedOut.WriteString("\n\n")
			}
			combinedOut.WriteString(subOut)
		}

		o.mu.Lock()
		task.Output = combinedOut.String()
		task.Status = StatusCompleted
		task.UpdatedAt = time.Now()
		o.rollupPlanLocked(plan)
		o.mu.Unlock()
		o.log.Info("join node completed automatically", "plan_id", plan.ID, "task_id", task.ID)
		return combinedOut.String(), nil

	case llm.ActionClarify:
		o.mu.Lock()
		task.Status = StatusAwaitingClarification
		task.PendingExplanation = remediation.Explanation
		task.PendingQuestion = remediation.Question
		task.UpdatedAt = time.Now()
		o.blockTransitiveDependentsLocked(plan, task.ID)
		o.rollupPlanLocked(plan)
		o.mu.Unlock()
		return "", errTaskHalted

	default: // ActionFail
		o.mu.Lock()
		task.Status = StatusFailed
		task.LastError = remediation.Explanation
		task.UpdatedAt = time.Now()
		o.blockTransitiveDependentsLocked(plan, task.ID)
		o.rollupPlanLocked(plan)
		o.mu.Unlock()
		return "", errTaskHalted
	}
}

// ---------------------------------------------------------------------------
// Dynamic Sub-Task Splicing (Atomic Validation Before Mutation)
// ---------------------------------------------------------------------------

func (o *Orchestrator) spliceSubTasks(plan *PlanNode, task *TaskNode, subTasks []llm.TaskSpec) ([]*TaskNode, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	_, story := o.findTaskParentsLocked(plan, task.ID)
	if story == nil {
		return nil, fmt.Errorf("parent story for task %s not found", task.ID)
	}

	idPrefix := task.ID + "."
	localMap := make(map[string]string, len(subTasks))
	for _, sub := range subTasks {
		localMap[sub.ID] = idPrefix + sub.ID
	}

	now := time.Now()
	newNodes := make([]*TaskNode, 0, len(subTasks))
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
			mappedDeps = append([]string(nil), task.DependsOn...)
		}

		newNodes = append(newNodes, &TaskNode{
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
		})
	}

	terminalSubIDs := make([]string, 0, len(newNodes))
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

	// Snapshot original task state for full atomic rollback on validation failure
	origDeps := append([]string(nil), task.DependsOn...)
	origStatus := task.Status
	origLen := len(story.Tasks)

	story.Tasks = append(story.Tasks, newNodes...)
	task.IsJoinNode = true
	task.SubTaskIDs = make([]string, 0, len(newNodes))
	for _, n := range newNodes {
		task.SubTaskIDs = append(task.SubTaskIDs, n.ID)
	}
	task.DependsOn = terminalSubIDs
	task.Status = StatusPending
	task.UpdatedAt = now

	sortedIDs, err := ValidatePlanSpec(o.planToSpecLocked(plan), o.registry)
	if err != nil {
		// Full rollback: restore story tasks, DependsOn, Status, IsJoinNode, SubTaskIDs
		story.Tasks = story.Tasks[:origLen]
		task.IsJoinNode = false
		task.SubTaskIDs = nil
		task.DependsOn = origDeps
		task.Status = origStatus
		return nil, fmt.Errorf("spliced plan DAG invalid: %w", err)
	}

	// Return spliced sub-nodes in validated topological order
	nodeByID := make(map[string]*TaskNode, len(newNodes))
	for _, n := range newNodes {
		nodeByID[n.ID] = n
	}
	ordered := make([]*TaskNode, 0, len(newNodes))
	for _, id := range sortedIDs {
		if n, ok := nodeByID[id]; ok {
			ordered = append(ordered, n)
		}
	}

	o.log.Info("successfully spliced sub-tasks into DAG", "original_task", task.ID, "sub_tasks_count", len(ordered))
	return ordered, nil
}

// ---------------------------------------------------------------------------
// Tree Construction, Rollups & Helpers (All *Locked require o.mu held)
// ---------------------------------------------------------------------------

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

func (o *Orchestrator) blockTransitiveDependentsLocked(plan *PlanNode, rootTaskID string) {
	downstreamMap, _ := BuildAdjacencyLists(plan)
	dependents := FindTransitiveDependents(rootTaskID, downstreamMap)
	now := time.Now()
	for _, depID := range dependents {
		if dep := o.findTaskLocked(plan, depID); dep != nil {
			dep.Status = StatusBlocked
			dep.UpdatedAt = now
		}
	}
}

func (o *Orchestrator) rollupPlanLocked(plan *PlanNode) {
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

func (o *Orchestrator) flattenTasksLocked(plan *PlanNode) []*TaskNode {
	var list []*TaskNode
	for _, e := range plan.Epics {
		for _, s := range e.Stories {
			list = append(list, s.Tasks...)
		}
	}
	return list
}

func (o *Orchestrator) findTaskLocked(plan *PlanNode, taskID string) *TaskNode {
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

func (o *Orchestrator) findTaskParentsLocked(plan *PlanNode, taskID string) (*EpicNode, *StoryNode) {
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

func (o *Orchestrator) planToSpecLocked(p *PlanNode) llm.PlanSpec {
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

func (o *Orchestrator) formatSystemStateSummaryLocked() string {
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
