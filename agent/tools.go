package agent

import (
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/tool"
	"google.golang.org/adk/v2/tool/functiontool"

	"go-orchestrator/engine"
	"go-orchestrator/llm"
	"go-orchestrator/rag"
)

// OrchestratorContext holds the core engine glue shared by ADK tools.
type OrchestratorContext struct {
	EventCh      chan any
	WorkerRouter *engine.WorkerRouter
	RAGClient    rag.Client
}

// ----------------------------------------------------------------------------
// 1. RAG Search Tool
// ----------------------------------------------------------------------------

type RAGSearchArgs struct {
	Query      string `json:"query" jsonschema:"The search query to match against indexed logs and code"`
	Collection string `json:"collection,omitempty" jsonschema:"Collection name (defaults to 'default_corpus')"`
	TopK       int    `json:"top_k,omitempty" jsonschema:"Number of top reranked matches (default: 5)"`
}

func NewRAGSearchTool(rc rag.Client) (tool.Tool, error) {
	return functiontool.New(functiontool.Config{
		Name:        "rag_search",
		Description: "Search indexed logs, documents, and code in the local vector database using dense embeddings + cross-encoder reranking.",
	}, func(ctx agent.Context, in RAGSearchArgs) (string, error) {
		if rc == nil {
			return "", fmt.Errorf("RAG client not configured")
		}
		coll := in.Collection
		if coll == "" {
			coll = "default_corpus"
		}
		topK := in.TopK
		if topK <= 0 {
			topK = 5
		}
		results, err := rc.SearchAndRerank(ctx, coll, in.Query, 25, topK)
		if err != nil {
			return "", fmt.Errorf("search failed: %w", err)
		}
		outBytes, _ := json.MarshalIndent(results, "", "  ")
		return string(outBytes), nil
	})
}

// ----------------------------------------------------------------------------
// 2. Draft Plan Tool
// ----------------------------------------------------------------------------

type DraftPlanArgs struct {
	Title     string         `json:"title" jsonschema:"Descriptive title of the plan"`
	Objective string         `json:"objective" jsonschema:"Detailed directive or objective describing what to build or analyze"`
	Epics     []llm.EpicSpec `json:"epics" jsonschema:"Hierarchical list of Epics, Stories, and Tasks forming an acyclic DAG"`
}

func NewDraftPlanTool(eventCh chan any) (tool.Tool, error) {
	return functiontool.New(functiontool.Config{
		Name:        "draft_plan",
		Description: "Submit and validate a proposed DAG plan without executing it. Verifies acyclicity and registers it as a draft awaiting human approval.",
	}, func(ctx agent.Context, in DraftPlanArgs) (string, error) {
		planSpec := llm.PlanSpec{
			Title:     in.Title,
			Objective: in.Objective,
			Epics:     in.Epics,
		}
		if _, err := engine.ValidatePlanSpec(planSpec, nil); err != nil {
			return "", fmt.Errorf("plan validation failed: %w", err)
		}

		replyCh := make(chan engine.CommandResult, 1)
		eventCh <- engine.StoreDraftPlanCommand{Plan: planSpec, ReplyCh: replyCh}
		cmdRes := <-replyCh
		if !cmdRes.Success {
			return "", fmt.Errorf("failed to save draft: %s", cmdRes.Message)
		}

		planID := planSpec.ID
		if strID, ok := cmdRes.Data.(string); ok && strID != "" {
			planID = strID
		}

		return fmt.Sprintf("DRAFT PLAN CREATED (ID: %s)\nStatus: Draft (Awaiting User Approval)\nObjective: %s\n"+
			"Instruct the user to review this plan. To execute, call approve_plan with plan_id: %q.",
			planID, planSpec.Objective, planID), nil
	})
}

// ----------------------------------------------------------------------------
// 3. Approve Plan Tool
// ----------------------------------------------------------------------------

type ApprovePlanArgs struct {
	PlanID string `json:"plan_id" jsonschema:"Deterministic ID of the draft plan to approve for execution"`
}

func NewApprovePlanTool(eventCh chan any) (tool.Tool, error) {
	return functiontool.New(functiontool.Config{
		Name:        "approve_plan",
		Description: "Approve a previously drafted plan to move it into active DAG execution.",
	}, func(ctx agent.Context, in ApprovePlanArgs) (string, error) {
		planID := strings.TrimSpace(in.PlanID)
		if planID == "" {
			return "", fmt.Errorf("plan_id is required")
		}

		replyCh := make(chan engine.CommandResult, 1)
		eventCh <- engine.SeedPlanEvent{PlanID: planID, RawJSON: "", ReplyCh: replyCh}
		cmdRes := <-replyCh
		if !cmdRes.Success {
			return "", fmt.Errorf("approval failed: %s", cmdRes.Message)
		}

		return fmt.Sprintf("Plan %s APPROVED and actively executing. Monitor via get_plan_status.", planID), nil
	})
}

// ----------------------------------------------------------------------------
// 4. Get Plan Status Tool
// ----------------------------------------------------------------------------

type GetPlanStatusArgs struct {
	PlanID string `json:"plan_id" jsonschema:"Deterministic ID of the plan to inspect"`
}

func NewGetPlanStatusTool(eventCh chan any) (tool.Tool, error) {
	return functiontool.New(functiontool.Config{
		Name:        "get_plan_status",
		Description: "Inspect live execution progress, NPU worker task scores, and retries for a plan.",
	}, func(ctx agent.Context, in GetPlanStatusArgs) (string, error) {
		planID := strings.TrimSpace(in.PlanID)
		if planID == "" {
			return "", fmt.Errorf("plan_id is required")
		}

		replyCh := make(chan engine.GetPlanResult, 1)
		eventCh <- engine.GetPlanQuery{PlanID: planID, ReplyCh: replyCh}
		res := <-replyCh
		if !res.Found {
			return "", fmt.Errorf("plan %q not found", planID)
		}

		outBytes, _ := json.MarshalIndent(res.Plan, "", "  ")
		return string(outBytes), nil
	})
}

// ----------------------------------------------------------------------------
// 5. List Workers Tool
// ----------------------------------------------------------------------------

type ListWorkersArgs struct{}

func NewListWorkersTool(router *engine.WorkerRouter) (tool.Tool, error) {
	return functiontool.New(functiontool.Config{
		Name:        "list_workers",
		Description: "Inspect registered worker nodes (local NPU and remote GPUs like 9070XT), queue depth, and status.",
	}, func(ctx agent.Context, in ListWorkersArgs) (string, error) {
		if router == nil {
			return "[]", nil
		}
		workers := router.ListWorkers()
		outBytes, _ := json.MarshalIndent(workers, "", "  ")
		return string(outBytes), nil
	})
}

// BuildTools compiles the complete ADK tool suite for the agent.
func BuildTools(oCtx OrchestratorContext) ([]tool.Tool, error) {
	var tools []tool.Tool

	if oCtx.RAGClient != nil {
		t, err := NewRAGSearchTool(oCtx.RAGClient)
		if err != nil {
			return nil, err
		}
		tools = append(tools, t)
	}

	tDraft, err := NewDraftPlanTool(oCtx.EventCh)
	if err != nil {
		return nil, err
	}
	tools = append(tools, tDraft)

	tApprove, err := NewApprovePlanTool(oCtx.EventCh)
	if err != nil {
		return nil, err
	}
	tools = append(tools, tApprove)

	tStatus, err := NewGetPlanStatusTool(oCtx.EventCh)
	if err != nil {
		return nil, err
	}
	tools = append(tools, tStatus)

	if oCtx.WorkerRouter != nil {
		tWorkers, err := NewListWorkersTool(oCtx.WorkerRouter)
		if err != nil {
			return nil, err
		}
		tools = append(tools, tWorkers)
	}

	return tools, nil
}

