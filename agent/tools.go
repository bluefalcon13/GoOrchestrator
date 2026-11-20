package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/tool"
	"google.golang.org/adk/v2/tool/functiontool"

	"go-orchestrator/engine"
	"go-orchestrator/llm"
	"go-orchestrator/rag"
	"go-orchestrator/tasks"
)

const (
	defaultChunkSize   = 1000
	defaultEventTimout = 10 * time.Second
)

// OrchestratorContext holds the core engine glue shared by ADK tools and MCP endpoints.
type OrchestratorContext struct {
	EventCh      chan any
	WorkerRouter *engine.WorkerRouter
	RAGClient    rag.Client
	Registry     *tasks.Registry
}

// ----------------------------------------------------------------------------
// Shared Tool Argument Schemas
// ----------------------------------------------------------------------------

type RAGSearchArgs struct {
	Query      string `json:"query" jsonschema:"The search query to match against indexed logs and code"`
	Collection string `json:"collection,omitempty" jsonschema:"Collection name (defaults to 'default_corpus')"`
	TopK       int    `json:"top_k,omitempty" jsonschema:"Number of top reranked matches (default: 5)"`
}

type IngestFileArgs struct {
	FilePath   string `json:"file_path,omitempty" jsonschema:"Local path to the file on disk (e.g. 'data/game.log' or '/data/game.log')"`
	Path       string `json:"path,omitempty" jsonschema:"Alias for file_path"`
	Content    string `json:"content,omitempty" jsonschema:"Raw string content (optional; if omitted or pointing to a file, file_path is used)"`
	Collection string `json:"collection,omitempty" jsonschema:"Qdrant collection name (default: 'default_corpus')"`
	Source     string `json:"source,omitempty" jsonschema:"Source identifier (default: file path or 'inline_upload')"`
	ChunkSize  int    `json:"chunk_size,omitempty" jsonschema:"Character chunk size (default: 1000)"`
}

type DraftPlanArgs struct {
	Title     string         `json:"title" jsonschema:"Descriptive title of the plan"`
	Objective string         `json:"objective" jsonschema:"Detailed directive or objective describing what to build or analyze"`
	Epics     []llm.EpicSpec `json:"epics" jsonschema:"Hierarchical list of Epics, Stories, and Tasks forming an acyclic DAG"`
}

type ApprovePlanArgs struct {
	PlanID string `json:"plan_id" jsonschema:"Deterministic ID of the draft plan to approve for execution"`
}

type GetPlanStatusArgs struct {
	PlanID string `json:"plan_id" jsonschema:"Deterministic ID of the plan to inspect"`
}

type ListWorkersArgs struct{}

// ----------------------------------------------------------------------------
// Shared Execution Methods (Single Source of Truth for ADK + MCP)
// ----------------------------------------------------------------------------

func (o OrchestratorContext) sendEvent(ctx context.Context, ev any) error {
	if o.EventCh == nil {
		return fmt.Errorf("orchestrator event bus is not configured")
	}
	timer := time.NewTimer(defaultEventTimout)
	defer timer.Stop()
	select {
	case o.EventCh <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("timed out sending command to orchestrator event loop")
	}
}

func waitReply[T any](ctx context.Context, ch <-chan T) (T, error) {
	var zero T
	timer := time.NewTimer(defaultEventTimout)
	defer timer.Stop()
	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-timer.C:
		return zero, fmt.Errorf("timed out waiting for orchestrator reply")
	}
}

// ExecuteRAGSearch performs dense vector retrieval + cross-encoder reranking.
func (o OrchestratorContext) ExecuteRAGSearch(ctx context.Context, in RAGSearchArgs) (string, error) {
	if o.RAGClient == nil {
		return "", fmt.Errorf("RAG client is not configured")
	}
	coll := strings.TrimSpace(in.Collection)
	if coll == "" {
		coll = "default_corpus"
	}
	topK := in.TopK
	if topK <= 0 {
		topK = 5
	}
	candidateLimit := max(topK*2, 25)

	searchCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	results, err := o.RAGClient.SearchAndRerank(searchCtx, coll, in.Query, candidateLimit, topK)
	if err != nil {
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "doesn't exist") {
			return fmt.Sprintf("Collection %q not found or empty. Ingest files first using ingest_file(collection=%q, ...).", coll, coll), nil
		}
		return "", fmt.Errorf("search failed: %w", err)
	}
	if len(results) == 0 {
		return fmt.Sprintf("No relevant matches found in collection %q for query %q.", coll, in.Query), nil
	}

	var sb strings.Builder
	for i, r := range results {
		fmt.Fprintf(&sb, "[%d] Score: %.3f | Source: %s\n%s\n\n", i+1, r.Score, r.Source, r.Text)
	}
	return sb.String(), nil
}

// ExecuteIngestFile streams a disk file or indexes inline text into Qdrant.
func (o OrchestratorContext) ExecuteIngestFile(ctx context.Context, in IngestFileArgs) (string, error) {
	if o.RAGClient == nil {
		return "", fmt.Errorf("RAG client is not configured")
	}
	coll := strings.TrimSpace(in.Collection)
	if coll == "" {
		coll = "default_corpus"
	}
	cs := in.ChunkSize
	if cs <= 0 {
		cs = defaultChunkSize
	}

	resolvedPath := strings.TrimSpace(in.FilePath)
	if resolvedPath == "" {
		resolvedPath = strings.TrimSpace(in.Path)
	}

	// Polymorphic check: if Content or Source looks like a path to an existing file, stream it.
	if resolvedPath == "" {
		for _, candidate := range []string{in.Content, in.Source} {
			cand := strings.TrimSpace(candidate)
			if len(cand) > 0 && len(cand) < 500 && strings.Contains(cand, ".") {
				if p, ok := resolveExistingPath(cand); ok {
					resolvedPath = p
					break
				}
			}
		}
	}

	ingestCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()

	if resolvedPath != "" {
		// Auto-repathing: if client passed a Windows/workspace path, check /data/<basename>.
		if _, err := os.Stat(resolvedPath); os.IsNotExist(err) {
			if repathed, ok := resolveExistingPath(resolvedPath); ok {
				log.Printf("[Ingest] Auto-repathing tool input %q -> %q", resolvedPath, repathed)
				resolvedPath = repathed
			}
		}
		numChunks, err := o.RAGClient.IndexFile(ingestCtx, coll, resolvedPath, cs)
		if err != nil {
			return "", fmt.Errorf("streaming file ingest error for %q: %w", resolvedPath, err)
		}
		return fmt.Sprintf("Successfully indexed %d chunks from file %s into collection %q (zero-RAM stream).", numChunks, resolvedPath, coll), nil
	}

	if len(in.Content) > 0 {
		src := strings.TrimSpace(in.Source)
		if src == "" {
			src = "inline_upload"
		}
		numChunks, err := o.RAGClient.IndexText(ingestCtx, coll, in.Content, src, cs)
		if err != nil {
			return "", fmt.Errorf("text ingest error: %w", err)
		}
		return fmt.Sprintf("Successfully indexed %d chunks from %s into collection %q.", numChunks, src, coll), nil
	}

	return "", fmt.Errorf("neither file_path nor content was provided for ingestion")
}

func resolveExistingPath(raw string) (string, bool) {
	if _, err := os.Stat(raw); err == nil {
		return raw, true
	}
	trimmed := strings.TrimPrefix(raw, "/")
	for _, prefix := range []string{"/data/", "data/", "./data/", "./"} {
		cand := prefix + trimmed
		if _, err := os.Stat(cand); err == nil {
			return cand, true
		}
	}
	cleanBase := filepath.Base(strings.ReplaceAll(raw, "\\", "/"))
	if cleanBase != "." && cleanBase != "/" && cleanBase != "" {
		for _, prefix := range []string{"/data", "data", "./data"} {
			cand := filepath.Join(prefix, cleanBase)
			if _, err := os.Stat(cand); err == nil {
				return cand, true
			}
		}
	}
	return "", false
}

// ExecuteDraftPlan validates and registers a draft DAG plan awaiting approval.
func (o OrchestratorContext) ExecuteDraftPlan(ctx context.Context, planSpec llm.PlanSpec) (string, error) {
	if _, err := engine.ValidatePlanSpec(planSpec, o.Registry); err != nil {
		return "", fmt.Errorf("plan validation failed: %w", err)
	}

	replyCh := make(chan engine.CommandResult, 1)
	if err := o.sendEvent(ctx, engine.StoreDraftPlanCommand{Plan: planSpec, ReplyCh: replyCh}); err != nil {
		return "", err
	}
	cmdRes, err := waitReply(ctx, replyCh)
	if err != nil {
		return "", err
	}
	if !cmdRes.Success {
		return "", fmt.Errorf("failed to save draft: %s", cmdRes.Message)
	}

	planID := planSpec.ID
	if strID, ok := cmdRes.Data.(string); ok && strID != "" {
		planID = strID
	} else if pSpec, ok := cmdRes.Data.(llm.PlanSpec); ok && pSpec.ID != "" {
		planID = pSpec.ID
	}

	return fmt.Sprintf("DRAFT PLAN CREATED (ID: %s)\nStatus: Draft (Awaiting User Approval)\nObjective: %s\n"+
		"Instruct the user to review this plan. To execute, call approve_plan with plan_id: %q.",
		planID, planSpec.Objective, planID), nil
}

// ExecuteApprovePlan moves a drafted plan into active DAG execution.
func (o OrchestratorContext) ExecuteApprovePlan(ctx context.Context, in ApprovePlanArgs) (string, error) {
	planID := strings.TrimSpace(in.PlanID)
	if planID == "" {
		return "", fmt.Errorf("plan_id is required")
	}

	replyCh := make(chan engine.CommandResult, 1)
	if err := o.sendEvent(ctx, engine.SeedPlanEvent{PlanID: planID, RawJSON: "", ReplyCh: replyCh}); err != nil {
		return "", err
	}
	cmdRes, err := waitReply(ctx, replyCh)
	if err != nil {
		return "", err
	}
	if !cmdRes.Success {
		return "", fmt.Errorf("approval failed: %s", cmdRes.Message)
	}

	return fmt.Sprintf("Plan %s APPROVED and actively executing. Monitor via get_plan_status.", planID), nil
}

// ExecuteGetPlanStatus returns a JSON snapshot of a plan's live execution state.
func (o OrchestratorContext) ExecuteGetPlanStatus(ctx context.Context, in GetPlanStatusArgs) (string, error) {
	planID := strings.TrimSpace(in.PlanID)
	if planID == "" {
		return "", fmt.Errorf("plan_id is required")
	}

	replyCh := make(chan engine.GetPlanResult, 1)
	if err := o.sendEvent(ctx, engine.GetPlanQuery{PlanID: planID, ReplyCh: replyCh}); err != nil {
		return "", err
	}
	res, err := waitReply(ctx, replyCh)
	if err != nil {
		return "", err
	}
	if !res.Found {
		return "", fmt.Errorf("plan %q not found", planID)
	}

	outBytes, _ := json.MarshalIndent(res.Plan, "", "  ")
	return string(outBytes), nil
}

// ExecuteListWorkers returns a JSON snapshot of registered worker nodes.
func (o OrchestratorContext) ExecuteListWorkers() (string, error) {
	if o.WorkerRouter == nil {
		return "[]", nil
	}
	workers := o.WorkerRouter.ListWorkers()
	outBytes, _ := json.MarshalIndent(workers, "", "  ")
	return string(outBytes), nil
}

// ----------------------------------------------------------------------------
// ADK functiontool Wrappers
// ----------------------------------------------------------------------------

func NewRAGSearchTool(oCtx OrchestratorContext) (tool.Tool, error) {
	return functiontool.New(functiontool.Config{
		Name:        "rag_search",
		Description: "Search indexed logs, documents, and code in the local vector database using dense embeddings + cross-encoder reranking.",
	}, func(ctx agent.Context, in RAGSearchArgs) (string, error) {
		return oCtx.ExecuteRAGSearch(ctx, in)
	})
}

func NewIngestFileTool(oCtx OrchestratorContext) (tool.Tool, error) {
	return functiontool.New(functiontool.Config{
		Name:        "ingest_file",
		Description: "Polymorphic RAG indexer: stream-index a file from disk by file_path (e.g. '/data/game.log') or raw text by content into local Qdrant.",
	}, func(ctx agent.Context, in IngestFileArgs) (string, error) {
		return oCtx.ExecuteIngestFile(ctx, in)
	})
}

func NewDraftPlanTool(oCtx OrchestratorContext) (tool.Tool, error) {
	return functiontool.New(functiontool.Config{
		Name:        "draft_plan",
		Description: "Submit and validate a proposed DAG plan without executing it. Verifies acyclicity and registers it as a draft awaiting human approval.",
	}, func(ctx agent.Context, in DraftPlanArgs) (string, error) {
		return oCtx.ExecuteDraftPlan(ctx, llm.PlanSpec{
			Title:     in.Title,
			Objective: in.Objective,
			Epics:     in.Epics,
		})
	})
}

func NewApprovePlanTool(oCtx OrchestratorContext) (tool.Tool, error) {
	return functiontool.New(functiontool.Config{
		Name:        "approve_plan",
		Description: "Approve a previously drafted plan to move it into active DAG execution.",
	}, func(ctx agent.Context, in ApprovePlanArgs) (string, error) {
		return oCtx.ExecuteApprovePlan(ctx, in)
	})
}

func NewGetPlanStatusTool(oCtx OrchestratorContext) (tool.Tool, error) {
	return functiontool.New(functiontool.Config{
		Name:        "get_plan_status",
		Description: "Inspect live execution progress, NPU worker task scores, and retries for a plan.",
	}, func(ctx agent.Context, in GetPlanStatusArgs) (string, error) {
		return oCtx.ExecuteGetPlanStatus(ctx, in)
	})
}

func NewListWorkersTool(oCtx OrchestratorContext) (tool.Tool, error) {
	return functiontool.New(functiontool.Config{
		Name:        "list_workers",
		Description: "Inspect registered worker nodes (local NPU and remote GPUs like 9070XT), queue depth, and status.",
	}, func(ctx agent.Context, in ListWorkersArgs) (string, error) {
		return oCtx.ExecuteListWorkers()
	})
}

// BuildTools compiles the complete ADK tool suite for the agent.
func BuildTools(oCtx OrchestratorContext) ([]tool.Tool, error) {
	var tools []tool.Tool

	if oCtx.RAGClient != nil {
		tSearch, err := NewRAGSearchTool(oCtx)
		if err != nil {
			return nil, err
		}
		tIngest, err := NewIngestFileTool(oCtx)
		if err != nil {
			return nil, err
		}
		tools = append(tools, tSearch, tIngest)
	}

	tDraft, err := NewDraftPlanTool(oCtx)
	if err != nil {
		return nil, err
	}
	tApprove, err := NewApprovePlanTool(oCtx)
	if err != nil {
		return nil, err
	}
	tStatus, err := NewGetPlanStatusTool(oCtx)
	if err != nil {
		return nil, err
	}
	tools = append(tools, tDraft, tApprove, tStatus)

	if oCtx.WorkerRouter != nil {
		tWorkers, err := NewListWorkersTool(oCtx)
		if err != nil {
			return nil, err
		}
		tools = append(tools, tWorkers)
	}

	return tools, nil
}
