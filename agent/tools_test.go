package agent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"go-orchestrator/engine"
	"go-orchestrator/llm"
	"go-orchestrator/rag"
	"go-orchestrator/tasks"
)

func TestBuildTools_AndMCPParity(t *testing.T) {
	router := engine.NewWorkerRouter(llm.ModeMock, engine.WorkerNode{
		ID:          "npu-local",
		Name:        "AMD NPU",
		Type:        "npu",
		MaxContext:  8192,
		Concurrency: 1,
	})
	mockRAG := rag.NewMockRAGClient()
	reg := tasks.NewRegistry()
	reg.Register("code_gen", tasks.NewCodeGenHandler(router))

	oCtx := OrchestratorContext{
		EventCh:      make(chan any, 8),
		WorkerRouter: router,
		RAGClient:    mockRAG,
		Registry:     reg,
	}

	adkTools, err := BuildTools(oCtx)
	if err != nil {
		t.Fatalf("BuildTools failed: %v", err)
	}
	if len(adkTools) != 6 {
		t.Fatalf("expected 6 ADK tools (rag_search, ingest_file, draft_plan, approve_plan, get_plan_status, list_workers), got %d", len(adkTools))
	}

	mcp := NewMCPServer(oCtx)
	listResp := mcp.dispatch(context.Background(), jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/list",
	})
	if listResp == nil || listResp.Error != nil {
		t.Fatalf("MCP tools/list failed: %+v", listResp)
	}
	resMap, ok := listResp.Result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected tools/list result type: %T", listResp.Result)
	}
	mcpTools, _ := resMap["tools"].([]map[string]any)
	if len(mcpTools) != 6 {
		t.Fatalf("expected 6 MCP tools matching ADK parity, got %d", len(mcpTools))
	}

	// Verify MCP list_workers delegates to shared OrchestratorContext
	callParams, _ := json.Marshal(toolCallParams{
		Name:      "go-orchestrator__strix_orchestrator_list_workers",
		Arguments: json.RawMessage(`{}`),
	})
	callResp := mcp.dispatch(context.Background(), jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/call",
		Params:  callParams,
	})
	rawResp, _ := json.Marshal(callResp)
	if !strings.Contains(string(rawResp), "npu-local") {
		t.Errorf("expected MCP list_workers response to include npu-local, got: %s", string(rawResp))
	}

	// Verify MCP ingest_file + rag_search round-trip through shared OrchestratorContext
	ingestArgs, _ := json.Marshal(toolCallParams{
		Name:      "strix_orchestrator_ingest_file",
		Arguments: json.RawMessage(`{"content":"DuplicatedItemPorts on CLoadoutDummy::InitLoadout","collection":"test_col","source":"errors.log"}`),
	})
	ingestResp := mcp.dispatch(context.Background(), jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      3,
		Method:  "tools/call",
		Params:  ingestArgs,
	})
	rawIngest, _ := json.Marshal(ingestResp)
	if !strings.Contains(string(rawIngest), "Successfully indexed") {
		t.Errorf("expected successful ingest, got: %s", string(rawIngest))
	}
}
