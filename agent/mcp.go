package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"go-orchestrator/engine"
	"go-orchestrator/llm"
)

// MCPServer exposes the orchestrator's tools over the standard MCP SSE protocol for VS Code Continue.
type MCPServer struct {
	oCtx     OrchestratorContext
	mu       sync.RWMutex
	sessions map[string]chan string
}

// NewMCPServer creates a new MCP SSE server.
func NewMCPServer(oCtx OrchestratorContext) *MCPServer {
	return &MCPServer{
		oCtx:     oCtx,
		sessions: make(map[string]chan string),
	}
}

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id,omitempty"`
	Result  any    `json:"result,omitempty"`
	Error   any    `json:"error,omitempty"`
}

func (s *MCPServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/mcp/sse", s.handleSSE)
	mux.HandleFunc("/mcp/messages", s.handleMessages)
}

func (s *MCPServer) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE streaming unsupported", http.StatusInternalServerError)
		return
	}

	b := make([]byte, 16)
	_, _ = rand.Read(b)
	sessionID := hex.EncodeToString(b)

	ch := make(chan string, 64)
	s.mu.Lock()
	s.sessions[sessionID] = ch
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.sessions, sessionID)
		s.mu.Unlock()
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// 1. Initial endpoint event required by MCP SSE spec
	endpointURI := fmt.Sprintf("/mcp/messages?sessionId=%s", sessionID)
	fmt.Fprintf(w, "event: endpoint\ndata: %s\n\n", endpointURI)
	flusher.Flush()
	log.Printf("[MCP] Session started: %s -> %s", sessionID, endpointURI)

	// 2. Loop streaming outgoing JSON-RPC responses
	for {
		select {
		case <-r.Context().Done():
			log.Printf("[MCP] Session closed: %s", sessionID)
			return
		case msg, open := <-ch:
			if !open {
				return
			}
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

func (s *MCPServer) handleMessages(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	sessionID := r.URL.Query().Get("sessionId")
	s.mu.RLock()
	ch, exists := s.sessions[sessionID]
	s.mu.RUnlock()

	if !exists {
		http.Error(w, fmt.Sprintf("session %q not found", sessionID), http.StatusNotFound)
		return
	}

	var req jsonRPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[MCP] Error decoding JSON-RPC request: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("[MCP] Request: method=%s, id=%v, session=%s", req.Method, req.ID, sessionID)
	w.WriteHeader(http.StatusAccepted)

	go func() {
		resp := s.dispatch(req)
		if resp != nil {
			data, err := json.Marshal(resp)
			if err == nil {
				select {
				case ch <- string(data):
				case <-time.After(5 * time.Second):
					log.Printf("[MCP] Timeout sending response to session %s", sessionID)
				}
			}
		}
	}()
}

func (s *MCPServer) dispatch(req jsonRPCRequest) *jsonRPCResponse {
	switch req.Method {
	case "initialize":
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"protocolVersion": "2024-11-05",
				"capabilities": map[string]any{
					"tools": map[string]any{},
				},
				"serverInfo": map[string]any{
					"name":    "strix-orchestrator",
					"version": "1.0.0",
				},
			},
		}

	case "notifications/initialized":
		return nil

	case "ping":
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  map[string]any{},
		}

	case "tools/list":
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"tools": s.toolDefinitions(),
			},
		}

	case "tools/call":
		return s.handleToolCall(req)

	default:
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error: map[string]any{
				"code":    -32601,
				"message": fmt.Sprintf("method %q not found", req.Method),
			},
		}
	}
}

func (s *MCPServer) toolDefinitions() []map[string]any {
	return []map[string]any{
		{
			"name":        "strix_orchestrator_draft_plan",
			"description": "Submit and validate a proposed DAG plan without executing it. Verifies acyclicity and registers it as a draft awaiting human approval.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"title":     map[string]any{"type": "string", "description": "Descriptive title of the plan"},
					"objective": map[string]any{"type": "string", "description": "Directive describing what to build or investigate"},
					"epics": map[string]any{
						"type":        "array",
						"description": "List of Epics, Stories, and Tasks forming an acyclic DAG",
						"items": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"id":    map[string]any{"type": "string", "description": "Epic ID, e.g. EPIC_001"},
								"title": map[string]any{"type": "string", "description": "Epic title"},
								"stories": map[string]any{
									"type": "array",
									"items": map[string]any{
										"type": "object",
										"properties": map[string]any{
											"id":    map[string]any{"type": "string", "description": "Story ID, e.g. STORY_001"},
											"title": map[string]any{"type": "string", "description": "Story title"},
											"tasks": map[string]any{
												"type": "array",
												"items": map[string]any{
													"type": "object",
													"properties": map[string]any{
														"id":          map[string]any{"type": "string", "description": "Unique task ID (REQUIRED), e.g. TASK_001"},
														"title":       map[string]any{"type": "string", "description": "Task title"},
														"description": map[string]any{"type": "string", "description": "Task description"},
														"task_type":   map[string]any{"type": "string", "description": "Task type: code_gen, text_extract, rag_query, etc."},
														"depends_on":  map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Prerequisite task IDs"},
													},
													"required": []string{"id", "title"},
												},
											},
										},
										"required": []string{"id", "title", "tasks"},
									},
								},
							},
							"required": []string{"id", "title", "stories"},
						},
					},
				},
				"required": []string{"title", "objective", "epics"},
			},
		},
		{
			"name":        "strix_orchestrator_approve_plan",
			"description": "Approve a previously drafted plan to move it into active DAG execution across local Ryzen AI NPU.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"plan_id": map[string]any{"type": "string", "description": "Deterministic ID of the draft plan"},
				},
				"required": []string{"plan_id"},
			},
		},
		{
			"name":        "strix_orchestrator_get_execution_status",
			"description": "Inspect live execution progress, NPU worker scores, and retries for an APPROVED, running plan. Requires approved plan_id.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"plan_id": map[string]any{"type": "string", "description": "Deterministic ID of the plan to inspect"},
				},
				"required": []string{"plan_id"},
			},
		},
		{
			"name":        "strix_orchestrator_rag_search",
			"description": "Search local documents, code, and logs using dense embeddings and cross-encoder reranking.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query":      map[string]any{"type": "string", "description": "Search query"},
					"collection": map[string]any{"type": "string", "description": "Qdrant collection name"},
					"top_k":      map[string]any{"type": "integer", "description": "Number of results to return"},
				},
				"required": []string{"query"},
			},
		},
		{
			"name":        "strix_orchestrator_ingest_file",
			"description": "Index a text document or log file into the local Qdrant vector database.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"content":    map[string]any{"type": "string", "description": "Document or log contents to index"},
					"collection": map[string]any{"type": "string", "description": "Qdrant collection name (default: default_corpus)"},
					"source":     map[string]any{"type": "string", "description": "File path or identifier"},
					"chunk_size": map[string]any{"type": "integer", "description": "Token chunk size (default: 512)"},
				},
				"required": []string{"content"},
			},
		},
	}
}

type toolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

func (s *MCPServer) handleToolCall(req jsonRPCRequest) *jsonRPCResponse {
	var params toolCallParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   map[string]any{"code": -32602, "message": "invalid params"},
		}
	}

	name := strings.TrimPrefix(params.Name, "strix_orchestrator_")
	name = strings.TrimPrefix(name, "orchestrator_")
	log.Printf("[MCP] Invoking tool %q (raw: %q) with args: %s", name, params.Name, string(params.Arguments))

	var outputText string
	var err error

	switch name {
	case "draft_plan":
		planSpec, err := llm.ParsePlanSpec(string(params.Arguments))
		if err != nil {
			outputText = fmt.Sprintf("Error parsing draft_plan arguments: %v", err)
			break
		}
		if _, err := engine.ValidatePlanSpec(planSpec, nil); err != nil {
			outputText = fmt.Sprintf("Plan validation failed: %v", err)
			break
		}
		replyCh := make(chan engine.CommandResult, 1)
		s.oCtx.EventCh <- engine.StoreDraftPlanCommand{Plan: planSpec, ReplyCh: replyCh}
		cmdRes := <-replyCh
		if !cmdRes.Success {
			outputText = fmt.Sprintf("Failed to save draft: %s", cmdRes.Message)
			break
		}
		planID := planSpec.ID
		if strID, ok := cmdRes.Data.(string); ok && strID != "" {
			planID = strID
		} else if pSpec, ok := cmdRes.Data.(llm.PlanSpec); ok && pSpec.ID != "" {
			planID = pSpec.ID
		}
		outputText = fmt.Sprintf("DRAFT PLAN CREATED (ID: %s)\nStatus: Draft (Awaiting User Approval)\nObjective: %s\n"+
			"Instruct the user to review this plan. To execute, call approve_plan with plan_id: %q.",
			planID, planSpec.Objective, planID)

	case "approve_plan":
		var in ApprovePlanArgs
		if err := json.Unmarshal(params.Arguments, &in); err != nil {
			outputText = fmt.Sprintf("Error parsing approve_plan arguments: %v", err)
			break
		}
		planID := strings.TrimSpace(in.PlanID)
		replyCh := make(chan engine.CommandResult, 1)
		s.oCtx.EventCh <- engine.SeedPlanEvent{PlanID: planID, RawJSON: "", ReplyCh: replyCh}
		cmdRes := <-replyCh
		if !cmdRes.Success {
			outputText = fmt.Sprintf("Approval failed: %s", cmdRes.Message)
			break
		}
		outputText = fmt.Sprintf("Plan %s APPROVED and actively executing across Ryzen AI NPU. Monitor via get_execution_status.", planID)

	case "get_execution_status", "get_plan_status":
		var in GetPlanStatusArgs
		if err := json.Unmarshal(params.Arguments, &in); err != nil {
			outputText = fmt.Sprintf("Error parsing get_plan_status arguments: %v", err)
			break
		}
		planID := strings.TrimSpace(in.PlanID)
		replyCh := make(chan engine.GetPlanResult, 1)
		s.oCtx.EventCh <- engine.GetPlanQuery{PlanID: planID, ReplyCh: replyCh}
		res := <-replyCh
		if !res.Found {
			outputText = fmt.Sprintf("Plan %s not found.", planID)
			break
		}
		outBytes, _ := json.MarshalIndent(res.Plan, "", "  ")
		outputText = string(outBytes)

	case "rag_search":
		var in RAGSearchArgs
		if err := json.Unmarshal(params.Arguments, &in); err != nil {
			outputText = fmt.Sprintf("Error parsing rag_search arguments: %v", err)
			break
		}
		if s.oCtx.RAGClient == nil {
			outputText = "RAG search client is not configured."
			break
		}
		col := in.Collection
		if col == "" {
			col = "default_corpus"
		}
		limit := in.TopK
		if limit <= 0 {
			limit = 5
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		results, err := s.oCtx.RAGClient.SearchAndRerank(ctx, col, in.Query, limit*2, limit)
		cancel()
		if err != nil {
			if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "doesn't exist") {
				outputText = fmt.Sprintf("Collection %q not found or empty. You can ingest files using strix_orchestrator_ingest_file(collection=%q, content=...).", col, col)
			} else {
				outputText = fmt.Sprintf("Search error: %v", err)
			}
			break
		}
		if len(results) == 0 {
			outputText = fmt.Sprintf("No relevant matches found in collection %q for query %q.", col, in.Query)
			break
		}
		var sb strings.Builder
		for i, r := range results {
			fmt.Fprintf(&sb, "[%d] Score: %.3f | Source: %s\n%s\n\n", i+1, r.Score, r.Source, r.Text)
		}
		outputText = sb.String()

	case "ingest_file", "index_text":
		type IngestFileArgs struct {
			Content    string `json:"content"`
			Collection string `json:"collection"`
			Source     string `json:"source"`
			ChunkSize  int    `json:"chunk_size"`
		}
		var in IngestFileArgs
		if err := json.Unmarshal(params.Arguments, &in); err != nil {
			outputText = fmt.Sprintf("Error parsing ingest_file arguments: %v", err)
			break
		}
		if s.oCtx.RAGClient == nil {
			outputText = "RAG client is not configured."
			break
		}
		col := in.Collection
		if col == "" {
			col = "default_corpus"
		}
		src := in.Source
		if src == "" {
			src = "continue_upload"
		}
		cs := in.ChunkSize
		if cs <= 0 {
			cs = 512
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		numChunks, err := s.oCtx.RAGClient.IndexText(ctx, col, in.Content, src, cs)
		cancel()
		if err != nil {
			outputText = fmt.Sprintf("Ingest error: %v", err)
			break
		}
		outputText = fmt.Sprintf("Successfully indexed %d chunks from %s into collection %q.", numChunks, src, col)

	default:
		outputText = fmt.Sprintf("Unknown tool %q", name)
	}

	if err != nil {
		outputText = fmt.Sprintf("Error: %v", err)
	}

	return &jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: map[string]any{
			"content": []map[string]any{
				{
					"type": "text",
					"text": outputText,
				},
			},
		},
	}
}
