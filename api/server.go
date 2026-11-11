package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"go-orchestrator/engine"
	"go-orchestrator/llm"
)

// Server exposes the REST and Conversational API for the orchestrator engine.
type Server struct {
	port       int
	eventCh    chan any
	lrm        *llm.LRMPersonaEngine
	httpServer *http.Server
	log        *slog.Logger
}

// NewServer constructs a new API server instance.
func NewServer(port int, eventCh chan any, lrm *llm.LRMPersonaEngine) *Server {
	return &Server{
		port:    port,
		eventCh: eventCh,
		lrm:     lrm,
		log:     slog.Default(),
	}
}

// Start binds to the configured port and starts listening for HTTP traffic.
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// Go 1.22+ method + path routing
	mux.HandleFunc("POST /api/v1/chat", s.handleChat)
	mux.HandleFunc("GET /api/v1/plans", s.handleListPlans)
	mux.HandleFunc("GET /api/v1/plans/{planID}", s.handleGetPlan)
	mux.HandleFunc("POST /api/v1/plans/{planID}/approve", s.handleApprovePlan)
	mux.HandleFunc("POST /api/v1/plans/{planID}/tasks/{taskID}/clarify", s.handleClarifyTask)
	mux.HandleFunc("GET /openapi.yaml", s.handleOpenAPI)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	s.httpServer = &http.Server{
		Addr:              fmt.Sprintf(":%d", s.port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	s.log.Info("conversational API HTTP server starting", "port", s.port)
	if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server failure: %w", err)
	}
	return nil
}

// Shutdown gracefully terminates active HTTP connections.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer != nil {
		s.log.Info("shutting down HTTP server")
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

// ---------------------------------------------------------------------------
// HTTP Endpoint Handlers
// ---------------------------------------------------------------------------

type chatRequest struct {
	Message string        `json:"message"`
	PlanID  string        `json:"plan_id,omitempty"`
	History []llm.Message `json:"history,omitempty"`
}

type chatResponse struct {
	Reply     string        `json:"reply"`
	Intent    llm.Intent    `json:"intent"`
	Directive string        `json:"directive,omitempty"`
	DraftPlan *llm.PlanSpec `json:"draft_plan,omitempty"`
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if strings.TrimSpace(req.Message) == "" {
		writeError(w, http.StatusBadRequest, "message is required")
		return
	}

	// 1. Query real-time system state summary from engine event loop
	summary, err := dispatchQuery(r.Context(), s.eventCh, func(reply chan string) any {
		return engine.GetSystemStateQuery{ReplyCh: reply}
	})
	if err != nil {
		writeError(w, http.StatusGatewayTimeout, "failed to query engine state: "+err.Error())
		return
	}

	// 2. Invoke Conversational Hat
	reply, err := s.lrm.Converse(r.Context(), llm.ConversationInput{
		Message:      req.Message,
		History:      req.History,
		StateSummary: summary,
		FocusPlanID:  req.PlanID,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "conversational reasoning error: "+err.Error())
		return
	}

	resp := chatResponse{
		Reply:     reply.Reply,
		Intent:    reply.Intent,
		Directive: reply.Directive,
	}

	// 3. If user requested a new directive, invoke Planner Hat to draft a plan
	if reply.Intent == llm.IntentDraftPlan && reply.Directive != "" {
		// Provide capabilities for planning
		caps := []llm.Capability{
			{TaskType: "code_gen", Description: "Synthesizes source code artifacts and unit tests"},
			{TaskType: "text_extract", Description: "Extracts structured insights from documents"},
		}

		planDraft, err := s.lrm.DraftPlan(r.Context(), llm.PlanningInput{
			Directive:    reply.Directive,
			Capabilities: caps,
			StateSummary: summary,
		})
		if err == nil {
			// Save draft into engine
			draftCmdRes, err := dispatchQuery(r.Context(), s.eventCh, func(reply chan engine.CommandResult) any {
				return engine.StoreDraftPlanCommand{Plan: planDraft, ReplyCh: reply}
			})
			if err == nil && draftCmdRes.Success {
				if saved, ok := draftCmdRes.Data.(llm.PlanSpec); ok {
					planDraft = saved
				}
			}
			resp.DraftPlan = &planDraft
		} else {
			s.log.Warn("planner hat failed to draft plan", "err", err)
			resp.Reply += fmt.Sprintf("\n(Note: Automatic plan drafting encountered an issue: %v)", err)
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleListPlans(w http.ResponseWriter, r *http.Request) {
	plans, err := dispatchQuery(r.Context(), s.eventCh, func(reply chan []*engine.PlanNode) any {
		return engine.ListPlansQuery{ReplyCh: reply}
	})
	if err != nil {
		writeError(w, http.StatusGatewayTimeout, "engine query error: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, plans)
}

func (s *Server) handleGetPlan(w http.ResponseWriter, r *http.Request) {
	planID := r.PathValue("planID")
	if planID == "" {
		writeError(w, http.StatusBadRequest, "planID path parameter is required")
		return
	}

	res, err := dispatchQuery(r.Context(), s.eventCh, func(reply chan engine.GetPlanResult) any {
		return engine.GetPlanQuery{PlanID: planID, ReplyCh: reply}
	})
	if err != nil {
		writeError(w, http.StatusGatewayTimeout, "engine query error: "+err.Error())
		return
	}
	if !res.Found {
		writeError(w, http.StatusNotFound, fmt.Sprintf("plan %q not found", planID))
		return
	}

	writeJSON(w, http.StatusOK, res.Plan)
}

func (s *Server) handleApprovePlan(w http.ResponseWriter, r *http.Request) {
	planID := r.PathValue("planID")
	if planID == "" {
		writeError(w, http.StatusBadRequest, "planID path parameter is required")
		return
	}

	bodyBytes, _ := io.ReadAll(r.Body)
	rawJSON := strings.TrimSpace(string(bodyBytes))

	res, err := dispatchQuery(r.Context(), s.eventCh, func(reply chan engine.CommandResult) any {
		return engine.SeedPlanEvent{
			PlanID:  planID,
			RawJSON: rawJSON,
			ReplyCh: reply,
		}
	})
	if err != nil {
		writeError(w, http.StatusGatewayTimeout, "engine dispatch error: "+err.Error())
		return
	}
	if !res.Success {
		writeError(w, http.StatusBadRequest, res.Message+": "+res.Err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"plan_id": res.Data,
		"message": res.Message,
	})
}

type clarifyRequest struct {
	Answer string `json:"answer"`
}

func (s *Server) handleClarifyTask(w http.ResponseWriter, r *http.Request) {
	planID := r.PathValue("planID")
	taskID := r.PathValue("taskID")
	if planID == "" || taskID == "" {
		writeError(w, http.StatusBadRequest, "planID and taskID path parameters are required")
		return
	}

	var req clarifyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if strings.TrimSpace(req.Answer) == "" {
		writeError(w, http.StatusBadRequest, "answer must not be empty")
		return
	}

	// 1. Fetch current plan snapshot to inspect the paused task
	planRes, err := dispatchQuery(r.Context(), s.eventCh, func(reply chan engine.GetPlanResult) any {
		return engine.GetPlanQuery{PlanID: planID, ReplyCh: reply}
	})
	if err != nil {
		writeError(w, http.StatusGatewayTimeout, "engine query error: "+err.Error())
		return
	}
	if !planRes.Found {
		writeError(w, http.StatusNotFound, fmt.Sprintf("plan %q not found", planID))
		return
	}

	var targetTask *engine.TaskNode
	for _, e := range planRes.Plan.Epics {
		for _, st := range e.Stories {
			for _, t := range st.Tasks {
				if t.ID == taskID {
					targetTask = t
					break
				}
			}
		}
	}

	if targetTask == nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("task %q not found in plan %q", taskID, planID))
		return
	}
	if targetTask.Status != engine.StatusAwaitingClarification {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("task %q is not awaiting clarification (status: %s)", taskID, targetTask.Status))
		return
	}

	// 2. Interpret clarification using LRM Conversational Hat
	interpretation, err := s.lrm.InterpretClarification(r.Context(), llm.ClarificationInput{
		Task: llm.TaskSpec{
			ID:                 targetTask.ID,
			Title:              targetTask.Title,
			Description:        targetTask.Description,
			TaskType:           targetTask.TaskType,
			DependsOn:          targetTask.DependsOn,
			AcceptanceCriteria: targetTask.AcceptanceCriteria,
			Params:             targetTask.Params,
		},
		Explanation: targetTask.PendingExplanation,
		Question:    targetTask.PendingQuestion,
		Answer:      req.Answer,
		Feedback:    targetTask.FeedbackHistory,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "clarification interpretation failed: "+err.Error())
		return
	}

	// 3. Dispatch ClarificationResolvedEvent to engine event loop
	res, err := dispatchQuery(r.Context(), s.eventCh, func(reply chan engine.CommandResult) any {
		return engine.ClarificationResolvedEvent{
			PlanID:         planID,
			TaskID:         taskID,
			Interpretation: interpretation,
			Answer:         req.Answer,
			ReplyCh:        reply,
		}
	})
	if err != nil {
		writeError(w, http.StatusGatewayTimeout, "failed to apply clarification: "+err.Error())
		return
	}
	if !res.Success {
		writeError(w, http.StatusBadRequest, res.Message)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": res.Message,
		"task":    res.Data,
	})
}

func (s *Server) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(OpenAPIYAML))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func dispatchQuery[T any](ctx context.Context, ch chan any, buildMsg func(chan T) any) (T, error) {
	replyCh := make(chan T, 1)
	msg := buildMsg(replyCh)

	select {
	case ch <- msg:
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}

	select {
	case res := <-replyCh:
		return res, nil
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

