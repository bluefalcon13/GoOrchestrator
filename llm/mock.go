package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// MockClient is a deterministic, offline LLMClient used when no model server
// is available. It dispatches on CompletionRequest.Tag and reads structured
// hints from CompletionRequest.Metadata, producing contract-conformant JSON
// for every LRM hat and worker handler, so that the complete orchestration
// loop (planning, execution, review, retry, escalation, re-decomposition and
// user clarification) can be exercised end to end.
//
// Scenario triggers — whole words (case-insensitive) in a task description:
//
//	flaky       the first worker attempt is defective; it succeeds once reviewer
//	            feedback is present (exercises the tactical retry loop).
//	monolithic  every worker attempt is defective until the Project Manager splits
//	            the task (exercises strategic re-decomposition and DAG splicing).
//	ambiguous   the Reviewer requests clarification until the user answers
//	            (exercises AwaitingClarification, Blocked dependents and resume).
type MockClient struct {
	model   string
	latency time.Duration
}

// NewMockClient returns a deterministic mock with a fixed simulated latency.
func NewMockClient(model string, latency time.Duration) *MockClient {
	return &MockClient{model: model, latency: max(latency, 0)}
}

const mockDefectMarker = "MOCK_DEFECT"

// Complete implements LLMClient.
func (m *MockClient) Complete(ctx context.Context, req CompletionRequest) (CompletionResponse, error) {
	start := time.Now()
	if d := m.latencyFor(req.Tag); d > 0 {
		timer := time.NewTimer(d)
		select {
		case <-ctx.Done():
			timer.Stop()
			return CompletionResponse{}, ctx.Err()
		case <-timer.C:
		}
	}
	if err := ctx.Err(); err != nil {
		return CompletionResponse{}, err
	}
	meta := req.Metadata
	if meta == nil {
		meta = map[string]string{}
	}

	var content string
	switch req.Tag {
	case TagConversational:
		content = mustJSON(mockConverse(meta))
	case TagClarification:
		content = mustJSON(mockClarify(meta))
	case TagPlanner:
		content = mustJSON(mockPlan(meta))
	case TagProjectManager:
		content = mustJSON(mockTriage(meta))
	case TagReviewer:
		content = mustJSON(mockReview(meta))
	case TagWorkerCodeGen:
		content = mockCodeGen(meta)
	case TagWorkerTextExtract:
		content = mockExtract(meta)
	case TagWorkerCompact:
		content = mockCompact(meta)
	default:
		last := ""
		if n := len(req.Messages); n > 0 {
			last = req.Messages[n-1].Content
		}
		content = mustJSON(map[string]string{"reply": "mock response", "echo": truncate(last, 200)})
	}

	promptChars := 0
	for _, msg := range req.Messages {
		promptChars += len(msg.Content)
	}
	usage := Usage{PromptTokens: promptChars/4 + 1, CompletionTokens: len(content)/4 + 1}
	usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens
	return CompletionResponse{
		Content:      content,
		Model:        m.model,
		FinishReason: "stop",
		Usage:        usage,
		Latency:      time.Since(start),
	}, nil
}

func (m *MockClient) latencyFor(tag string) time.Duration {
	if tag == TagWorkerCompact {
		return m.latency / 4
	}
	return m.latency
}

// ---------------------------------------------------------------------------
// LRM hats
// ---------------------------------------------------------------------------

func mockConverse(meta map[string]string) ConversationReply {
	msg := strings.TrimSpace(meta[MetaMessage])
	if directive, ok := detectDirective(msg); ok {
		return ConversationReply{
			Intent:    IntentDraftPlan,
			Directive: directive,
			Reply: fmt.Sprintf("Understood. I've asked the Planner to draft a plan for: %q. "+
				"Review the draft and approve it to start execution.", truncate(directive, 160)),
		}
	}

	summary := strings.TrimSpace(meta[MetaStateSummary])
	lower := strings.ToLower(msg)
	var b strings.Builder
	if summary == "" || strings.HasPrefix(summary, "No plans") {
		b.WriteString("There are no plans in the system yet. Describe what you want built — for example " +
			"\"plan: build a token-bucket rate limiter in Go\" — and I'll draft a plan for your approval.")
		return ConversationReply{Intent: IntentAnswer, Reply: b.String()}
	}
	if containsAny(lower, "block", "stuck", "clarif", "question", "wait", "pause", "why", "fail") {
		if attention := filterLines(summary, "clarification", "awaiting", "failed", "blocked", "question"); attention != "" {
			b.WriteString("These items need attention:\n")
			b.WriteString(attention)
			b.WriteString("\n\nAnswer a pending question with POST /api/v1/plans/{planID}/tasks/{taskID}/clarify.\n\n")
		} else {
			b.WriteString("Nothing is blocked or waiting on you right now.\n\n")
		}
	}
	b.WriteString("Current system state:\n")
	b.WriteString(summary)
	return ConversationReply{Intent: IntentAnswer, Reply: b.String()}
}

func detectDirective(msg string) (string, bool) {
	trimmed := strings.TrimSpace(msg)
	lower := strings.ToLower(trimmed)
	for _, prefix := range []string{"plan:", "/plan", "directive:"} {
		if strings.HasPrefix(lower, prefix) {
			return strings.TrimSpace(trimmed[len(prefix):]), true
		}
	}
	if strings.HasSuffix(lower, "?") {
		return "", false
	}
	for _, q := range []string{"what", "why", "how", "when", "where", "which", "who", "is ", "are ", "can ", "status", "show", "explain", "list"} {
		if strings.HasPrefix(lower, q) {
			return "", false
		}
	}
	for _, verb := range []string{"build", "create", "implement", "develop", "generate", "write", "design", "make", "extract", "produce", "draft"} {
		if hasWord(lower, verb) {
			return trimmed, true
		}
	}
	return "", false
}

func mockClarify(meta map[string]string) ClarificationInterpretation {
	answer := strings.TrimSpace(meta[MetaAnswer])
	return ClarificationInterpretation{
		AmendedDescription:           strings.TrimSpace(meta[MetaDescription]) + "\n\nClarified by the user: " + answer,
		AdditionalAcceptanceCriteria: []string{"Behaviour matches the user's clarification: " + truncate(answer, 160)},
		Acknowledgement: fmt.Sprintf("Thanks — task %s now incorporates your answer and has been re-queued for execution.",
			meta[MetaTaskID]),
	}
}

func mockPlan(meta map[string]string) PlanSpec {
	directive := strings.TrimSpace(meta[MetaDirective])
	if directive == "" {
		directive = "Unspecified objective"
	}
	types := splitCSV(meta[MetaTaskTypes])
	pick := func(want string) string {
		if len(types) == 0 || slices.Contains(types, want) {
			return want
		}
		return types[0]
	}
	extract, code := pick("text_extract"), pick("code_gen")
	params := func(v any) json.RawMessage { return json.RawMessage(mustJSON(v)) }

	return PlanSpec{
		Title:     "Plan: " + truncate(directive, 72),
		Objective: directive,
		Epics: []EpicSpec{
			{
				ID: "E1", Title: "Discovery & specification",
				Description: "Turn the directive into structured, verifiable requirements.",
				Stories: []StorySpec{{
					ID: "E1.S1", Title: "Requirements analysis",
					Tasks: []TaskSpec{{
						ID: "T1", Title: "Extract structured requirements",
						Description: "Extract the goals, constraints, deliverables and risks stated in the directive document.",
						TaskType:    extract,
						AcceptanceCriteria: []string{
							"Each requested field is present when the document supports it",
							"Every insight cites evidence from the document",
						},
						Params: params(map[string]any{"document": directive, "fields": []string{"goals", "constraints", "deliverables", "risks"}}),
					}},
				}},
			},
			{
				ID: "E2", Title: "Implementation",
				Description: "Deliver working, tested code for the objective.",
				Stories: []StorySpec{
					{
						ID: "E2.S1", Title: "Core build",
						Tasks: []TaskSpec{
							{
								ID: "T2", Title: "Define domain types and interfaces",
								Description: "Define the exported Go domain types and interfaces required by the objective, guided by the extracted requirements.",
								TaskType:    code, DependsOn: []string{"T1"},
								AcceptanceCriteria: []string{"Declares exported types with doc comments", "Compiles as valid Go"},
								Params:             params(map[string]string{"language": "go", "filename": "types.go", "package": "app"}),
							},
							{
								ID: "T3", Title: "Implement core logic",
								Description: "Implement the core logic for the following objective using the upstream types: " + directive,
								TaskType:    code, DependsOn: []string{"T1", "T2"},
								AcceptanceCriteria: []string{"Implements the behaviour without placeholders", "Returns errors instead of panicking on invalid input"},
								Params:             params(map[string]string{"language": "go", "filename": "core.go", "package": "app"}),
							},
						},
					},
					{
						ID: "E2.S2", Title: "Quality & documentation",
						Tasks: []TaskSpec{
							{
								ID: "T4", Title: "Write table-driven unit tests",
								Description: "Write table-driven unit tests covering normal and edge cases of the core logic.",
								TaskType:    code, DependsOn: []string{"T3"},
								AcceptanceCriteria: []string{"Uses table-driven subtests", "Covers the empty-input edge case"},
								Params:             params(map[string]string{"language": "go", "filename": "core_test.go", "package": "app"}),
							},
							{
								ID: "T5", Title: "Extract release notes",
								Description: "Extract the delivered features, public APIs and known limitations from the implementation and tests.",
								TaskType:    extract, DependsOn: []string{"T3", "T4"},
								AcceptanceCriteria: []string{"Lists public APIs", "Mentions known limitations"},
								Params:             params(map[string]any{"fields": []string{"features", "apis", "limitations"}}),
							},
						},
					},
				},
			},
		},
	}
}

func mockReview(meta map[string]string) ReviewVerdict {
	out := meta[MetaOutput]
	desc := strings.ToLower(meta[MetaDescription])
	switch {
	case strings.TrimSpace(out) == "":
		return ReviewVerdict{Feedback: "The output is empty. Produce the complete artefact described by the task.", Score: 0}
	case strings.Contains(out, mockDefectMarker):
		return ReviewVerdict{
			Feedback: "The implementation contains a placeholder that panics (\"not implemented\") instead of real logic. " +
				"Replace the panic with a complete implementation, validate empty input, and return a descriptive error instead of panicking.",
			Score: 0.3,
		}
	case hasWord(desc, "ambiguous") && meta[MetaHasClarification] != "true":
		return ReviewVerdict{
			NeedsClarification: true,
			Feedback: "The task description is ambiguous: it does not define the exact expected behaviour (inputs, outputs and " +
				"edge-case policy), so no output can be verified against it.",
			Score: 0.5,
		}
	default:
		return ReviewVerdict{Approved: true, Feedback: "All acceptance criteria are satisfied.", Score: 0.92}
	}
}

func mockTriage(meta map[string]string) Remediation {
	id, title := meta[MetaTaskID], meta[MetaTitle]
	feedback := meta[MetaFeedback]
	if meta[MetaReason] != "needs_clarification" && meta[MetaCanSplit] == "true" {
		base := scrubMarkers(meta[MetaDescription])
		tt := meta[MetaTaskType]
		return Remediation{
			Action: ActionSplit,
			Rationale: fmt.Sprintf("Task %s failed review after %s attempt(s) with recurring defects; the work mixes scaffolding "+
				"and full implementation, which is too much for the worker in one pass. Splitting it into two focused steps.", id, meta[MetaAttempt]),
			SubTasks: []TaskSpec{
				{
					ID: "s1", Title: title + " — part 1: scaffolding and interfaces",
					Description: "Create the types, function signatures and input validation for: " + base,
					TaskType:    tt,
					AcceptanceCriteria: []string{"Declares every type and function signature needed", "Contains no placeholder panics"},
				},
				{
					ID: "s2", Title: title + " — part 2: complete implementation",
					Description: "Complete the implementation, building on part 1, for: " + base,
					TaskType:    tt, DependsOn: []string{"s1"},
					AcceptanceCriteria: []string{"Implements the full behaviour", "Returns an error on empty input"},
				},
			},
		}
	}
	why := "the reviewer could not verify the result against the specification"
	if feedback != "" {
		why = "the reviewer reported: " + feedback
	}
	return Remediation{
		Action:      ActionClarify,
		Rationale:   "The failure is caused by under-specified requirements that only the user can resolve.",
		Explanation: fmt.Sprintf("Task %s (%q) could not be accepted because %s", id, title, why),
		Question: fmt.Sprintf("To finish %q, please specify the exact expected behaviour: what input it receives, what it must "+
			"return, and how edge cases (empty or invalid input) should be handled.", title),
	}
}

// ---------------------------------------------------------------------------
// Worker handlers
// ---------------------------------------------------------------------------

func mockCodeGen(meta map[string]string) string {
	title := strings.TrimSpace(meta[MetaTitle])
	if title == "" {
		title = "generated"
	}
	desc := strings.ToLower(meta[MetaDescription])
	lang := strings.ToLower(strings.TrimSpace(meta[MetaLanguage]))
	if lang == "" {
		lang = "go"
	}
	pkg := strings.TrimSpace(meta[MetaPackage])
	if pkg == "" {
		pkg = "generated"
	}
	name := exportedIdent(title)
	file := strings.TrimSpace(meta[MetaFileName])
	if file == "" {
		ext := ".go"
		if lang == "python" || lang == "py" {
			ext = ".py"
		}
		file = snakeIdent(title) + ext
	}
	defective := hasWord(desc, "monolithic") || (hasWord(desc, "flaky") && meta[MetaHasFeedback] != "true")

	var code string
	switch {
	case lang == "python" || lang == "py":
		code = pythonStub(name, title, meta[MetaTaskID], defective)
	case strings.HasSuffix(file, "_test.go"):
		code = goTestStub(pkg, name, meta[MetaTaskID], meta[MetaUpstream])
	default:
		code = goStub(pkg, name, title, meta[MetaTaskID], meta[MetaUpstream], defective)
	}
	notes := "Complete implementation synthesized deterministically by the mock worker."
	if defective {
		notes = "Draft implementation; core logic still pending."
	}
	return mustJSON(map[string]any{
		"files": []map[string]string{{"path": file, "language": lang, "content": code}},
		"notes": notes,
	})
}

func goStub(pkg, name, title, taskID, upstream string, defective bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "// Code synthesized by the worker model for task %s: %s.\n", taskID, title)
	if upstream != "" {
		fmt.Fprintf(&b, "// Builds on upstream task outputs: %s.\n", upstream)
	}
	fmt.Fprintf(&b, "\npackage %s\n\n", pkg)
	if defective {
		fmt.Fprintf(&b, "// %sResult holds the outcome of %s.\ntype %sResult struct {\n\tItems []string\n}\n\n", name, name, name)
		fmt.Fprintf(&b, "// %s is not finished yet.\nfunc %s(input string) (%sResult, error) {\n\t// %s: placeholder implementation\n\tpanic(\"not implemented\")\n}\n",
			name, name, name, mockDefectMarker)
		return b.String()
	}
	b.WriteString("import (\n\t\"errors\"\n\t\"strings\"\n)\n\n")
	fmt.Fprintf(&b, "// Err%sEmptyInput is returned when %s receives blank input.\nvar Err%sEmptyInput = errors.New(%q)\n\n",
		name, name, name, strings.ToLower(name)+": empty input")
	fmt.Fprintf(&b, "// %sResult holds the outcome of %s.\ntype %sResult struct {\n\tItems []string\n}\n\n", name, name, name)
	fmt.Fprintf(&b, "// %s normalises input into its constituent items.\nfunc %s(input string) (%sResult, error) {\n"+
		"\tif strings.TrimSpace(input) == \"\" {\n\t\treturn %sResult{}, Err%sEmptyInput\n\t}\n"+
		"\treturn %sResult{Items: strings.Fields(input)}, nil\n}\n", name, name, name, name, name, name)
	return b.String()
}

func goTestStub(pkg, name, taskID, upstream string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "// Tests synthesized by the worker model for task %s.\n", taskID)
	if upstream != "" {
		fmt.Fprintf(&b, "// Exercises upstream task outputs: %s.\n", upstream)
	}
	fmt.Fprintf(&b, "\npackage %s\n\nimport (\n\t\"strings\"\n\t\"testing\"\n)\n\n", pkg)
	fmt.Fprintf(&b, "func Test%s(t *testing.T) {\n", name)
	b.WriteString("\tcases := []struct {\n\t\tname  string\n\t\tinput string\n\t\twant  int\n\t}{\n" +
		"\t\t{name: \"empty input\", input: \"\", want: 0},\n" +
		"\t\t{name: \"single item\", input: \"alpha\", want: 1},\n" +
		"\t\t{name: \"multiple items\", input: \"alpha beta  gamma\", want: 3},\n\t}\n" +
		"\tfor _, tc := range cases {\n\t\tt.Run(tc.name, func(t *testing.T) {\n" +
		"\t\t\tif got := len(strings.Fields(tc.input)); got != tc.want {\n" +
		"\t\t\t\tt.Fatalf(\"got %d items, want %d\", got, tc.want)\n\t\t\t}\n\t\t})\n\t}\n}\n")
	return b.String()
}

func pythonStub(name, title, taskID string, defective bool) string {
	fn := snakeIdent(name)
	if defective {
		return fmt.Sprintf("# Synthesized for task %s: %s\n\n\ndef %s(text: str) -> list[str]:\n    # %s: placeholder implementation\n    raise NotImplementedError\n",
			taskID, title, fn, mockDefectMarker)
	}
	return fmt.Sprintf("# Synthesized for task %s: %s\n\n\ndef %s(text: str) -> list[str]:\n    \"\"\"Normalise text into its constituent items.\"\"\"\n"+
		"    if not text.strip():\n        raise ValueError(\"%s: empty input\")\n    return text.split()\n", taskID, title, fn, fn)
}

type mockInsight struct {
	Field      string  `json:"field"`
	Value      string  `json:"value"`
	Evidence   string  `json:"evidence"`
	Confidence float64 `json:"confidence"`
}

func mockExtract(meta map[string]string) string {
	chunk := meta[MetaChunk]
	fields := splitCSV(meta[MetaFields])
	sentences := splitSentences(chunk)

	insights := []mockInsight{}
	for _, field := range fields {
		var matches []string
		kws := fieldKeywords(field)
		for _, s := range sentences {
			ls := strings.ToLower(s)
			for _, kw := range kws {
				if strings.Contains(ls, kw) {
					matches = append(matches, s)
					break
				}
			}
		}
		if len(matches) == 0 {
			continue
		}
		insights = append(insights, mockInsight{
			Field:      field,
			Value:      truncate(matches[0], 240),
			Evidence:   truncate(strings.Join(matches[:min(len(matches), 3)], " | "), 480),
			Confidence: min(0.5+0.1*float64(len(matches)), 0.95),
		})
	}
	summary := ""
	if len(sentences) > 0 {
		summary = truncate(sentences[0], 200)
	}
	return mustJSON(map[string]any{
		"insights": insights,
		"entities": topTokens(chunk, true, 8),
		"keywords": topTokens(chunk, false, 8),
		"summary":  summary,
	})
}

func mockCompact(meta map[string]string) string {
	text := []rune(meta[MetaText])
	budget, err := strconv.Atoi(meta[MetaBudget])
	if err != nil || budget <= 0 {
		budget = 2000
	}
	if len(text) <= budget {
		return string(text)
	}
	head := budget * 2 / 3
	tail := budget - head
	return fmt.Sprintf("%s\n…[%d characters condensed by the worker model]…\n%s",
		string(text[:head]), len(text)-head-tail, string(text[len(text)-tail:]))
}

// ---------------------------------------------------------------------------
// text helpers
// ---------------------------------------------------------------------------

var markerRe = regexp.MustCompile(`(?i)\b(flaky|monolithic|ambiguous)\b`)

func scrubMarkers(s string) string {
	return strings.Join(strings.Fields(markerRe.ReplaceAllString(s, "")), " ")
}

func words(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
}

func hasWord(text, word string) bool {
	for _, w := range words(strings.ToLower(text)) {
		if w == word {
			return true
		}
	}
	return false
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func filterLines(text string, keywords ...string) string {
	var out []string
	for _, line := range strings.Split(text, "\n") {
		if containsAny(strings.ToLower(line), keywords...) {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func splitSentences(s string) []string {
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == '.' || r == '!' || r == '?' || r == '\n' })
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); len(p) >= 3 {
			out = append(out, p)
		}
	}
	return out
}

func fieldKeywords(field string) []string {
	f := strings.ToLower(strings.TrimSpace(field))
	synonyms := map[string][]string{
		"goals":        {"goal", "objective", "aim", "should", "must", "need", "want"},
		"constraints":  {"constraint", "must not", "limit", "within", "only", "require", "without"},
		"deliverables": {"deliver", "provide", "produce", "output", "build", "implement", "create", "write"},
		"risks":        {"risk", "ambiguous", "unclear", "fail", "concern", "issue", "flaky"},
		"features":     {"func", "feature", "support", "type", "implements"},
		"apis":         {"func ", "interface", "endpoint", "api", "type "},
		"limitations":  {"limit", "not ", "panic", "error", "only", "todo"},
	}
	kws := []string{f}
	if len(f) > 5 {
		kws = append(kws, f[:5])
	}
	return append(kws, synonyms[f]...)
}

var stopwords = map[string]bool{
	"the": true, "this": true, "that": true, "with": true, "from": true, "into": true, "and": true, "for": true,
	"should": true, "would": true, "could": true, "their": true, "there": true, "which": true, "where": true,
	"about": true, "these": true, "those": true, "while": true, "string": true, "return": true, "package": true,
	"func": true, "type": true, "import": true, "struct": true, "error": true, "errors": true, "strings": true,
}

func topTokens(text string, capitalised bool, n int) []string {
	counts := map[string]int{}
	for _, w := range words(text) {
		r := []rune(w)
		if len(r) < 3 || stopwords[strings.ToLower(w)] {
			continue
		}
		if capitalised {
			if !unicode.IsUpper(r[0]) {
				continue
			}
			counts[w]++
		} else if len(r) >= 5 {
			counts[strings.ToLower(w)]++
		}
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if counts[keys[i]] != counts[keys[j]] {
			return counts[keys[i]] > counts[keys[j]]
		}
		return keys[i] < keys[j]
	})
	if len(keys) > n {
		keys = keys[:n]
	}
	return keys
}

func exportedIdent(title string) string {
	ws := words(title)
	if len(ws) > 5 {
		ws = ws[:5]
	}
	var b strings.Builder
	for _, w := range ws {
		r := []rune(strings.ToLower(w))
		r[0] = unicode.ToUpper(r[0])
		b.WriteString(string(r))
	}
	id := b.String()
	if id == "" {
		return "Generated"
	}
	if unicode.IsDigit([]rune(id)[0]) {
		id = "X" + id
	}
	return id
}

func snakeIdent(s string) string {
	var out []rune
	for i, r := range []rune(s) {
		switch {
		case unicode.IsUpper(r):
			if i > 0 && len(out) > 0 && out[len(out)-1] != '_' {
				out = append(out, '_')
			}
			out = append(out, unicode.ToLower(r))
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			out = append(out, r)
		default:
			if len(out) > 0 && out[len(out)-1] != '_' {
				out = append(out, '_')
			}
		}
	}
	id := strings.Trim(string(out), "_")
	if id == "" {
		return "generated"
	}
	return id
}

