package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"go-orchestrator/llm"
)

// CodeGenParams represents the optional JSON configuration passed in TaskSpec.Params for code_gen.
type CodeGenParams struct {
	Language string `json:"language,omitempty"`
	Filename string `json:"filename,omitempty"`
	Package  string `json:"package,omitempty"`
}

// CodeGenHandler synthesizes source code artifacts using the Small Task Model.
type CodeGenHandler struct {
	client llm.LLMClient
}

// NewCodeGenHandler creates a new CodeGenHandler.
func NewCodeGenHandler(client llm.LLMClient) *CodeGenHandler {
	return &CodeGenHandler{client: client}
}

func (h *CodeGenHandler) Description() string {
	return "Synthesizes production source code, unit tests, and interface implementations based on upstream contracts."
}

type codeGenResponse struct {
	Files []struct {
		Path     string `json:"path"`
		Language string `json:"language"`
		Content  string `json:"content"`
	} `json:"files"`
	Notes string `json:"notes"`
}

// Execute fulfills the TaskHandler interface for code generation.
func (h *CodeGenHandler) Execute(ctx context.Context, input TaskInput) (TaskOutput, error) {
	var params CodeGenParams
	if len(input.Params) > 0 && string(input.Params) != "null" {
		_ = json.Unmarshal(input.Params, &params)
	}
	if params.Language == "" {
		params.Language = "go"
	}

	// Prepare upstream summary
	var upstreamSummary strings.Builder
	for id, out := range input.UpstreamOutputs {
		upstreamSummary.WriteString(fmt.Sprintf("--- Upstream Task %s Output ---\n%s\n\n", id, out))
	}

	// Metadata for mock and structured tracing
	meta := map[string]string{
		llm.MetaTaskID:          input.TaskID,
		llm.MetaTitle:           input.Title,
		llm.MetaDescription:     input.Description,
		llm.MetaLanguage:        params.Language,
		llm.MetaFileName:        params.Filename,
		llm.MetaPackage:         params.Package,
		llm.MetaAttempt:         strconv.Itoa(input.Attempt),
		llm.MetaHasFeedback:      strconv.FormatBool(len(input.Feedback) > 0),
		llm.MetaHasClarification: strconv.FormatBool(len(input.Clarifications) > 0),
		llm.MetaUpstream:        strings.Join(mapKeys(input.UpstreamOutputs), ", "),
	}

	var prompt strings.Builder
	prompt.WriteString(fmt.Sprintf("Task ID: %s\nTitle: %s\nTarget Language: %s\n\n", input.TaskID, input.Title, params.Language))
	if params.Package != "" {
		prompt.WriteString(fmt.Sprintf("Package: %s\n", params.Package))
	}
	if params.Filename != "" {
		prompt.WriteString(fmt.Sprintf("Preferred Filename: %s\n", params.Filename))
	}
	prompt.WriteString(fmt.Sprintf("Description:\n%s\n\n", input.Description))

	if upstreamSummary.Len() > 0 {
		prompt.WriteString("## Upstream Contracts and Artifacts:\n")
		prompt.WriteString(upstreamSummary.String())
	}

	if len(input.Feedback) > 0 {
		prompt.WriteString("## Prior Reviewer Feedback (MUST BE ADDRESSED):\n")
		for _, fb := range input.Feedback {
			prompt.WriteString(fmt.Sprintf("- %s\n", fb))
		}
		prompt.WriteString("\n")
	}

	if len(input.Clarifications) > 0 {
		prompt.WriteString("## User Clarifications:\n")
		for _, c := range input.Clarifications {
			prompt.WriteString(fmt.Sprintf("- %s\n", c))
		}
		prompt.WriteString("\n")
	}

	prompt.WriteString(`Output Format:
Return a JSON object conforming strictly to:
{
  "files": [
    { "path": "filename.ext", "language": "language", "content": "complete code" }
  ],
  "notes": "concise explanation of design decisions"
}
Do not use markdown code blocks outside of the JSON payload.`)

	resp, err := h.client.Complete(ctx, llm.CompletionRequest{
		Messages: []llm.Message{
			{
				Role:    llm.RoleSystem,
				Content: "You are an expert systems software engineer synthesizing clean, robust code without placeholders or stubs.",
			},
			{
				Role:    llm.RoleUser,
				Content: prompt.String(),
			},
		},
		Temperature: 0.1,
		JSONMode:    true,
		Tag:         llm.TagWorkerCodeGen,
		Metadata:    meta,
	})
	if err != nil {
		return TaskOutput{}, fmt.Errorf("code_gen execution failed: %w", err)
	}

	parsed, err := llm.DecodeJSON[codeGenResponse](resp.Content)
	artifacts := make(map[string]string)
	if err == nil && len(parsed.Files) > 0 {
		for _, f := range parsed.Files {
			artifacts[f.Path] = f.Content
		}
		return TaskOutput{
			Content:   resp.Content,
			Artifacts: artifacts,
			Metadata: map[string]any{
				"file_count": len(parsed.Files),
				"notes":      parsed.Notes,
			},
		}, nil
	}

	// Fallback if unstructured
	path := params.Filename
	if path == "" {
		path = "generated_code." + params.Language
	}
	artifacts[path] = resp.Content

	return TaskOutput{
		Content:   resp.Content,
		Artifacts: artifacts,
		Metadata: map[string]any{
			"raw_unstructured": true,
		},
	}, nil
}

func mapKeys[K comparable, V any](m map[K]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, fmt.Sprint(k))
	}
	return keys
}

