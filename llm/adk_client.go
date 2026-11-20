package llm

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/adk/v2/model"
	"google.golang.org/adk/v2/model/openaimodel"
	"google.golang.org/genai"
)

// adkModelClient adapts any google.golang.org/adk/v2/model.LLM (such as
// openaimodel.NewModel) to the package's LLMClient interface, eliminating
// duplicate OpenAI HTTP transport code.
type adkModelClient struct {
	llm model.LLM
}

// NewADKClient wraps an existing ADK model.LLM as an LLMClient so callers
// (e.g. LRMPersonaEngine and the root ADK agent) share a single model client.
func NewADKClient(m model.LLM) LLMClient {
	return &adkModelClient{llm: m}
}

func newADKHTTPClient(baseURL string, cfg clientConfig) LLMClient {
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if !strings.HasSuffix(base, "/v1") {
		base += "/v1"
	}
	apiKey := cfg.apiKey
	if apiKey == "" {
		// Prevent openai-go from falling back to host OPENAI_API_KEY env var
		// and leaking it to local/custom endpoints.
		apiKey = "local"
	}
	modelName := cfg.model
	if modelName == "" {
		modelName = "default"
	}

	m, err := openaimodel.NewModel(context.Background(), modelName, &openaimodel.ClientConfig{
		APIKey:     apiKey,
		BaseURL:    base,
		HTTPClient: cfg.httpClient,
		API:        openaimodel.APIChatCompletions,
	})
	if err != nil {
		return &errLLMClient{err: fmt.Errorf("init ADK openaimodel for %s: %w", base, err)}
	}
	return &adkModelClient{llm: m}
}

type errLLMClient struct {
	err error
}

func (e *errLLMClient) Complete(context.Context, CompletionRequest) (CompletionResponse, error) {
	return CompletionResponse{}, e.err
}

// Complete executes a non-streaming GenerateContent call through the ADK model.LLM.
func (c *adkModelClient) Complete(ctx context.Context, req CompletionRequest) (CompletionResponse, error) {
	var sysParts []*genai.Part
	var contents []*genai.Content

	for _, m := range req.Messages {
		switch m.Role {
		case RoleSystem:
			sysParts = append(sysParts, genai.NewPartFromText(m.Content))
		case RoleAssistant:
			contents = append(contents, genai.NewContentFromText(m.Content, genai.RoleModel))
		default:
			contents = append(contents, genai.NewContentFromText(m.Content, genai.RoleUser))
		}
	}

	temp := float32(req.Temperature)
	genCfg := &genai.GenerateContentConfig{
		Temperature: &temp,
	}
	if len(sysParts) > 0 {
		genCfg.SystemInstruction = &genai.Content{Parts: sysParts}
	}
	if req.MaxTokens > 0 {
		genCfg.MaxOutputTokens = int32(req.MaxTokens)
	}
	if req.JSONMode {
		genCfg.ResponseMIMEType = "application/json"
	}

	llmReq := &model.LLMRequest{
		Model:    req.Model,
		Contents: contents,
		Config:   genCfg,
	}

	start := time.Now()
	var contentBuf strings.Builder
	var lastResp *model.LLMResponse

	for resp, err := range c.llm.GenerateContent(ctx, llmReq, false) {
		if err != nil {
			return CompletionResponse{}, fmt.Errorf("adk model %s (tag %s): %w", c.llm.Name(), req.Tag, err)
		}
		if resp == nil {
			continue
		}
		if resp.ErrorMessage != "" {
			return CompletionResponse{}, fmt.Errorf("adk model %s error (%s): %s", c.llm.Name(), resp.ErrorCode, resp.ErrorMessage)
		}
		lastResp = resp
		if resp.Content != nil {
			for _, p := range resp.Content.Parts {
				if p != nil && p.Text != "" && !p.Thought {
					contentBuf.WriteString(p.Text)
				}
			}
		}
	}

	out := CompletionResponse{
		Content: contentBuf.String(),
		Model:   c.llm.Name(),
		Latency: time.Since(start),
	}
	if lastResp != nil {
		if lastResp.ModelVersion != "" {
			out.Model = lastResp.ModelVersion
		}
		out.FinishReason = string(lastResp.FinishReason)
		if lastResp.UsageMetadata != nil {
			out.Usage = Usage{
				PromptTokens:     int(lastResp.UsageMetadata.PromptTokenCount),
				CompletionTokens: int(lastResp.UsageMetadata.CandidatesTokenCount),
				TotalTokens:      int(lastResp.UsageMetadata.TotalTokenCount),
			}
		}
	}
	return out, nil
}
