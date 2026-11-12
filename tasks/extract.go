package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"go-orchestrator/llm"
	"go-orchestrator/rag"
)

// TextExtractParams holds parameters for the text_extract task handler.
type TextExtractParams struct {
	Document  string   `json:"document,omitempty"`
	Fields    []string `json:"fields,omitempty"`
	ChunkSize int      `json:"chunk_size,omitempty"`
}

// TextExtractHandler performs chunked extraction of structured insights from unstructured text.
type TextExtractHandler struct {
	client    llm.LLMClient
	ragClient rag.Client
}

// NewTextExtractHandler creates a new TextExtractHandler.
func NewTextExtractHandler(client llm.LLMClient, ragClient ...rag.Client) *TextExtractHandler {
	var rc rag.Client
	if len(ragClient) > 0 {
		rc = ragClient[0]
	}
	return &TextExtractHandler{client: client, ragClient: rc}
}

func (h *TextExtractHandler) Description() string {
	return "Extracts structured insights, entities, and key metrics from large unstructured text documents using chunked analysis."
}

type singleChunkResult struct {
	Insights []struct {
		Field      string  `json:"field"`
		Value      string  `json:"value"`
		Evidence   string  `json:"evidence"`
		Confidence float64 `json:"confidence"`
	} `json:"insights"`
	Entities []string `json:"entities"`
	Keywords []string `json:"keywords"`
	Summary  string   `json:"summary"`
}

type aggregatedExtractResult struct {
	Insights []struct {
		Field      string  `json:"field"`
		Value      string  `json:"value"`
		Evidence   string  `json:"evidence"`
		Confidence float64 `json:"confidence"`
	} `json:"insights"`
	Entities []string `json:"entities"`
	Keywords []string `json:"keywords"`
	Summary  string   `json:"summary"`
	Chunks   int      `json:"chunks_analyzed"`
}

// Execute performs chunked structured information extraction.
func (h *TextExtractHandler) Execute(ctx context.Context, input TaskInput) (TaskOutput, error) {
	var params TextExtractParams
	if len(input.Params) > 0 && string(input.Params) != "null" {
		_ = json.Unmarshal(input.Params, &params)
	}

	doc := params.Document
	if doc == "" {
		// Try using upstream outputs if no explicit document in params
		for _, up := range input.UpstreamOutputs {
			if doc != "" {
				doc += "\n\n"
			}
			doc += up
		}
	}
	if doc == "" {
		doc = input.Description
	}

	// 1. If doc points to a local file path, attempt reading the file contents directly
	if len(doc) < 300 && strings.Contains(doc, ".") {
		for _, prefix := range []string{"", "./", "/data/", "data/"} {
			target := prefix + doc
			if fileData, err := os.ReadFile(target); err == nil && len(fileData) > 0 {
				doc = string(fileData)
				break
			}
		}
	}

	// 2. If doc is still just a short filename/title and RAG is available, retrieve relevant passages
	if len(doc) < 200 && h.ragClient != nil {
		searchQuery := input.Title + " " + strings.Join(params.Fields, " ")
		if matches, err := h.ragClient.SearchAndRerank(ctx, "default_corpus", searchQuery, 20, 5); err == nil && len(matches) > 0 {
			var sb strings.Builder
			for _, m := range matches {
				sb.WriteString(m.Text)
				sb.WriteString("\n\n")
			}
			doc = sb.String()
		}
	}

	if len(params.Fields) == 0 {
		params.Fields = []string{"goals", "constraints", "deliverables", "risks"}
	}

	chunkSize := params.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 3500
	}

	chunks := splitTextIntoChunks(doc, chunkSize)
	if len(chunks) == 0 {
		chunks = []string{doc}
	}

	var aggregated aggregatedExtractResult
	aggregated.Chunks = len(chunks)
	fieldsCSV := strings.Join(params.Fields, ",")

	entitySet := make(map[string]bool)
	keywordSet := make(map[string]bool)
	var summaries []string

	for idx, chunk := range chunks {
		if err := ctx.Err(); err != nil {
			return TaskOutput{}, err
		}

		meta := map[string]string{
			llm.MetaTaskID:     input.TaskID,
			llm.MetaTitle:      input.Title,
			llm.MetaChunk:      chunk,
			llm.MetaChunkIndex: strconv.Itoa(idx),
			llm.MetaFields:     fieldsCSV,
		}

		prompt := fmt.Sprintf(`Extract structured information from the following text chunk for the requested fields: %s.
Chunk %d of %d:
"""
%s
"""

Return a JSON object conforming to:
{
  "insights": [
    { "field": "field_name", "value": "extracted fact", "evidence": "direct quote/sentence", "confidence": 0.9 }
  ],
  "entities": ["entity1", "entity2"],
  "keywords": ["kw1", "kw2"],
  "summary": "one-sentence summary of this chunk"
}`, fieldsCSV, idx+1, len(chunks), chunk)

		resp, err := h.client.Complete(ctx, llm.CompletionRequest{
			Messages: []llm.Message{
				{
					Role:    llm.RoleSystem,
					Content: "You are an analytical data extraction specialist extracting precise structured insights from text.",
				},
				{
					Role:    llm.RoleUser,
					Content: prompt,
				},
			},
			Temperature: 0.0,
			JSONMode:    true,
			Tag:         llm.TagWorkerTextExtract,
			Metadata:    meta,
		})
		if err != nil {
			return TaskOutput{}, fmt.Errorf("chunk %d extraction failed: %w", idx, err)
		}

		parsed, err := llm.DecodeJSON[singleChunkResult](resp.Content)
		if err == nil {
			aggregated.Insights = append(aggregated.Insights, parsed.Insights...)
			for _, e := range parsed.Entities {
				entitySet[e] = true
			}
			for _, k := range parsed.Keywords {
				keywordSet[k] = true
			}
			if parsed.Summary != "" {
				summaries = append(summaries, parsed.Summary)
			}
		}
	}

	for e := range entitySet {
		aggregated.Entities = append(aggregated.Entities, e)
	}
	for k := range keywordSet {
		aggregated.Keywords = append(aggregated.Keywords, k)
	}
	aggregated.Summary = strings.Join(summaries, " ")

	outBytes, err := json.MarshalIndent(aggregated, "", "  ")
	if err != nil {
		return TaskOutput{}, fmt.Errorf("marshal extraction output: %w", err)
	}

	return TaskOutput{
		Content: string(outBytes),
		Metadata: map[string]any{
			"insight_count": len(aggregated.Insights),
			"chunks_count":  len(chunks),
		},
	}, nil
}

func splitTextIntoChunks(s string, chunkSize int) []string {
	if len(s) <= chunkSize {
		return []string{s}
	}
	var chunks []string
	lines := strings.Split(s, "\n")
	var current strings.Builder

	for _, line := range lines {
		if current.Len()+len(line)+1 > chunkSize && current.Len() > 0 {
			chunks = append(chunks, current.String())
			current.Reset()
		}
		if current.Len() > 0 {
			current.WriteByte('\n')
		}
		current.WriteString(line)
	}
	if current.Len() > 0 {
		chunks = append(chunks, current.String())
	}
	return chunks
}

