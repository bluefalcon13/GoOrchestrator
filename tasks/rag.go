package tasks

import (
	"context"
	"encoding/json"
	"fmt"

	"go-orchestrator/rag"
)

// RAGIndexParams holds parameters for the rag_index task.
type RAGIndexParams struct {
	Document   string `json:"document"`
	Collection string `json:"collection"`
	Source     string `json:"source,omitempty"`
	ChunkSize  int    `json:"chunk_size,omitempty"`
}

// RAGIndexHandler indexes raw text/documents into Qdrant.
type RAGIndexHandler struct {
	ragClient rag.Client
}

// NewRAGIndexHandler creates a new RAGIndexHandler.
func NewRAGIndexHandler(ragClient rag.Client) *RAGIndexHandler {
	return &RAGIndexHandler{ragClient: ragClient}
}

func (h *RAGIndexHandler) Description() string {
	return "Indexes unstructured documents, logs, or source code into the vector database using dense embeddings."
}

func (h *RAGIndexHandler) Execute(ctx context.Context, input TaskInput) (TaskOutput, error) {
	if h.ragClient == nil {
		return TaskOutput{}, fmt.Errorf("RAG client is not configured")
	}

	var params RAGIndexParams
	if len(input.Params) > 0 && string(input.Params) != "null" {
		_ = json.Unmarshal(input.Params, &params)
	}

	doc := params.Document
	if doc == "" {
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

	collection := params.Collection
	if collection == "" {
		collection = "default_corpus"
	}

	source := params.Source
	if source == "" {
		source = input.Title
	}

	chunkSize := params.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 2000
	}

	chunksIndexed, err := h.ragClient.IndexText(ctx, collection, doc, source, chunkSize)
	if err != nil {
		return TaskOutput{}, fmt.Errorf("failed to index text: %w", err)
	}

	res := map[string]interface{}{
		"status":         "indexed",
		"collection":     collection,
		"source":         source,
		"chunks_indexed": chunksIndexed,
	}
	outBytes, _ := json.MarshalIndent(res, "", "  ")

	return TaskOutput{
		Content: string(outBytes),
	}, nil
}

// RAGSearchParams holds parameters for the rag_search task.
type RAGSearchParams struct {
	Query          string `json:"query"`
	Collection     string `json:"collection"`
	TopK           int    `json:"top_k,omitempty"`
	CandidateLimit int    `json:"candidate_limit,omitempty"`
}

// RAGSearchHandler performs vector retrieval + cross-encoder reranking.
type RAGSearchHandler struct {
	ragClient rag.Client
}

// NewRAGSearchHandler creates a new RAGSearchHandler.
func NewRAGSearchHandler(ragClient rag.Client) *RAGSearchHandler {
	return &RAGSearchHandler{ragClient: ragClient}
}

func (h *RAGSearchHandler) Description() string {
	return "Performs dense semantic vector search and cross-encoder reranking over indexed documents to retrieve top relevant passages."
}

func (h *RAGSearchHandler) Execute(ctx context.Context, input TaskInput) (TaskOutput, error) {
	if h.ragClient == nil {
		return TaskOutput{}, fmt.Errorf("RAG client is not configured")
	}

	var params RAGSearchParams
	if len(input.Params) > 0 && string(input.Params) != "null" {
		_ = json.Unmarshal(input.Params, &params)
	}

	query := params.Query
	if query == "" {
		query = input.Description
	}

	collection := params.Collection
	if collection == "" {
		collection = "default_corpus"
	}

	topK := params.TopK
	if topK <= 0 {
		topK = 5
	}

	candidateLimit := params.CandidateLimit
	if candidateLimit <= 0 {
		candidateLimit = 25
	}

	results, err := h.ragClient.SearchAndRerank(ctx, collection, query, candidateLimit, topK)
	if err != nil {
		return TaskOutput{}, fmt.Errorf("failed to search and rerank: %w", err)
	}

	type MatchItem struct {
		Text       string  `json:"text"`
		Score      float64 `json:"score"`
		Source     string  `json:"source,omitempty"`
		ChunkIndex int     `json:"chunk_index"`
	}

	matches := make([]MatchItem, len(results))
	for i, r := range results {
		matches[i] = MatchItem{
			Text:       r.Text,
			Score:      r.Score,
			Source:     r.Source,
			ChunkIndex: r.ChunkIndex,
		}
	}

	res := map[string]interface{}{
		"query":         query,
		"collection":    collection,
		"matches_count": len(matches),
		"matches":       matches,
	}
	outBytes, _ := json.MarshalIndent(res, "", "  ")

	return TaskOutput{
		Content: string(outBytes),
	}, nil
}

