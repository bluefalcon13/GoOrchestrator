package rag

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// SearchResult represents a retrieved and reranked passage.
type SearchResult struct {
	ID         string                 `json:"id"`
	Score      float64                `json:"score"`
	Text       string                 `json:"text"`
	Source     string                 `json:"source,omitempty"`
	ChunkIndex int                    `json:"chunk_index"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

// DocumentChunk is a chunk of text prepared for embedding and vector storage.
type DocumentChunk struct {
	ID         string                 `json:"id"`
	Text       string                 `json:"text"`
	Source     string                 `json:"source,omitempty"`
	ChunkIndex int                    `json:"chunk_index"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

// Client defines the contract for interacting with the local RAG cluster
// (llama-embed, llama-rerank, and qdrant).
type Client interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	Rerank(ctx context.Context, query string, documents []string, topK int) ([]RerankItem, error)
	EnsureCollection(ctx context.Context, collection string, dim int) error
	UpsertChunks(ctx context.Context, collection string, chunks []DocumentChunk) error
	Search(ctx context.Context, collection string, query string, limit int) ([]SearchResult, error)
	SearchAndRerank(ctx context.Context, collection string, query string, candidateLimit int, topK int) ([]SearchResult, error)
	IndexText(ctx context.Context, collection string, text string, source string, chunkSize int) (int, error)
}

// RerankItem holds individual score output from llama-rerank.
type RerankItem struct {
	Index          int     `json:"index"`
	RelevanceScore float64 `json:"relevance_score"`
}

// HTTPRAGClient implements Client using standard net/http.
type HTTPRAGClient struct {
	embedURL   string
	rerankURL  string
	qdrantURL  string
	httpClient *http.Client
}

// NewHTTPRAGClient creates a new HTTPRAGClient.
func NewHTTPRAGClient(embedURL, rerankURL, qdrantURL string) *HTTPRAGClient {
	return &HTTPRAGClient{
		embedURL:  strings.TrimRight(embedURL, "/"),
		rerankURL: strings.TrimRight(rerankURL, "/"),
		qdrantURL: strings.TrimRight(qdrantURL, "/"),
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

// Embed calls llama-embed (/v1/embeddings) to produce vector embeddings.
func (c *HTTPRAGClient) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	reqBody, err := json.Marshal(map[string]interface{}{
		"input": texts,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal embed request: %w", err)
	}

	url := c.embedURL + "/v1/embeddings"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("create embed request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute embed request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("embed API returned status %d: %s", resp.StatusCode, string(body))
	}

	var res struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
			Index     int       `json:"index"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("decode embed response: %w", err)
	}

	vectors := make([][]float32, len(texts))
	for _, item := range res.Data {
		if item.Index >= 0 && item.Index < len(vectors) {
			vectors[item.Index] = item.Embedding
		}
	}
	return vectors, nil
}

// Rerank calls llama-rerank (/v1/rerank) to score document relevance against a query.
func (c *HTTPRAGClient) Rerank(ctx context.Context, query string, documents []string, topK int) ([]RerankItem, error) {
	if len(documents) == 0 {
		return nil, nil
	}

	reqBody, err := json.Marshal(map[string]interface{}{
		"query":     query,
		"documents": documents,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal rerank request: %w", err)
	}

	url := c.rerankURL + "/v1/rerank"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("create rerank request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute rerank request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("rerank API returned status %d: %s", resp.StatusCode, string(body))
	}

	var res struct {
		Results []RerankItem `json:"results"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("decode rerank response: %w", err)
	}

	if topK > 0 && len(res.Results) > topK {
		res.Results = res.Results[:topK]
	}
	return res.Results, nil
}

// EnsureCollection verifies the collection exists in Qdrant; creates it if missing.
func (c *HTTPRAGClient) EnsureCollection(ctx context.Context, collection string, dim int) error {
	getURL := fmt.Sprintf("%s/collections/%s", c.qdrantURL, collection)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, getURL, nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil // Exists
		}
	}

	if dim <= 0 {
		dim = 768 // default nomic dimension
	}

	createPayload := map[string]interface{}{
		"vectors": map[string]interface{}{
			"size":     dim,
			"distance": "Cosine",
		},
	}
	payloadBytes, _ := json.Marshal(createPayload)
	putURL := fmt.Sprintf("%s/collections/%s", c.qdrantURL, collection)
	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, putURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return err
	}
	putReq.Header.Set("Content-Type", "application/json")

	putResp, err := c.httpClient.Do(putReq)
	if err != nil {
		return fmt.Errorf("create collection %s: %w", collection, err)
	}
	defer putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK && putResp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(putResp.Body)
		return fmt.Errorf("failed to create collection %s (status %d): %s", collection, putResp.StatusCode, string(b))
	}
	return nil
}

// UpsertChunks embeds chunks and uploads points to Qdrant.
func (c *HTTPRAGClient) UpsertChunks(ctx context.Context, collection string, chunks []DocumentChunk) error {
	if len(chunks) == 0 {
		return nil
	}

	texts := make([]string, len(chunks))
	for i, ch := range chunks {
		// Nomic embed recommendation: prepend search_document:
		texts[i] = "search_document: " + ch.Text
	}

	vectors, err := c.Embed(ctx, texts)
	if err != nil {
		return fmt.Errorf("embed chunks: %w", err)
	}

	dim := len(vectors[0])
	if err := c.EnsureCollection(ctx, collection, dim); err != nil {
		return fmt.Errorf("ensure collection: %w", err)
	}

	type Point struct {
		ID      string                 `json:"id"`
		Vector  []float32              `json:"vector"`
		Payload map[string]interface{} `json:"payload"`
	}

	points := make([]Point, len(chunks))
	for i, ch := range chunks {
		payload := make(map[string]interface{})
		for k, v := range ch.Metadata {
			payload[k] = v
		}
		payload["text"] = ch.Text
		payload["source"] = ch.Source
		payload["chunk_index"] = ch.ChunkIndex

		pointID := ch.ID
		if pointID == "" {
			hash := sha256.Sum256([]byte(fmt.Sprintf("%s_%d_%s", ch.Source, ch.ChunkIndex, ch.Text)))
			pointID = hex.EncodeToString(hash[:16])
		}

		points[i] = Point{
			ID:      pointID,
			Vector:  vectors[i],
			Payload: payload,
		}
	}

	batchPayload := map[string]interface{}{
		"points": points,
	}
	body, _ := json.Marshal(batchPayload)

	url := fmt.Sprintf("%s/collections/%s/points?wait=true", c.qdrantURL, collection)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("qdrant upsert: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("qdrant upsert returned %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

// Search queries Qdrant using vector similarity.
func (c *HTTPRAGClient) Search(ctx context.Context, collection string, query string, limit int) ([]SearchResult, error) {
	if limit <= 0 {
		limit = 10
	}

	// Nomic embed recommendation: prepend search_query:
	vectors, err := c.Embed(ctx, []string{"search_query: " + query})
	if err != nil {
		return nil, fmt.Errorf("embed search query: %w", err)
	}
	if len(vectors) == 0 {
		return nil, fmt.Errorf("no vector returned for query")
	}

	searchReq := map[string]interface{}{
		"vector":       vectors[0],
		"limit":        limit,
		"with_payload": true,
	}
	reqBytes, _ := json.Marshal(searchReq)

	url := fmt.Sprintf("%s/collections/%s/points/search", c.qdrantURL, collection)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("qdrant search: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("qdrant search returned %d: %s", resp.StatusCode, string(b))
	}

	var res struct {
		Result []struct {
			ID      interface{}            `json:"id"`
			Score   float64                `json:"score"`
			Payload map[string]interface{} `json:"payload"`
		} `json:"result"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("decode qdrant search response: %w", err)
	}

	results := make([]SearchResult, 0, len(res.Result))
	for _, item := range res.Result {
		txt, _ := item.Payload["text"].(string)
		src, _ := item.Payload["source"].(string)
		var chunkIdx int
		if idxVal, ok := item.Payload["chunk_index"].(float64); ok {
			chunkIdx = int(idxVal)
		}

		results = append(results, SearchResult{
			ID:         fmt.Sprintf("%v", item.ID),
			Score:      item.Score,
			Text:       txt,
			Source:     src,
			ChunkIndex: chunkIdx,
			Metadata:   item.Payload,
		})
	}
	return results, nil
}

// SearchAndRerank performs dense vector search, then passes candidates through the cross-encoder reranker.
func (c *HTTPRAGClient) SearchAndRerank(ctx context.Context, collection string, query string, candidateLimit int, topK int) ([]SearchResult, error) {
	if candidateLimit <= 0 {
		candidateLimit = 25
	}
	if topK <= 0 {
		topK = 5
	}

	candidates, err := c.Search(ctx, collection, query, candidateLimit)
	if err != nil {
		return nil, fmt.Errorf("vector candidate search: %w", err)
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	docs := make([]string, len(candidates))
	for i, c := range candidates {
		docs[i] = c.Text
	}

	rerankScores, err := c.Rerank(ctx, query, docs, topK)
	if err != nil {
		// Fallback to vector search results if reranker fails
		if len(candidates) > topK {
			return candidates[:topK], nil
		}
		return candidates, nil
	}

	finalResults := make([]SearchResult, 0, len(rerankScores))
	for _, r := range rerankScores {
		if r.Index >= 0 && r.Index < len(candidates) {
			item := candidates[r.Index]
			item.Score = r.RelevanceScore
			finalResults = append(finalResults, item)
		}
	}
	return finalResults, nil
}

// IndexText splits text into sliding chunks and upserts them into Qdrant.
func (c *HTTPRAGClient) IndexText(ctx context.Context, collection string, text string, source string, chunkSize int) (int, error) {
	if chunkSize <= 0 {
		chunkSize = 2000
	}

	lines := strings.Split(text, "\n")
	var chunks []DocumentChunk
	var curChunk strings.Builder
	chunkIndex := 0

	for _, line := range lines {
		if curChunk.Len()+len(line)+1 > chunkSize && curChunk.Len() > 0 {
			chunks = append(chunks, DocumentChunk{
				Text:       curChunk.String(),
				Source:     source,
				ChunkIndex: chunkIndex,
			})
			chunkIndex++
			curChunk.Reset()
		}
		if curChunk.Len() > 0 {
			curChunk.WriteString("\n")
		}
		curChunk.WriteString(line)
	}

	if curChunk.Len() > 0 {
		chunks = append(chunks, DocumentChunk{
			Text:       curChunk.String(),
			Source:     source,
			ChunkIndex: chunkIndex,
		})
	}

	if len(chunks) == 0 {
		return 0, nil
	}

	// Batch upsert in groups of 50
	batchSize := 50
	for i := 0; i < len(chunks); i += batchSize {
		end := i + batchSize
		if end > len(chunks) {
			end = len(chunks)
		}
		if err := c.UpsertChunks(ctx, collection, chunks[i:end]); err != nil {
			return 0, fmt.Errorf("upsert batch %d-%d: %w", i, end, err)
		}
	}
	return len(chunks), nil
}

