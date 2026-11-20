package rag

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// MockRAGClient provides an in-memory implementation of Client for unit testing.
type MockRAGClient struct {
	mu          sync.Mutex
	collections map[string][]DocumentChunk
}

// NewMockRAGClient creates a new in-memory RAG client mock.
func NewMockRAGClient() *MockRAGClient {
	return &MockRAGClient{
		collections: make(map[string][]DocumentChunk),
	}
}

func (m *MockRAGClient) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = []float32{0.1, 0.2, 0.3}
	}
	return out, nil
}

func (m *MockRAGClient) Rerank(ctx context.Context, query string, documents []string, topK int) ([]RerankItem, error) {
	var items []RerankItem
	for i := range documents {
		if topK > 0 && len(items) >= topK {
			break
		}
		items = append(items, RerankItem{
			Index:          i,
			RelevanceScore: float64(len(documents) - i),
		})
	}
	return items, nil
}

func (m *MockRAGClient) EnsureCollection(ctx context.Context, collection string, dim int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.collections[collection]; !ok {
		m.collections[collection] = []DocumentChunk{}
	}
	return nil
}

func (m *MockRAGClient) UpsertChunks(ctx context.Context, collection string, chunks []DocumentChunk) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.collections[collection] = append(m.collections[collection], chunks...)
	return nil
}

func (m *MockRAGClient) Search(ctx context.Context, collection string, query string, limit int) ([]SearchResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	chunks, ok := m.collections[collection]
	if !ok {
		return []SearchResult{}, nil
	}
	var res []SearchResult
	for i, c := range chunks {
		if limit > 0 && len(res) >= limit {
			break
		}
		// simple case-insensitive substring match or include
		score := 1.0
		if strings.Contains(strings.ToLower(c.Text), strings.ToLower(query)) {
			score = 5.0
		}
		res = append(res, SearchResult{
			ID:         c.ID,
			Score:      score,
			Text:       c.Text,
			Source:     c.Source,
			ChunkIndex: i,
		})
	}
	return res, nil
}

func (m *MockRAGClient) SearchAndRerank(ctx context.Context, collection string, query string, candidateLimit int, topK int) ([]SearchResult, error) {
	return m.Search(ctx, collection, query, topK)
}

func (m *MockRAGClient) IndexText(ctx context.Context, collection string, text string, source string, chunkSize int) (int, error) {
	_ = m.EnsureCollection(ctx, collection, 768)
	chunk := DocumentChunk{
		ID:         fmt.Sprintf("chunk-%s-0", collection),
		Text:       text,
		Source:     source,
		ChunkIndex: 0,
	}
	_ = m.UpsertChunks(ctx, collection, []DocumentChunk{chunk})
	return 1, nil
}

func (m *MockRAGClient) IndexFile(ctx context.Context, collection string, filePath string, chunkSize int) (int, error) {
	return m.IndexText(ctx, collection, "mock file content from "+filePath, filePath, chunkSize)
}

