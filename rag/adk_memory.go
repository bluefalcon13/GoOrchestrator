package rag

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/adk/v2/memory"
	"google.golang.org/adk/v2/session"
	"google.golang.org/genai"
)

// ADKMemoryService adapts rag.Client (Qdrant + Nomic Embed + BGE Reranker)
// to google.golang.org/adk/v2/memory.Service so ADK agents and adkrest
// sessions can natively persist and recall long-term memory.
type ADKMemoryService struct {
	client     Client
	collection string
	topK       int
}

// NewADKMemoryService creates a memory.Service backed by the local RAG cluster.
func NewADKMemoryService(client Client, collection string) memory.Service {
	if strings.TrimSpace(collection) == "" {
		collection = "default_corpus"
	}
	return &ADKMemoryService{
		client:     client,
		collection: collection,
		topK:       5,
	}
}

// AddSessionToMemory extracts text events from an ADK session and indexes them into Qdrant.
func (m *ADKMemoryService) AddSessionToMemory(ctx context.Context, s session.Session) error {
	if m.client == nil || s == nil {
		return nil
	}
	var sb strings.Builder
	for ev := range s.Events().All() {
		if ev == nil || ev.Content == nil {
			continue
		}
		for _, p := range ev.Content.Parts {
			if p != nil && p.Text != "" && !p.Thought {
				fmt.Fprintf(&sb, "[%s] %s\n", ev.Author, p.Text)
			}
		}
	}
	text := strings.TrimSpace(sb.String())
	if text == "" {
		return nil
	}
	source := fmt.Sprintf("session:%s/%s/%s", s.AppName(), s.UserID(), s.ID())
	_, err := m.client.IndexText(ctx, m.collection, text, source, 1000)
	return err
}

// SearchMemory queries Qdrant with cross-encoder reranking and returns ADK memory entries.
func (m *ADKMemoryService) SearchMemory(ctx context.Context, req *memory.SearchRequest) (*memory.SearchResponse, error) {
	if m.client == nil || req == nil || strings.TrimSpace(req.Query) == "" {
		return &memory.SearchResponse{}, nil
	}
	results, err := m.client.SearchAndRerank(ctx, m.collection, req.Query, m.topK*3, m.topK)
	if err != nil {
		return nil, err
	}
	entries := make([]memory.Entry, 0, len(results))
	now := time.Now()
	for _, r := range results {
		entries = append(entries, memory.Entry{
			ID:        r.ID,
			Content:   genai.NewContentFromText(r.Text, genai.RoleModel),
			Author:    r.Source,
			Timestamp: now,
			CustomMetadata: map[string]any{
				"score":       r.Score,
				"source":      r.Source,
				"chunk_index": r.ChunkIndex,
			},
		})
	}
	return &memory.SearchResponse{Memories: entries}, nil
}
