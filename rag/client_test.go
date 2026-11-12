package rag

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPRAGClient_Embed(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/embeddings" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": []map[string]interface{}{
				{
					"index":     0,
					"embedding": []float32{0.1, 0.2, 0.3},
				},
			},
		})
	}))
	defer mockServer.Close()

	client := NewHTTPRAGClient(mockServer.URL, mockServer.URL, mockServer.URL)
	vecs, err := client.Embed(context.Background(), []string{"test text"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(vecs) != 1 || len(vecs[0]) != 3 {
		t.Fatalf("expected 1 vector with 3 dims, got %v", vecs)
	}
}

func TestHTTPRAGClient_Rerank(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/rerank" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"results": []map[string]interface{}{
				{"index": 0, "relevance_score": 4.5},
				{"index": 1, "relevance_score": -2.1},
			},
		})
	}))
	defer mockServer.Close()

	client := NewHTTPRAGClient(mockServer.URL, mockServer.URL, mockServer.URL)
	scores, err := client.Rerank(context.Background(), "query", []string{"doc1", "doc2"}, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(scores) != 2 || scores[0].RelevanceScore != 4.5 {
		t.Fatalf("unexpected scores: %v", scores)
	}
}

