package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// GatewayConfig tunes the proxy's behavior.
type GatewayConfig struct {
	TargetBaseURL  string // e.g. "http://code-assistant:8000"
	DataDir        string // e.g. "/data"
	MaxInlineBytes int    // e.g. 16 * 1024 (16KB threshold)
}

// ProxyGateway intercepts OpenAI-compatible chat completion requests,
// strips large files or payloads, saves them to disk/artifacts, and forwards
// a lean reference prompt to vLLM to completely prevent context window overflow.
type ProxyGateway struct {
	cfg          GatewayConfig
	targetURL    *url.URL
	reverseProxy *httputil.ReverseProxy

	// Tag patterns used by Cline, Continue, and VS Code extensions
	fileContentRegex *regexp.Regexp
	fileTagRegex     *regexp.Regexp
}

// NewProxyGateway creates and initializes the gateway proxy.
func NewProxyGateway(cfg GatewayConfig) (*ProxyGateway, error) {
	if cfg.DataDir == "" {
		cfg.DataDir = "/data"
	}
	if cfg.MaxInlineBytes <= 0 {
		cfg.MaxInlineBytes = 16 * 1024 // 16KB default
	}

	targetURL, err := url.Parse(strings.TrimRight(cfg.TargetBaseURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("invalid target base URL %q: %w", cfg.TargetBaseURL, err)
	}

	_ = os.MkdirAll(cfg.DataDir, 0755)

	return &ProxyGateway{
		cfg:              cfg,
		targetURL:        targetURL,
		reverseProxy:     httputil.NewSingleHostReverseProxy(targetURL),
		fileContentRegex: regexp.MustCompile(`(?s)<file_content\s+path="([^"]+)">([\s\S]*?)</file_content>`),
		fileTagRegex:     regexp.MustCompile(`(?s)<file\s+path="([^"]+)">([\s\S]*?)</file>`),
	}, nil
}

// RegisterRoutes registers the /v1/chat/completions and /v1/models endpoints on the mux.
func (p *ProxyGateway) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/chat/completions", p.handleChatCompletions)
	mux.HandleFunc("POST /chat/completions", p.handleChatCompletions)
	mux.HandleFunc("GET /v1/models", p.handleProxyGeneric)
	mux.HandleFunc("GET /models", p.handleProxyGeneric)
}

func (p *ProxyGateway) handleProxyGeneric(w http.ResponseWriter, r *http.Request) {
	r.Host = p.targetURL.Host
	p.reverseProxy.ServeHTTP(w, r)
}

// handleChatCompletions intercepts incoming requests, strips any massive inline file contents,
// offloads them to /data on the host disk, and passes the 100% lossless payload to vLLM via ReverseProxy.
func (p *ProxyGateway) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		http.Error(w, fmt.Sprintf("read request body: %v", err), http.StatusBadRequest)
		return
	}

	// 1. Unpack into raw dynamic object (like JS/TS JSON.parse) to prevent ANY field loss
	var raw map[string]any
	if err := json.Unmarshal(bodyBytes, &raw); err != nil {
		// If it's not standard JSON, pass through untouched
		p.forwardRequest(w, r, bodyBytes)
		return
	}

	modified := false
	if messages, ok := raw["messages"].([]any); ok {
		for _, m := range messages {
			msgMap, ok := m.(map[string]any)
			if !ok {
				continue
			}

			// Determine if this message is targeted at orchestrator or internal
			role, _ := msgMap["role"].(string)
			toolName, _ := msgMap["name"].(string)

			// Internal tool calls (e.g. read_file, list_files) -> DO NOT write to MCP server disk
			saveToDisk := true
			if role == "tool" && toolName != "" && !strings.Contains(toolName, "orchestrator") && !strings.Contains(toolName, "strix") {
				saveToDisk = false
			}

			// Case A: string content
			if strContent, ok := msgMap["content"].(string); ok && len(strContent) >= 100 {
				isOrchTarget := shouldSaveToDisk(role, saveToDisk, strContent)
				newContent := p.interceptAndOffload(strContent, isOrchTarget)
				if newContent != strContent {
					msgMap["content"] = newContent
					modified = true
				}
			} else if arrContent, ok := msgMap["content"].([]any); ok {
				// Case B: multi-part array content [{"type": "text", "text": "..."}]
				for _, part := range arrContent {
					partMap, ok := part.(map[string]any)
					if !ok {
						continue
					}
					if textStr, ok := partMap["text"].(string); ok && len(textStr) >= 100 {
						isOrchTarget := shouldSaveToDisk(role, saveToDisk, textStr)
						newText := p.interceptAndOffload(textStr, isOrchTarget)
						if newText != textStr {
							partMap["text"] = newText
							modified = true
						}
					}
				}
			}
		}
	}

	outPayload := bodyBytes
	if modified {
		reEncoded, err := json.Marshal(raw)
		if err == nil {
			outPayload = reEncoded
			log.Printf("[Gateway Proxy] Intercepted and offloaded large payload. Original: %d bytes -> Slimmed: %d bytes",
				len(bodyBytes), len(outPayload))
		}
	}

	p.forwardRequest(w, r, outPayload)
}

// interceptAndOffload checks for file tags or massive strings and offloads them.
func (p *ProxyGateway) interceptAndOffload(content string, saveToDisk bool) string {
	res := content
	tagMatched := false

	// 1. Intercept <file_content path="...">...</file_content>
	res = p.fileContentRegex.ReplaceAllStringFunc(res, func(match string) string {
		sub := p.fileContentRegex.FindStringSubmatch(match)
		if len(sub) < 3 {
			return match
		}
		path := sub[1]
		body := sub[2]

		// If body is small (<= MaxInlineBytes), keep inline! Do not intercept small reads.
		if len(body) <= p.cfg.MaxInlineBytes {
			return match
		}

		tagMatched = true
		return p.saveAndMakeReference(path, body, saveToDisk)
	})

	// 2. Intercept <file path="...">...</file>
	res = p.fileTagRegex.ReplaceAllStringFunc(res, func(match string) string {
		sub := p.fileTagRegex.FindStringSubmatch(match)
		if len(sub) < 3 {
			return match
		}
		path := sub[1]
		body := sub[2]

		if len(body) <= p.cfg.MaxInlineBytes {
			return match
		}

		tagMatched = true
		return p.saveAndMakeReference(path, body, saveToDisk)
	})

	// 3. Intercept Cline JSON tool output arrays: [{"query": "errors.log", "result": "..."}]
	if !tagMatched && len(res) > p.cfg.MaxInlineBytes {
		if replaced, ok := p.tryInterceptClineJSON(res, saveToDisk); ok {
			return replaced
		}
	}

	// 4. Fallback: if no file tags were matched and raw content is still a massive block (> MaxInlineBytes), offload it
	if !tagMatched && len(res) > p.cfg.MaxInlineBytes {
		mb := float64(len(res)) / (1024 * 1024)
		if saveToDisk {
			cleanedBody := stripUTF16Nulls(res)
			hash := sha256.Sum256([]byte(cleanedBody))
			hashStr := hex.EncodeToString(hash[:8])
			fn := fmt.Sprintf("intercepted_%d_%s.txt", time.Now().Unix(), hashStr)
			targetFile := filepath.Join(p.cfg.DataDir, fn)
			if err := os.WriteFile(targetFile, []byte(cleanedBody), 0644); err == nil {
				log.Printf("[Gateway Proxy] Offloaded large orchestrator payload to %s (%.2f MB)", targetFile, mb)
				return fmt.Sprintf("[Large document offloaded to orchestrator storage at %s (size: %.2f MB) to prevent context overflow. Use strix_orchestrator_rag_search or tools to inspect.]", targetFile, mb)
			}
		} else {
			log.Printf("[Gateway Proxy] Stripped large internal payload (%.2f MB) -> reference passed, NOT written to disk", mb)
			return fmt.Sprintf("[Large content block omitted (size: %.2f MB) to prevent context window overflow. Content remains on client.]", mb)
		}
	}

	return res
}

// shouldSaveToDisk decides whether an intercepted payload should be cached under /data/.
// User-attached files (<file_content> / <file>) or prompts mentioning RAG/ingest/index/upload
// (case-insensitive) are cached so orchestrator MCP tools can immediately stream them.
func shouldSaveToDisk(role string, defaultSave bool, text string) bool {
	if role != "user" {
		return defaultSave
	}
	lower := strings.ToLower(text)
	if strings.Contains(lower, "<file_content") || strings.Contains(lower, "<file ") {
		return true
	}
	for _, kw := range []string{"orchestrator", "ingest", "rag", "index", "upload"} {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

// tryInterceptClineJSON handles Cline's JSON array file-read output:
// [{"query":"errors.log","result":"  1 | ...","success":true}]
func (p *ProxyGateway) tryInterceptClineJSON(raw string, saveToDisk bool) (string, bool) {
	trimmed := strings.TrimSpace(raw)
	if !strings.HasPrefix(trimmed, "[") {
		return "", false
	}
	var entries []struct {
		Query  string `json:"query"`
		Result string `json:"result"`
	}
	if err := json.Unmarshal([]byte(trimmed), &entries); err != nil || len(entries) == 0 {
		return "", false
	}
	var refs []string
	intercepted := false
	for _, e := range entries {
		if e.Query != "" && len(e.Result) > p.cfg.MaxInlineBytes {
			intercepted = true
			refs = append(refs, p.saveAndMakeReference(e.Query, e.Result, saveToDisk))
		}
	}
	if !intercepted {
		return "", false
	}
	return strings.Join(refs, "\n"), true
}

// stripUTF16Nulls cleans UTF-16LE BOM/null bytes (common from PowerShell redirects like `> errors.log`)
// so files cached to /data/ are clean UTF-8 for streaming RAG ingestion.
func stripUTF16Nulls(s string) string {
	if !strings.ContainsRune(s, '\x00') {
		return s
	}
	cleaned := strings.ReplaceAll(s, "\x00", "")
	cleaned = strings.TrimPrefix(cleaned, "\xff\xfe")
	cleaned = strings.TrimPrefix(cleaned, "\xfe\xff")
	cleaned = strings.TrimPrefix(cleaned, "\xef\xbb\xbf")
	cleaned = strings.TrimPrefix(cleaned, "\ufffd\ufffd")
	return cleaned
}

func (p *ProxyGateway) saveAndMakeReference(originalPath, fileBody string, saveToDisk bool) string {
	cleaned := strings.ReplaceAll(originalPath, "\\", "/")
	filename := filepath.Base(cleaned)
	if filename == "." || filename == "/" || filename == "" {
		filename = "uploaded_file.txt"
	}

	cleanedBody := stripUTF16Nulls(fileBody)
	mb := float64(len(cleanedBody)) / (1024 * 1024)

	if saveToDisk {
		targetFile := filepath.Join(p.cfg.DataDir, filename)
		if err := os.WriteFile(targetFile, []byte(cleanedBody), 0644); err != nil {
			log.Printf("[Gateway Proxy] Failed to write intercepted file %s: %v", targetFile, err)
			return fileBody
		}
		log.Printf("[Gateway Proxy] Intercepted orchestrator file %q -> saved to %s (%.2f MB, %d lines)",
			originalPath, targetFile, mb, strings.Count(cleanedBody, "\n"))

		return fmt.Sprintf("[Referenced File: %s | Size: %.2f MB | Original Path: %s]\n"+
			"(Note: Large file offloaded to orchestrator storage at %s for indexing/RAG tools).",
			filename, mb, originalPath, targetFile)
	}

	// Internal client-side file: DO NOT write to MCP server disk!
	log.Printf("[Gateway Proxy] Intercepted internal file %q (%.2f MB, %d lines) -> reference passed, NOT written to server disk",
		originalPath, mb, strings.Count(fileBody, "\n"))

	return fmt.Sprintf("[Referenced File: %s | Size: %.2f MB | Original Path: %s]\n"+
		"(Note: Content omitted from context window to prevent overflow. File remains on local client workspace at %s. Use targeted line reads to inspect).",
		filename, mb, originalPath, originalPath)
}

func (p *ProxyGateway) forwardRequest(w http.ResponseWriter, r *http.Request, payload []byte) {
	// Normalize path to /v1/chat/completions so upstream vLLM routes properly
	if r.URL.Path == "/chat/completions" {
		r.URL.Path = "/v1/chat/completions"
	}
	r.Body = io.NopCloser(bytes.NewReader(payload))
	r.ContentLength = int64(len(payload))
	r.Header.Set("Content-Length", fmt.Sprintf("%d", len(payload)))
	r.Host = p.targetURL.Host

	// ReverseProxy handles transparent SSE streaming, hop-by-hop headers, and flushes
	p.reverseProxy.ServeHTTP(w, r)
}

