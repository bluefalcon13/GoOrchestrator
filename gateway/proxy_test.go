package gateway

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInterceptAndOffload_FileContentTag_Orchestrator(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gateway_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	gw, err := NewProxyGateway(GatewayConfig{
		TargetBaseURL:  "http://localhost:8000",
		DataDir:        tmpDir,
		MaxInlineBytes: 10, // low threshold to force intercept
	})
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}

	input := "Please analyze this log file:\n<file_content path=\"c:\\Users\\test\\game.log\">\nLine 1\nLine 2\nLine 3\n</file_content>\nTell me what happened."
	output := gw.interceptAndOffload(input, true) // saveToDisk = true

	if strings.Contains(output, "<file_content") {
		t.Errorf("expected <file_content> to be stripped, got: %s", output)
	}
	if !strings.Contains(output, "[Referenced File: game.log") {
		t.Errorf("expected reference placeholder, got: %s", output)
	}

	// Verify file was written to disk for orchestrator target
	savedPath := filepath.Join(tmpDir, "game.log")
	data, err := os.ReadFile(savedPath)
	if err != nil {
		t.Fatalf("expected saved file at %s: %v", savedPath, err)
	}
	if !strings.Contains(string(data), "Line 1") {
		t.Errorf("expected file content to contain 'Line 1', got: %s", string(data))
	}
}

func TestInterceptAndOffload_Internal_NotWrittenToDisk(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gateway_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	gw, err := NewProxyGateway(GatewayConfig{
		TargetBaseURL:  "http://localhost:8000",
		DataDir:        tmpDir,
		MaxInlineBytes: 10, // low threshold to force intercept
	})
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}

	input := "Internal tool result:\n<file_content path=\"Game Build.log\">\nLine 1\nLine 2\n</file_content>"
	output := gw.interceptAndOffload(input, false) // saveToDisk = false

	if strings.Contains(output, "<file_content") {
		t.Errorf("expected <file_content> to be stripped, got: %s", output)
	}
	if !strings.Contains(output, "Content omitted from context window") {
		t.Errorf("expected omission message, got: %s", output)
	}

	// Verify file was NOT written to disk for internal calls
	savedPath := filepath.Join(tmpDir, "Game Build.log")
	if _, err := os.Stat(savedPath); !os.IsNotExist(err) {
		t.Errorf("expected file NOT to be written to disk for internal calls, but it exists at %s", savedPath)
	}
}

func TestInterceptAndOffload_SmallFile_KeptInline(t *testing.T) {
	gw, err := NewProxyGateway(GatewayConfig{
		TargetBaseURL:  "http://localhost:8000",
		DataDir:        "/tmp",
		MaxInlineBytes: 1000, // 1KB threshold
	})
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}

	input := "Reading 50 lines:\n<file_content path=\"test.go\">\npackage main\nfunc main() {}\n</file_content>"
	output := gw.interceptAndOffload(input, false)

	// Small file should NOT be stripped!
	if !strings.Contains(output, "<file_content path=\"test.go\">") {
		t.Errorf("expected small file to be kept inline, but it was modified: %s", output)
	}
}

func TestInterceptAndOffload_LargeRawText(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gateway_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	gw, err := NewProxyGateway(GatewayConfig{
		TargetBaseURL:  "http://localhost:8000",
		DataDir:        tmpDir,
		MaxInlineBytes: 100, // Small threshold for testing
	})
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}

	largeString := strings.Repeat("This is a very long log line that exceeds the threshold.\n", 10)
	output := gw.interceptAndOffload(largeString, true)

	if !strings.Contains(output, "[Large document offloaded to orchestrator storage at") {
		t.Errorf("expected large document offload message, got: %s", output)
	}
}

func TestInterceptAndOffload_ClineJSON_AndUTF16Cleanup(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gateway_cline_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	gw, err := NewProxyGateway(GatewayConfig{
		TargetBaseURL:  "http://localhost:8000",
		DataDir:        tmpDir,
		MaxInlineBytes: 20,
	})
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}

	// Simulate UTF-16LE null-padded Cline JSON file read output
	raw := `[{"query":"errors.log","result":"  1 | <\u00002\u00000\u00002\u00006\u0000>\u0000 [\u0000E\u0000r\u0000r\u0000o\u0000r\u0000]\u0000\n","success":true}]`
	output := gw.interceptAndOffload(raw, true)

	if !strings.Contains(output, "[Referenced File: errors.log") {
		t.Errorf("expected named reference for errors.log, got: %s", output)
	}

	savedPath := filepath.Join(tmpDir, "errors.log")
	data, err := os.ReadFile(savedPath)
	if err != nil {
		t.Fatalf("expected errors.log to be saved at %s: %v", savedPath, err)
	}
	if strings.ContainsRune(string(data), '\x00') {
		t.Errorf("expected UTF-16 null bytes to be stripped, got: %q", string(data))
	}
	if !strings.Contains(string(data), "<2026> [Error]") {
		t.Errorf("expected cleaned log text, got: %q", string(data))
	}
}

func TestShouldSaveToDisk_CaseInsensitiveAndFileTags(t *testing.T) {
	if !shouldSaveToDisk("user", true, "Please upload errors.log to the RAG") {
		t.Errorf("expected uppercase RAG mention to save to disk")
	}
	if !shouldSaveToDisk("user", true, "Look at this:\n<file_content path=\"errors.log\">data</file_content>") {
		t.Errorf("expected explicit <file_content> attachment to save to disk")
	}
	if shouldSaveToDisk("user", true, "Just chatting with no files or keywords") {
		t.Errorf("expected plain chat message not to save to disk")
	}
}

