package llm

import (
	"encoding/json"
	"errors"
	"strings"
)

// ExtractJSON returns the first syntactically valid top-level JSON object or
// array embedded in s. It tolerates markdown code fences and surrounding prose,
// which models frequently emit even when asked not to.
func ExtractJSON(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", errors.New("empty response")
	}
	if json.Valid([]byte(s)) && (s[0] == '{' || s[0] == '[') {
		return s, nil
	}
	const maxCandidates = 32
	tried := 0
	for i := 0; i < len(s) && tried < maxCandidates; i++ {
		if s[i] != '{' && s[i] != '[' {
			continue
		}
		tried++
		if end, ok := matchBracket(s, i); ok {
			candidate := s[i : end+1]
			if json.Valid([]byte(candidate)) {
				return candidate, nil
			}
		}
	}
	return "", errors.New("no valid JSON object found in response")
}

// matchBracket finds the index of the bracket closing the one at start,
// respecting JSON string literals and escapes.
func matchBracket(s string, start int) (int, bool) {
	depth := 0
	inString := false
	escaped := false
	for i := start; i < len(s); i++ {
		c := s[i]
		if inString {
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth == 0 {
				return i, true
			}
		}
	}
	return 0, false
}

// DecodeJSON extracts and unmarshals the JSON payload embedded in s into T.
func DecodeJSON[T any](s string) (T, error) {
	var v T
	raw, err := ExtractJSON(s)
	if err != nil {
		return v, err
	}
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return v, err
	}
	return v, nil
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

