package openai

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func sseStream(events ...string) string {
	var b strings.Builder
	for _, e := range events {
		if e == "[DONE]" {
			b.WriteString("data: [DONE]\n\n")
			continue
		}
		b.WriteString("data: " + e + "\n\n")
	}
	return b.String()
}

func deltaContent(s string) string {
	return `{"choices":[{"index":0,"delta":{"content":` + quote(s) + `}}]}`
}

func quote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func TestAssistantFromSSETextAccumulation(t *testing.T) {
	payload := sseStream(
		`{"choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}`,
		deltaContent("Hel"),
		deltaContent("lo"),
		"[DONE]",
	)
	canonical, sawDone, err := AssistantFromSSE([]byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	if !sawDone {
		t.Fatal("[DONE] not detected")
	}
	want := `{"content":"Hello","role":"assistant"}`
	if string(canonical) != want {
		t.Fatalf("got %s want %s", canonical, want)
	}
}

func TestAssistantFromSSESplitChunks(t *testing.T) {
	// the stream is split at arbitrary byte boundaries by the network;
	// the collector assembles exact bytes, so parsing must cope with any
	// framing of the same payload
	full := sseStream(deltaContent("Hel"), deltaContent("lo"), "[DONE]")
	for split := 1; split < len(full); split += 7 {
		piece := full[:split] + full[split:]
		_, sawDone, err := AssistantFromSSE([]byte(piece))
		if err != nil || !sawDone {
			t.Fatalf("split %d: err=%v sawDone=%v", split, err, sawDone)
		}
	}
}

func TestAssistantFromSSENoDone(t *testing.T) {
	payload := sseStream(deltaContent("Hi"))
	_, sawDone, err := AssistantFromSSE([]byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	if sawDone {
		t.Fatal("sawDone must be false without [DONE]")
	}
}

func TestAssistantFromSSEEmpty(t *testing.T) {
	payload := sseStream(
		`{"choices":[{"index":0,"delta":{"role":"assistant"}}]}`,
		"[DONE]",
	)
	_, _, err := AssistantFromSSE([]byte(payload))
	if !errors.Is(err, ErrNoAssistantContent) {
		t.Fatalf("got %v", err)
	}
}

func TestAssistantFromSSEContentNull(t *testing.T) {
	payload := sseStream(
		`{"choices":[{"index":0,"delta":{"role":"assistant","content":null},"finish_reason":"tool_calls"}]}`,
		"[DONE]",
	)
	canonical, _, err := AssistantFromSSE([]byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"content":null,"role":"assistant"}`
	if string(canonical) != want {
		t.Fatalf("got %s want %s", canonical, want)
	}
}

func TestAssistantFromSSEToolCalls(t *testing.T) {
	payload := sseStream(
		`{"choices":[{"index":0,"delta":{"role":"assistant","content":null}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\""}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":":\"Paris\"}"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		"[DONE]",
	)
	canonical, sawDone, err := AssistantFromSSE([]byte(payload))
	if err != nil || !sawDone {
		t.Fatalf("err=%v sawDone=%v", err, sawDone)
	}
	want := `{"content":null,"role":"assistant","tool_calls":[{"function":{"arguments":"{\"city\":\"Paris\"}","name":"get_weather"},"id":"call_1","type":"function"}]}`
	if string(canonical) != want {
		t.Fatalf("got  %s\nwant %s", canonical, want)
	}
}

func TestAssistantFromSSEMessageFallback(t *testing.T) {
	// some providers stream full message objects instead of deltas
	payload := sseStream(
		`{"choices":[{"index":0,"message":{"role":"assistant","content":"Hi"}}]}`,
		"[DONE]",
	)
	canonical, _, err := AssistantFromSSE([]byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"content":"Hi","role":"assistant"}`
	if string(canonical) != want {
		t.Fatalf("got %s want %s", canonical, want)
	}
}

func TestAssistantFromMessage(t *testing.T) {
	canonical, err := AssistantFromMessage(json.RawMessage(`{"role":"assistant","content":"Hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"content":"Hello","role":"assistant"}`
	if string(canonical) != want {
		t.Fatalf("got %s want %s", canonical, want)
	}
}

func TestAssistantFromMessageMultipleToolCalls(t *testing.T) {
	canonical, err := AssistantFromMessage(json.RawMessage(
		`{"role":"assistant","content":null,"tool_calls":[`+
			`{"id":"a","type":"function","function":{"name":"f1","arguments":"{}"}},`+
			`{"id":"b","type":"function","function":{"name":"f2","arguments":"{\"x\":1}"}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(canonical), `"name":"f2"`) {
		t.Fatalf("second tool call missing: %s", canonical)
	}
	if !strings.Contains(string(canonical), `"id":"b"`) {
		t.Fatalf("second tool call id missing: %s", canonical)
	}
}

func TestParseChatRequest(t *testing.T) {
	if _, ok := ParseChatRequest([]byte(`{"model":"m","messages":[{"role":"user","content":"x"}]}`)); !ok {
		t.Fatal("valid request rejected")
	}
	if _, ok := ParseChatRequest([]byte(`{"model":"m"}`)); ok {
		t.Fatal("missing messages accepted")
	}
	if _, ok := ParseChatRequest([]byte(`{"messages":"nope"}`)); ok {
		t.Fatal("non-array messages accepted")
	}
	if _, ok := ParseChatRequest([]byte(`not json`)); ok {
		t.Fatal("invalid json accepted")
	}
}

func TestParseCompletionResponse(t *testing.T) {
	msg, err := ParseCompletionResponse([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "ok") {
		t.Fatalf("got %s", msg)
	}
	if _, err := ParseCompletionResponse([]byte(`{"choices":[]}`)); err == nil {
		t.Fatal("expected error for empty choices")
	}
}
