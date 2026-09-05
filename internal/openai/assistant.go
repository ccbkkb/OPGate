package openai

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"ocgate/internal/canon"
	"ocgate/internal/sse"
)

var (
	// ErrComplexContent means the assistant content was an array or another
	// structure the gateway cannot represent as a plain string, so no
	// reliable canonical assistant message can be built.
	ErrComplexContent = errors.New("openai: assistant content is not a string/null")
	// ErrNoAssistantContent means the stream carried no assistant content
	// and no tool calls at all.
	ErrNoAssistantContent = errors.New("openai: no assistant content found")
)

// toolCallState accumulates one streamed tool call.
type toolCallState struct {
	ID        string
	Type      string
	Name      string
	Arguments string
}

// accumulator accumulates one choice's assistant message.
type accumulator struct {
	text     strings.Builder
	textSeen bool
	nullSeen bool
	complex  bool

	tools     map[int]*toolCallState
	toolOrder []int
}

// deltaLike covers both streaming deltas and full messages: they share the
// same fields for everything the gateway cares about.
type deltaLike struct {
	// Content is kept raw so that absent vs null vs string vs array are
	// distinguishable.
	Content          json.RawMessage `json:"content"`
	ToolCalls        []toolCallDelta `json:"tool_calls"`
	ReasoningContent *string         `json:"reasoning_content"`
	Reasoning        *string         `json:"reasoning"`
}

type toolCallDelta struct {
	Index    *int   `json:"index"`
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      *string `json:"name"`
		Arguments *string `json:"arguments"`
	} `json:"function"`
}

type streamChunk struct {
	Choices []struct {
		Index   *int       `json:"index"`
		Delta   *deltaLike `json:"delta"`
		Message *deltaLike `json:"message"`
		Text    *string    `json:"text"`
	} `json:"choices"`
}

func (a *accumulator) tool(idx int) *toolCallState {
	if a.tools == nil {
		a.tools = make(map[int]*toolCallState)
	}
	st, ok := a.tools[idx]
	if !ok {
		st = &toolCallState{}
		a.tools[idx] = st
		a.toolOrder = append(a.toolOrder, idx)
	}
	return st
}

// mergeDelta applies one delta / message object to the accumulator.
func (a *accumulator) mergeDelta(d *deltaLike) {
	if len(d.Content) > 0 {
		trimmed := bytes.TrimSpace(d.Content)
		if bytes.Equal(trimmed, []byte("null")) {
			a.nullSeen = true
		} else {
			var s string
			if err := json.Unmarshal(d.Content, &s); err != nil {
				// e.g. an array of content parts: cannot reconstruct reliably
				a.complex = true
			} else {
				a.text.WriteString(s)
				a.textSeen = true
			}
		}
	}
	for i := range d.ToolCalls {
		tc := &d.ToolCalls[i]
		idx := 0
		if tc.Index != nil {
			idx = *tc.Index
		}
		st := a.tool(idx)
		if tc.ID != "" && st.ID == "" {
			st.ID = tc.ID
		}
		if tc.Type != "" && st.Type == "" {
			st.Type = tc.Type
		}
		if tc.Function.Name != nil && *tc.Function.Name != "" {
			if st.Name == "" {
				st.Name = *tc.Function.Name
			} else if *tc.Function.Name != st.Name {
				// streamed name fragments
				st.Name += *tc.Function.Name
			}
		}
		if tc.Function.Arguments != nil {
			st.Arguments += *tc.Function.Arguments
		}
	}
}

func (a *accumulator) empty() bool {
	return !a.textSeen && !a.nullSeen && !a.complex && len(a.tools) == 0
}

// canonical renders the accumulated assistant message as canonical JSON,
// using the representation an OpenAI-compatible client is expected to echo
// back in follow-up requests: {"content":...,"role":"assistant",...}.
// Matching that echo shape is what makes session resolution hit on the next
// turn.
func (a *accumulator) canonical() ([]byte, error) {
	if a.complex {
		return nil, ErrComplexContent
	}
	if a.empty() {
		return nil, ErrNoAssistantContent
	}
	m := map[string]any{"role": "assistant"}
	switch {
	case a.textSeen:
		m["content"] = a.text.String()
	case a.nullSeen:
		m["content"] = nil
	}
	if len(a.tools) > 0 {
		sort.Ints(a.toolOrder)
		arr := make([]any, 0, len(a.toolOrder))
		for _, idx := range a.toolOrder {
			tc := a.tools[idx]
			fn := map[string]any{"arguments": tc.Arguments}
			if tc.Name != "" {
				fn["name"] = tc.Name
			}
			call := map[string]any{"function": fn}
			if tc.ID != "" {
				call["id"] = tc.ID
			}
			if tc.Type != "" {
				call["type"] = tc.Type
			}
			arr = append(arr, call)
		}
		m["tool_calls"] = arr
	}
	return canon.CanonicalValue(m)
}

// AssistantFromSSE scans a complete raw SSE payload (the exact bytes the
// gateway received from upstream) and reconstructs the assistant message of
// choice #0. sawDone reports whether a "data: [DONE]" sentinel was observed.
func AssistantFromSSE(payload []byte) (canonical []byte, sawDone bool, err error) {
	var root accumulator
	sse.Scan(payload, func(ev sse.Event) bool {
		data := bytes.TrimSpace(ev.Data)
		if bytes.Equal(data, []byte("[DONE]")) {
			sawDone = true
			return false
		}
		var ch streamChunk
		if err := json.Unmarshal(data, &ch); err != nil {
			return true // skip malformed / non-chat events
		}
		for i := range ch.Choices {
			c := &ch.Choices[i]
			idx := 0
			if c.Index != nil {
				idx = *c.Index
			}
			if idx != 0 {
				continue // the gateway tracks the primary choice
			}
			if c.Delta != nil {
				root.mergeDelta(c.Delta)
			}
			if c.Message != nil {
				root.mergeDelta(c.Message)
			}
			if c.Text != nil && *c.Text != "" {
				root.text.WriteString(*c.Text)
				root.textSeen = true
			}
		}
		return true
	})
	canonical, err = root.canonical()
	if err != nil {
		return nil, sawDone, err
	}
	return canonical, sawDone, nil
}

// AssistantFromMessage builds the canonical assistant message from a full
// (non-streaming) choices[0].message JSON object. Tool calls are indexed by
// their position because non-streaming tool_calls carry no index field.
func AssistantFromMessage(raw json.RawMessage) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("openai: decode message: %w", err)
	}
	a := &accumulator{}
	if c, ok := m["content"]; ok {
		d := deltaLike{Content: c}
		a.mergeDelta(&d)
	}
	if tcRaw, ok := m["tool_calls"]; ok {
		var arr []toolCallDelta
		if err := json.Unmarshal(tcRaw, &arr); err != nil {
			return nil, fmt.Errorf("openai: decode tool_calls: %w", err)
		}
		for i := range arr {
			tc := &arr[i]
			st := a.tool(i)
			if tc.ID != "" {
				st.ID = tc.ID
			}
			if tc.Type != "" {
				st.Type = tc.Type
			}
			if tc.Function.Name != nil {
				st.Name = *tc.Function.Name
			}
			if tc.Function.Arguments != nil {
				st.Arguments = *tc.Function.Arguments
			}
		}
	}
	return a.canonical()
}
