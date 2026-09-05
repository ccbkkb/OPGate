// Package openai contains the minimal OpenAI-compatible data model the
// gateway needs: parsing chat completion requests and reconstructing the
// assistant message from a streamed or non-streamed upstream response.
package openai

import (
	"encoding/json"
	"fmt"
)

// ParseChatRequest extracts the messages array from an OpenAI-compatible
// chat completion request body. ok is false when the body is not a JSON
// object containing a "messages" array; such requests are proxied
// transparently.
func ParseChatRequest(body []byte) (msgs []json.RawMessage, ok bool) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return nil, false
	}
	raw, exists := top["messages"]
	if !exists {
		return nil, false
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, false
	}
	return arr, true
}

// CompletionResponse is the subset of a non-streaming chat completion
// response the gateway needs.
type CompletionResponse struct {
	Choices []struct {
		Message json.RawMessage `json:"message"`
	} `json:"choices"`
}

// ParseCompletionResponse extracts choices[0].message from a full JSON
// response body.
func ParseCompletionResponse(body []byte) (message json.RawMessage, err error) {
	var resp CompletionResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("openai: decode completion response: %w", err)
	}
	if len(resp.Choices) == 0 || len(resp.Choices[0].Message) == 0 {
		return nil, fmt.Errorf("openai: completion response has no choices[0].message")
	}
	return resp.Choices[0].Message, nil
}
