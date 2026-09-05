// Package sse provides a minimal, allocation-friendly scanner for
// text/event-stream payloads.
//
// It intentionally knows nothing about OpenAI semantics: it only splits a
// raw SSE byte stream into events (the concatenated "data:" lines of each
// event, joined by "\n"). Interpretation happens elsewhere.
package sse

import "bytes"

// Event is one complete SSE event: the joined payload of all its data lines.
type Event struct {
	Data []byte
}

// Scan iterates over complete events in payload in order. fn is called for
// every event that has at least one data line; returning false stops the
// scan. Comment lines (":...") and non-data fields (event:, id:, retry:) are
// ignored. A trailing event without a final blank line is still dispatched.
func Scan(payload []byte, fn func(Event) bool) {
	var data [][]byte
	dispatch := func() bool {
		if len(data) == 0 {
			return true
		}
		ev := Event{Data: bytes.Join(data, []byte("\n"))}
		data = data[:0]
		return fn(ev)
	}

	i := 0
	for i < len(payload) {
		j := bytes.IndexByte(payload[i:], '\n')
		var line []byte
		if j < 0 {
			line = payload[i:]
			i = len(payload)
		} else {
			line = payload[i : i+j]
			i += j + 1
		}
		line = bytes.TrimSuffix(line, []byte("\r"))

		switch {
		case len(line) == 0:
			// blank line: end of event
			if !dispatch() {
				return
			}
		case line[0] == ':':
			// comment / keep-alive
		case bytes.HasPrefix(line, []byte("data:")):
			v := line[len("data:"):]
			v = bytes.TrimPrefix(v, []byte(" "))
			data = append(data, v)
		default:
			// event:, id:, retry: and unknown fields are irrelevant here
		}
	}
	// flush a trailing event that lacks the final blank line
	dispatch()
}
