// Package canon implements the deterministic canonical JSON serialization
// used for conversation state hashing.
//
// Canonical form rules (must stay stable for the lifetime of a key version):
//
//   - UTF-8, compact (no insignificant whitespace)
//   - object keys sorted by byte order; duplicate keys collapse (last wins,
//     as produced by the JSON decoder)
//   - numbers are preserved verbatim in their original lexical form
//     (1, 1.0 and 1e0 hash differently; this is deterministic and simple)
//   - strings are minimally escaped: only quote, backslash and control
//     characters are escaped; every other codepoint is emitted literally
//   - array order is preserved
//   - absent fields and explicit nulls remain distinguishable
package canon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// Canonicalize parses raw JSON and re-serializes it in canonical form.
func Canonicalize(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // preserve the lexical form of numbers
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("canon: decode: %w", err)
	}
	var buf bytes.Buffer
	if err := emit(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// CanonicalValue serializes an arbitrary Go value into canonical form.
// It is used for values the gateway constructs itself (e.g. the
// reconstructed assistant message), never for client-provided JSON.
func CanonicalValue(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("canon: marshal: %w", err)
	}
	return Canonicalize(b)
}

func emit(b *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		if x {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case json.Number:
		b.WriteString(x.String()) // verbatim lexical form
	case string:
		WriteString(b, x)
	case []any:
		b.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := emit(b, e); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys) // byte-order sort of UTF-8 keys
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			WriteString(b, k)
			b.WriteByte(':')
			if err := emit(b, x[k]); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	default:
		return fmt.Errorf("canon: unsupported type %T", v)
	}
	return nil
}

// WriteString appends s as a canonical JSON string value.
func WriteString(b *bytes.Buffer, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		default:
			if r < 0x20 {
				fmt.Fprintf(b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
}
