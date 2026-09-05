package canon

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
)

// HashBytes returns the hex-encoded SHA-256 of data.
func HashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// MessageArray renders already-canonical messages as a canonical JSON array.
// The conversation state hash is always computed over such an array:
//
//	Conversation State → Canonical JSON → SHA-256 → State Hash
//
// The session ID never participates in the hash.
func MessageArray(msgs [][]byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, m := range msgs {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(m)
	}
	buf.WriteByte(']')
	return buf.Bytes()
}

// HistoryHash computes the state hash of the given canonical messages.
// For session resolution the caller passes messages[:-1] so the hash covers
// exactly the conversation state the new user message continues.
func HistoryHash(msgs [][]byte) string {
	return HashBytes(MessageArray(msgs))
}

// FullState appends the canonical assistant message to the request messages
// and returns the canonical full conversation state array.
func FullState(msgs [][]byte, assistant []byte) []byte {
	all := make([][]byte, 0, len(msgs)+1)
	all = append(all, msgs...)
	all = append(all, assistant)
	return MessageArray(all)
}

// Short returns a short prefix of a hash for logging purposes.
func Short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}
