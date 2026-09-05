// Package store defines the two business-level storage interfaces of the
// gateway (see DEVELOP.md §28/§29). Redis is the first implementation, but
// business code never touches a Redis client directly.
package store

import (
	"context"
	"time"
)

// SessionStore persists the conversation-state-hash → OpenCode session ID
// mapping. Keys must be versioned and must contain only hashed client
// identity, never raw credentials.
type SessionStore interface {
	GetSession(ctx context.Context, clientHash, stateHash string) (sessionID string, found bool, err error)
	PutSession(ctx context.Context, clientHash, stateHash, sessionID string, ttl time.Duration) error
}

// TempPageStore stores the temporary response pages of in-flight requests.
// Pages are addressed by (requestID, seq); seq starts at 1 and is rendered
// zero-padded ("000001"). Every write refreshes the TTL of the whole entry.
type TempPageStore interface {
	PutPage(ctx context.Context, requestID string, seq int, data []byte, ttl time.Duration) error
	GetPages(ctx context.Context, requestID string) (map[int][]byte, error)
	DeletePages(ctx context.Context, requestID string) error
}
