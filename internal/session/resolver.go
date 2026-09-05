// Package session implements the SessionResolver (DEVELOP.md §3–§5, §27):
//
//   - the client-supplied x-opencode-session header always wins;
//   - with more than one message, Hash(messages[:-1]) is looked up in the
//     SessionStore;
//   - on a miss — or on any store error, or for a first message — a fresh
//     UUID session is generated. A resolution miss must never block or
//     fail the upstream request.
package session

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"ocgate/internal/canon"
	"ocgate/internal/metrics"
	"ocgate/internal/store"
)

// Source explains how the session ID for a request was determined.
type Source string

const (
	SourceClient   Source = "client"   // supplied by the client; never overridden
	SourceResolved Source = "resolved" // recovered from a state-hash mapping
	SourceNew      Source = "new"      // freshly generated UUID
)

// Decision is the resolver's outcome for one request.
type Decision struct {
	SessionID   string
	Source      Source
	HistoryHash string // Hash(messages[:-1]); empty when not applicable
}

// Resolver resolves sessions for incoming chat requests.
type Resolver struct {
	sessions store.SessionStore
	timeout  time.Duration
	m        *metrics.Metrics
	log      *slog.Logger
}

// NewResolver builds a resolver.
func NewResolver(sessions store.SessionStore, timeout time.Duration, m *metrics.Metrics, log *slog.Logger) *Resolver {
	return &Resolver{sessions: sessions, timeout: timeout, m: m, log: log}
}

// Resolve determines the session for a request. ctx is the request context;
// lookup failures fall back to a new session.
func (r *Resolver) Resolve(ctx context.Context, clientHash string, canonMsgs [][]byte, clientSession string) Decision {
	if clientSession != "" {
		r.m.SessionClientSupplied.Inc()
		return Decision{SessionID: clientSession, Source: SourceClient}
	}

	histHash := ""
	if len(canonMsgs) > 1 {
		// the last message is the new user input; the conversation state it
		// continues is messages[:-1] (§6: never scan history prefixes)
		histHash = canon.HistoryHash(canonMsgs[:len(canonMsgs)-1])
		ctx, cancel := context.WithTimeout(ctx, r.timeout)
		id, found, err := r.sessions.GetSession(ctx, clientHash, histHash)
		cancel()
		switch {
		case err != nil:
			r.m.SessionStoreErrorInc("session_get")
			r.log.Warn("session lookup failed; generating new session",
				"error", err, "state_hash", canon.Short(histHash))
		case found:
			r.m.SessionResolveHit.Inc()
			return Decision{SessionID: id, Source: SourceResolved, HistoryHash: histHash}
		default:
			r.m.SessionResolveMiss.Inc()
		}
	}

	return Decision{SessionID: uuid.NewString(), Source: SourceNew, HistoryHash: histHash}
}
