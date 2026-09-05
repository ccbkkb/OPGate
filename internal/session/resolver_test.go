package session

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"ocgate/internal/canon"
	"ocgate/internal/metrics"
	"ocgate/internal/store"
)

// fakeSessions is an in-memory SessionStore with fault injection.
type fakeSessions struct {
	mu  sync.Mutex
	m   map[string]string
	err error
}

func newFakeSessions() *fakeSessions { return &fakeSessions{m: make(map[string]string)} }

func (f *fakeSessions) GetSession(_ context.Context, clientHash, stateHash string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return "", false, f.err
	}
	v, ok := f.m[clientHash+"|"+stateHash]
	return v, ok, nil
}

func (f *fakeSessions) PutSession(_ context.Context, clientHash, stateHash, sessionID string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.m[clientHash+"|"+stateHash] = sessionID
	return nil
}

func newResolver(st store.SessionStore, m *metrics.Metrics) *Resolver {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewResolver(st, 500*time.Millisecond, m, log)
}

func canonMsgs(t *testing.T, raws ...string) [][]byte {
	t.Helper()
	out := make([][]byte, 0, len(raws))
	for _, r := range raws {
		c, err := canon.Canonicalize([]byte(r))
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, c)
	}
	return out
}

const (
	U1 = `{"role":"user","content":"U1"}`
	A1 = `{"role":"assistant","content":"A1"}`
	U2 = `{"role":"user","content":"U2"}`
)

// TestClientSuppliedSessionWins covers §3.1.
func TestClientSuppliedSessionWins(t *testing.T) {
	r := newResolver(newFakeSessions(), metrics.New())
	d := r.Resolve(context.Background(), "c", canonMsgs(t, U1, A1, U2), "S99")
	if d.Source != SourceClient || d.SessionID != "S99" {
		t.Fatalf("got %+v", d)
	}
}

// TestFirstMessageGeneratesSession covers §4.1: one message → new session,
// no lookup.
func TestFirstMessageGeneratesSession(t *testing.T) {
	f := newFakeSessions()
	r := newResolver(f, metrics.New())
	d := r.Resolve(context.Background(), "c", canonMsgs(t, U1), "")
	if d.Source != SourceNew || d.HistoryHash != "" {
		t.Fatalf("got %+v", d)
	}
	if len(f.m) != 0 {
		t.Fatal("first message must not look up the store")
	}
}

// TestSecondRequestHits covers §5: Hash(messages[:-1]) → session.
func TestSecondRequestHits(t *testing.T) {
	f := newFakeSessions()
	r := newResolver(f, metrics.New())

	// simulate a completed previous turn: Hash(U1,A1) → S1
	hist := canon.HistoryHash(canonMsgs(t, U1, A1))
	if err := f.PutSession(context.Background(), "c", hist, "S1", time.Hour); err != nil {
		t.Fatal(err)
	}

	d := r.Resolve(context.Background(), "c", canonMsgs(t, U1, A1, U2), "")
	if d.Source != SourceResolved || d.SessionID != "S1" || d.HistoryHash != hist {
		t.Fatalf("got %+v", d)
	}
}

// TestMissGeneratesNewSession covers §5.1: a miss must not block.
func TestMissGeneratesNewSession(t *testing.T) {
	r := newResolver(newFakeSessions(), metrics.New())
	d := r.Resolve(context.Background(), "c", canonMsgs(t, U1, A1, U2), "")
	if d.Source != SourceNew || d.SessionID == "" || d.HistoryHash == "" {
		t.Fatalf("got %+v", d)
	}
}

// TestStoreErrorFallsBackToNewSession covers §42: Redis failure must not
// make the API unavailable.
func TestStoreErrorFallsBackToNewSession(t *testing.T) {
	f := newFakeSessions()
	f.err = errors.New("redis down")
	r := newResolver(f, metrics.New())
	d := r.Resolve(context.Background(), "c", canonMsgs(t, U1, A1, U2), "")
	if d.Source != SourceNew || d.SessionID == "" {
		t.Fatalf("got %+v", d)
	}
}

// TestClientIsolation: different clients do not share mappings (§9).
func TestClientIsolation(t *testing.T) {
	f := newFakeSessions()
	r := newResolver(f, metrics.New())
	hist := canon.HistoryHash(canonMsgs(t, U1, A1))
	_ = f.PutSession(context.Background(), "clientA", hist, "S-A", time.Hour)

	d := r.Resolve(context.Background(), "clientB", canonMsgs(t, U1, A1, U2), "")
	if d.Source != SourceNew {
		t.Fatalf("client B must not see client A's session: %+v", d)
	}
}
