package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"ocgate/internal/canon"
	"ocgate/internal/collector"
	"ocgate/internal/config"
	"ocgate/internal/metrics"
	"ocgate/internal/session"
	"ocgate/internal/store"
)

var uuidRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

const (
	U1  = `{"role":"user","content":"Hi"}`
	A1E = `{"role":"assistant","content":"Hello"}` // client echo of A1
	U2  = `{"role":"user","content":"How are you?"}`
	A2E = `{"role":"assistant","content":"World"}` // client echo of A2
	U3  = `{"role":"user","content":"Branch"}`
)

// ---------------------------------------------------------------- upstream

type testUpstream struct {
	mu                 sync.Mutex
	srv                *httptest.Server
	scripts            [][]string // one SSE chunk script per request
	jsonResp           string
	cut                bool
	echo               bool
	bigChunks, bigSize int
	delay              time.Duration

	sessions   []string
	bodies     []string
	chunkTimes []time.Time
}

func newUpstream(t *testing.T) *testUpstream {
	t.Helper()
	u := &testUpstream{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", u.handle)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"object":"list","data":[]}`)) // e.g. /v1/models
	})
	u.srv = httptest.NewServer(mux)
	t.Cleanup(u.srv.Close)
	return u
}

func (u *testUpstream) push(chunks ...string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.scripts = append(u.scripts, chunks)
}

func (u *testUpstream) setJSON(resp string) { u.mu.Lock(); u.jsonResp = resp; u.mu.Unlock() }
func (u *testUpstream) setCut(v bool)       { u.mu.Lock(); u.cut = v; u.mu.Unlock() }
func (u *testUpstream) setDelay(d time.Duration) {
	u.mu.Lock()
	u.delay = d
	u.mu.Unlock()
}

func (u *testUpstream) recorded() (sessions, bodies []string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.sessions...), append([]string(nil), u.bodies...)
}

func (u *testUpstream) lastChunkTime() time.Time {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.chunkTimes) == 0 {
		return time.Time{}
	}
	return u.chunkTimes[len(u.chunkTimes)-1]
}

func (u *testUpstream) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	u.mu.Lock()
	u.sessions = append(u.sessions, r.Header.Get("X-Opencode-Session"))
	u.bodies = append(u.bodies, string(body))
	var script []string
	if len(u.scripts) > 0 {
		script = u.scripts[0]
		u.scripts = u.scripts[1:]
	} else {
		script = []string{"Hello"}
	}
	bigN, bigSize := u.bigChunks, u.bigSize
	jsonResp, cut, delay, echo := u.jsonResp, u.cut, u.delay, u.echo
	u.mu.Unlock()

	if bigN > 0 {
		// big mode: stream bigChunks SSE events of bigSize bytes each
		w.Header().Set("Content-Type", "text/event-stream")
		f, _ := w.(http.Flusher)
		chunk := strings.Repeat("L", bigSize)
		for i := 0; i < bigN; i++ {
			payload := fmt.Sprintf(`{"choices":[{"index":0,"delta":{"content":%s}}]}`, mustJSONString(chunk))
			_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
			if f != nil {
				f.Flush()
			}
			time.Sleep(2 * time.Millisecond)
		}
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
		if f != nil {
			f.Flush()
		}
		return
	}

	if echo {
		// echo the content of the last user message back in 3 SSE chunks
		content := lastUserContent(body)
		parts := splitInto3(content)
		w.Header().Set("Content-Type", "text/event-stream")
		f, _ := w.(http.Flusher)
		for _, part := range parts {
			payload := fmt.Sprintf(`{"choices":[{"index":0,"delta":{"content":%s}}]}`, mustJSONString(part))
			_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
			if f != nil {
				f.Flush()
			}
		}
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
		if f != nil {
			f.Flush()
		}
		return
	}

	if jsonResp != "" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(jsonResp))
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	f, _ := w.(http.Flusher)
	for i, c := range script {
		payload := fmt.Sprintf(`{"choices":[{"index":0,"delta":{"content":%s}}]}`, mustJSONString(c))
		_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
		if f != nil {
			f.Flush()
		}
		u.mu.Lock()
		u.chunkTimes = append(u.chunkTimes, time.Now())
		u.mu.Unlock()
		if cut && i == 0 {
			// mid-stream abort: no EOF, no [DONE]
			panic(http.ErrAbortHandler)
		}
		if delay > 0 {
			time.Sleep(delay)
		}
	}
	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	if f != nil {
		f.Flush()
	}
}

func mustJSONString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func (u *testUpstream) setEcho(v bool) { u.mu.Lock(); u.echo = v; u.mu.Unlock() }

// lastUserContent extracts the content of the last message for echo mode.
func lastUserContent(body []byte) string {
	var req struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	_ = json.Unmarshal(body, &req)
	if len(req.Messages) == 0 {
		return ""
	}
	return req.Messages[len(req.Messages)-1].Content
}

func splitInto3(s string) []string {
	n := len(s) / 3
	if n == 0 {
		return []string{s}
	}
	return []string{s[:n], s[n : 2*n], s[2*n:]}
}

// ----------------------------------------------------------------- gateway

type gateway struct {
	h  *Handler
	m  *metrics.Metrics
	mr *miniredis.Miniredis
	up *testUpstream
}

func newGateway(t *testing.T, mutate func(*config.Config)) *gateway {
	t.Helper()
	mr := miniredis.RunT(t)
	up := newUpstream(t)

	cfg := config.Default()
	cfg.Upstream.BaseURL = up.srv.URL
	cfg.Redis.Addr = mr.Addr()
	cfg.Redis.Timeout = config.Duration(time.Second)
	cfg.ResponseCache.PageSize = 1 << 20
	if mutate != nil {
		mutate(cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	m := metrics.New()
	rs := store.NewRedisStore(cfg.Redis.Addr, 0, time.Duration(cfg.Redis.Timeout), m)
	t.Cleanup(func() { _ = rs.Close() })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	pool := collector.NewPool(rs, collector.Config{
		PageSize:        int(cfg.ResponseCache.PageSize),
		TTL:             time.Duration(cfg.ResponseCache.TTL),
		OpTimeout:       time.Duration(cfg.Redis.Timeout),
		MaxPendingPages: cfg.ResponseCache.MaxPendingPages,
	}, m, log)
	pool.Start(cfg.ResponseCache.Workers)
	t.Cleanup(pool.Stop)

	resolver := session.NewResolver(rs, time.Duration(cfg.Redis.Timeout), m, log)
	target, err := url.Parse(cfg.Upstream.BaseURL)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(cfg, target, rs, pool, resolver, m, log)
	return &gateway{h: h, m: m, mr: mr, up: up}
}

// ------------------------------------------------------------------ helpers

func chatBody(msgs ...string) string {
	var b strings.Builder
	b.WriteString(`{"model":"test-model","stream":true,"messages":[`)
	for i, m := range msgs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(m)
	}
	b.WriteString(`]}`)
	return b.String()
}

func doChat(h *Handler, body, session string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if session != "" {
		req.Header.Set("X-Opencode-Session", session)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

var testClientHash = canon.HashBytes([]byte("anonymous"))

// stateKey builds the Redis key for a full conversation state.
func stateKey(msgs ...string) string {
	canonMsgs := make([][]byte, 0, len(msgs))
	for _, m := range msgs {
		c, err := canon.Canonicalize([]byte(m))
		if err != nil {
			panic(err)
		}
		canonMsgs = append(canonMsgs, c)
	}
	full := canon.MessageArray(canonMsgs)
	return store.SessionKey(testClientHash, canon.HashBytes(full))
}

func waitFor(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", desc)
}

// ------------------------------------------------------------------- tests

// Test 1: a first request [U1] generates a fresh UUID session and, after a
// successful response, Hash(U1,A1) → S1 is stored.
func TestFirstRequestGeneratesSessionAndMapping(t *testing.T) {
	g := newGateway(t, nil)
	g.up.push("He", "llo") // A1 = "Hello"

	rec := doChat(g.h, chatBody(U1), "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	// the client receives the raw SSE stream: content is spread over events
	body := rec.Body.String()
	for _, frag := range []string{`content":"He"`, `content":"llo"`, "[DONE]"} {
		if !strings.Contains(body, frag) {
			t.Fatalf("client stream missing %s: %s", frag, body)
		}
	}

	sessions, _ := g.up.recorded()
	if len(sessions) != 1 || !uuidRe.MatchString(sessions[0]) {
		t.Fatalf("upstream sessions: %v", sessions)
	}
	s1 := sessions[0]

	key := stateKey(U1, A1E)
	waitFor(t, "session mapping", func() bool { return g.mr.Exists(key) })
	if got, _ := g.mr.Get(key); got != s1 {
		t.Fatalf("mapping = %q, want %q", got, s1)
	}
	// temporary pages must be cleaned up
	waitFor(t, "tmp cleanup", func() bool {
		for _, k := range g.mr.Keys() {
			if strings.Contains(k, ":tmp:") {
				return false
			}
		}
		return true
	})
}

// Tests 2+3: follow-up requests resolve the same session.
func TestFollowUpRequestsReuseSession(t *testing.T) {
	g := newGateway(t, nil)
	g.up.push("He", "llo")
	_ = doChat(g.h, chatBody(U1), "")
	key1 := stateKey(U1, A1E)
	waitFor(t, "mapping 1", func() bool { return g.mr.Exists(key1) })
	s1, _ := g.mr.Get(key1)

	// second request [U1,A1,U2] → S1
	g.up.push("W", "orld")
	_ = doChat(g.h, chatBody(U1, A1E, U2), "")
	sessions, _ := g.up.recorded()
	waitFor(t, "second upstream request", func() bool { return len(sessions) >= 2 })
	if sessions[1] != s1 {
		t.Fatalf("second request used %q, want %q", sessions[1], s1)
	}
	key2 := stateKey(U1, A1E, U2, A2E)
	waitFor(t, "mapping 2", func() bool { return g.mr.Exists(key2) })
	if got, _ := g.mr.Get(key2); got != s1 {
		t.Fatalf("mapping 2 = %q, want %q", got, s1)
	}

	// third request [U1,A1,U2,A2,U3] → still S1
	g.up.push("!")
	_ = doChat(g.h, chatBody(U1, A1E, U2, A2E, U3), "")
	sessions, _ = g.up.recorded()
	waitFor(t, "third upstream request", func() bool { return len(sessions) >= 3 })
	if sessions[2] != s1 {
		t.Fatalf("third request used %q, want %q", sessions[2], s1)
	}
}

// Test 4: branching — [U1,A1,U3] hits Hash(U1,A1) → S1 and the new branch
// mapping is stored next to the old one.
func TestBranching(t *testing.T) {
	g := newGateway(t, nil)
	g.up.push("He", "llo")
	_ = doChat(g.h, chatBody(U1), "")
	key1 := stateKey(U1, A1E)
	waitFor(t, "mapping 1", func() bool { return g.mr.Exists(key1) })
	s1, _ := g.mr.Get(key1)

	g.up.push("W", "orld")
	_ = doChat(g.h, chatBody(U1, A1E, U2), "")
	key2 := stateKey(U1, A1E, U2, A2E)
	waitFor(t, "mapping 2", func() bool { return g.mr.Exists(key2) })

	// user edits history: [U1,A1,U3]
	g.up.push("branch")
	_ = doChat(g.h, chatBody(U1, A1E, U3), "")
	sessions, _ := g.up.recorded()
	waitFor(t, "branched request", func() bool { return len(sessions) >= 3 })
	if sessions[2] != s1 {
		t.Fatalf("branched request used %q, want %q", sessions[2], s1)
	}
	key3 := stateKey(U1, A1E, U3, `{"role":"assistant","content":"branch"}`)
	waitFor(t, "branch mapping", func() bool { return g.mr.Exists(key3) })
	// both branches coexist
	if got, _ := g.mr.Get(key2); got != s1 {
		t.Fatalf("old branch mapping lost: %q", got)
	}
}

// Tests 5+6: identical first messages get independent sessions; identical
// complete histories may share one mapping (documented limitation, §10/§59).
func TestSameFirstMessagesIndependentSessions(t *testing.T) {
	g := newGateway(t, nil)
	g.up.push("He", "llo")
	_ = doChat(g.h, chatBody(U1), "")
	key1 := stateKey(U1, A1E)
	waitFor(t, "mapping A", func() bool { return g.mr.Exists(key1) })
	sA, _ := g.mr.Get(key1)

	g.up.push("He", "llo") // identical response → identical state hash
	_ = doChat(g.h, chatBody(U1), "")
	sessions, _ := g.up.recorded()
	waitFor(t, "second upstream request", func() bool { return len(sessions) >= 2 })
	sB := sessions[1]
	if sA == sB {
		t.Fatal("two independent first requests must get different sessions")
	}

	// the mapping was overwritten by the identical conversation; a follow-up
	// resolves to one of them without any error (Test 6)
	g.up.push("W", "orld")
	_ = doChat(g.h, chatBody(U1, A1E, U2), "")
	sessions, _ = g.up.recorded()
	waitFor(t, "third upstream request", func() bool { return len(sessions) >= 3 })
	if sessions[2] != sA && sessions[2] != sB {
		t.Fatalf("resolved %q, want one of %q/%q", sessions[2], sA, sB)
	}
}

// Test 12: a client-supplied x-opencode-session is passed through
// untouched, and the full-state mapping is recorded for it afterwards.
func TestClientSuppliedSessionPassthrough(t *testing.T) {
	g := newGateway(t, nil)
	g.up.push("He", "llo")

	rec := doChat(g.h, chatBody(U1), "S99")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	sessions, _ := g.up.recorded()
	if len(sessions) != 1 || sessions[0] != "S99" {
		t.Fatalf("upstream got %v, want [S99]", sessions)
	}
	key := stateKey(U1, A1E)
	waitFor(t, "client-supplied mapping", func() bool { return g.mr.Exists(key) })
	if got, _ := g.mr.Get(key); got != "S99" {
		t.Fatalf("mapping = %q, want S99", got)
	}
}

// Test 13: with Redis down the gateway still generates a session and
// forwards the request.
func TestRedisDownStillForwards(t *testing.T) {
	g := newGateway(t, nil)
	g.mr.Close() // Redis unavailable

	g.up.push("He", "llo")
	rec := doChat(g.h, chatBody(U1), "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	sessions, _ := g.up.recorded()
	if len(sessions) != 1 || !uuidRe.MatchString(sessions[0]) {
		t.Fatalf("upstream sessions: %v", sessions)
	}
}

// Test 14: stream=false — no temporary page cache, direct mapping.
func TestNonStreamingJSON(t *testing.T) {
	g := newGateway(t, nil)
	g.up.setJSON(`{"id":"1","choices":[{"index":0,"message":{"role":"assistant","content":"Hello"},"finish_reason":"stop"}]}`)

	rec := doChat(g.h, chatBody(U1), "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	key := stateKey(U1, A1E)
	waitFor(t, "non-stream mapping", func() bool { return g.mr.Exists(key) })

	if n := testutil.ToFloat64(g.m.PageFlushSuccess); n != 0 {
		t.Fatalf("non-streaming must not flush pages, got %d", int(n))
	}
	for _, k := range g.mr.Keys() {
		if strings.Contains(k, ":tmp:") {
			t.Fatalf("temporary key created for non-streaming: %s", k)
		}
	}
}

// Tests 7+9: a long stream spills into multiple Redis pages; Redis write
// count stays far below the SSE event count; RAM tail completes the payload.
func TestLongStreamPages(t *testing.T) {
	g := newGateway(t, func(c *config.Config) {
		c.ResponseCache.PageSize = 256
	})
	// 30 SSE events, each ~70 bytes: ~2.1KB total, way above page size.
	// A small delay keeps events in separate TCP segments so the collector
	// accumulates the tail incrementally instead of receiving everything
	// in a single read.
	g.up.setDelay(8 * time.Millisecond)
	chunks := make([]string, 30)
	for i := range chunks {
		chunks[i] = fmt.Sprintf("%03d-abcdefghij", i)
	}
	g.up.push(chunks...)

	rec := doChat(g.h, chatBody(U1), "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}

	var full strings.Builder
	for _, c := range chunks {
		full.WriteString(c)
	}
	key := stateKey(U1, `{"role":"assistant","content":"`+full.String()+`"}`)
	waitFor(t, "long stream mapping", func() bool { return g.mr.Exists(key) })
	body := rec.Body.String()
	for _, c := range chunks {
		if !strings.Contains(body, c) {
			t.Fatalf("client stream missing chunk %q", c)
		}
	}
	if !strings.Contains(body, "[DONE]") {
		t.Fatal("client stream missing [DONE]")
	}

	flushes := testutil.ToFloat64(g.m.PageFlushSuccess)
	if flushes < 2 {
		t.Fatalf("expected multiple page flushes, got %d", int(flushes))
	}
	if flushes >= float64(len(chunks)) {
		t.Fatalf("page flushes (%d) must stay below SSE event count (%d)", int(flushes), len(chunks))
	}
}

// Test 11: the upstream cuts the stream mid-way — abort, no mapping.
func TestUpstreamCutMidStream(t *testing.T) {
	g := newGateway(t, nil)
	g.up.setCut(true)
	g.up.push("partial")

	rec := doChat(g.h, chatBody(U1), "")
	_ = rec // client sees a truncated stream; that is the upstream's fault

	waitFor(t, "stream abort", func() bool {
		return testutil.ToFloat64(g.m.StreamAborted) >= 1
	})
	for _, k := range g.mr.Keys() {
		if strings.Contains(k, ":session:") {
			t.Fatalf("session mapping created despite abort: %s", k)
		}
	}
}

// SSE chunks must reach the client while the upstream is still streaming
// (§15/§16): the first client write happens before the upstream finishes.
func TestSSEStreamsIncrementally(t *testing.T) {
	g := newGateway(t, nil)
	g.up.setDelay(40 * time.Millisecond)
	g.up.push("a", "b", "c", "d")

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(chatBody(U1)))
	req.Header.Set("Content-Type", "application/json")
	tr := &timeRecorder{ResponseRecorder: httptest.NewRecorder()}
	g.h.ServeHTTP(tr, req)

	if tr.firstWrite.IsZero() {
		t.Fatal("client never received data")
	}
	last := g.up.lastChunkTime()
	if !tr.firstWrite.Before(last) {
		t.Fatalf("first client write (%v) happened after upstream finished (%v)",
			tr.firstWrite, last)
	}
	if !strings.Contains(tr.Body.String(), "[DONE]") {
		t.Fatal("stream incomplete at client")
	}
}

type timeRecorder struct {
	*httptest.ResponseRecorder
	firstWrite time.Time
}

func (t *timeRecorder) Write(p []byte) (int, error) {
	if t.firstWrite.IsZero() {
		t.firstWrite = time.Now()
	}
	return t.ResponseRecorder.Write(p)
}

func (t *timeRecorder) Flush() {}

// Non-chat endpoints are proxied transparently (§53).
func TestTransparentPassthrough(t *testing.T) {
	g := newGateway(t, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	rec := httptest.NewRecorder()
	g.h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "list") {
		t.Fatalf("models passthrough failed: %d %s", rec.Code, rec.Body.String())
	}

	// a chat path request with an unusable body is also transparent
	req = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`not-json`))
	rec = httptest.NewRecorder()
	g.h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("transparent chat proxy failed: %d", rec.Code)
	}
	sessions, bodies := g.up.recorded()
	if len(sessions) != 1 || sessions[0] != "" {
		t.Fatalf("session header must not be injected for unparsable body: %v", sessions)
	}
	if len(bodies) != 1 || bodies[0] != "not-json" {
		t.Fatalf("body was modified: %q", bodies)
	}
}

// Oversized request bodies are rejected with 413 (§54).
func TestRequestBodyTooLarge(t *testing.T) {
	g := newGateway(t, func(c *config.Config) {
		c.Limits.MaxRequestBody = 64
	})
	big := chatBody(U1) + strings.Repeat(" ", 200)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(big))
	rec := httptest.NewRecorder()
	g.h.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413", rec.Code)
	}
}

// §46: several conversations run concurrently with fully independent
// request-local state (no global session/buffer).
func TestConcurrentConversations(t *testing.T) {
	g := newGateway(t, nil)
	g.up.setEcho(true)

	const convs = 4
	var wg sync.WaitGroup
	sessions := make([][]string, convs)
	for c := 0; c < convs; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			u1 := fmt.Sprintf(`{"role":"user","content":"conv%d-hello-unique"}`, c)
			a1 := fmt.Sprintf(`{"role":"assistant","content":"conv%d-hello-unique"}`, c)
			u2 := fmt.Sprintf(`{"role":"user","content":"conv%d-second"}`, c)

			rec := doChat(g.h, chatBody(u1), "")
			if rec.Code != http.StatusOK {
				t.Errorf("conv %d: status %d", c, rec.Code)
				return
			}
			key := stateKey(u1, a1)
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) && !g.mr.Exists(key) {
				time.Sleep(10 * time.Millisecond)
			}
			if !g.mr.Exists(key) {
				t.Errorf("conv %d: mapping missing", c)
				return
			}
			s1, _ := g.mr.Get(key)

			_ = doChat(g.h, chatBody(u1, a1, u2), "")
			deadline = time.Now().Add(3 * time.Second)
			var got []string
			for time.Now().Before(deadline) {
				got = nil
				for _, s := range g.up.sessionsOf(u2) {
					got = append(got, s)
				}
				if len(got) > 0 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			sessions[c] = got
			if len(got) != 1 || got[0] != s1 {
				t.Errorf("conv %d: second request used %v, want [%s]", c, got, s1)
			}
		}(c)
	}
	wg.Wait()
}

// §58 (memory acceptance): a response much larger than the page size spills
// into Redis pages; the gateway does not need to buffer the whole stream
// while relaying it (only finalize assembles the payload for parsing).
func TestLargeResponseSpillsToPages(t *testing.T) {
	g := newGateway(t, func(c *config.Config) {
		c.ResponseCache.PageSize = 4 << 20 // 4MiB
	})
	g.up.setEcho(true)
	g.up.setBigChunks(100, 200<<10) // 100 chunks x 200KiB = ~20MB

	rec := doChat(g.h, chatBody(U1), "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if len(rec.Body.String()) < 100*200<<10 {
		t.Fatalf("client received %d bytes", len(rec.Body.String()))
	}
	if !strings.Contains(rec.Body.String(), "[DONE]") {
		t.Fatal("stream incomplete at client")
	}

	flushes := testutil.ToFloat64(g.m.PageFlushSuccess)
	if flushes < 3 || flushes > 7 {
		t.Fatalf("page flushes = %d, want ~5 (20MB / 4MiB)", int(flushes))
	}
}

func (u *testUpstream) setBigChunks(n, size int) {
	u.mu.Lock()
	u.bigChunks, u.bigSize = n, size
	u.mu.Unlock()
}

func (u *testUpstream) sessionsOf(marker string) []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	var out []string
	for i, b := range u.bodies {
		if strings.Contains(b, marker) {
			out = append(out, u.sessions[i])
		}
	}
	return out
}
