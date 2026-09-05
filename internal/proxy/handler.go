// Package proxy implements the gateway HTTP handler:
//
//   - non-chat requests are proxied transparently;
//   - chat completion requests participate in session logic: the request
//     body is read, canonicalized and resolved, the x-opencode-session
//     header is injected, and the body is restored before forwarding;
//   - streaming (SSE) responses are tee'd: bytes go to the client first
//     and are bypassed into a ResponseCollector (Redis never sits on the
//     real-time path);
//   - JSON responses are buffered with a cap and finalized directly.
package proxy

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"ocgate/internal/canon"
	"ocgate/internal/collector"
	"ocgate/internal/config"
	"ocgate/internal/metrics"
	"ocgate/internal/openai"
	"ocgate/internal/session"
	"ocgate/internal/store"
)

const (
	// finalizeTimeout bounds the whole finalize (page wait + Redis ops +
	// parse), independent from the request context which dies with the
	// client connection.
	finalizeTimeout = 60 * time.Second
	// jsonBufferLimit caps how much of a non-streaming JSON response is
	// buffered for parsing. Larger responses are proxied but not mapped.
	jsonBufferLimit = 64 << 20
)

type ctxKey struct{}

// reqState is the request-local state (never stored globally).
type reqState struct {
	requestID   string
	clientHash  string
	sessionID   string
	source      session.Source
	historyHash string
	canonMsgs   [][]byte
	participate bool
	start       time.Time

	collector *collector.Collector // streaming path
	jsonBuf   *bytes.Buffer        // non-streaming path
	jsonTrunc bool

	eof      atomic.Bool
	finisher sync.Once
}

// Handler is the gateway root handler.
type Handler struct {
	proxy    *httputil.ReverseProxy
	cfg      *config.Config
	resolver *session.Resolver
	sessions store.SessionStore
	pool     *collector.Pool
	m        *metrics.Metrics
	log      *slog.Logger

	finalizeWG sync.WaitGroup
}

// NewHandler builds the handler around a ReverseProxy.
func NewHandler(cfg *config.Config, upstream *url.URL, sessions store.SessionStore,
	pool *collector.Pool, resolver *session.Resolver, m *metrics.Metrics, log *slog.Logger) *Handler {

	h := &Handler{
		cfg:      cfg,
		resolver: resolver,
		sessions: sessions,
		pool:     pool,
		m:        m,
		log:      log,
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ForceAttemptHTTP2 = true

	h.proxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(upstream)
			// Outbound Host follows the upstream target (SetURL already
			// cleared Out.Host).
		},
		ModifyResponse: h.modifyResponse,
		FlushInterval:  -1, // flush immediately: hard requirement for SSE
		Transport:      transport,
		ErrorHandler:   h.errorHandler,
	}
	return h
}

// WaitFinalize blocks until all finalize goroutines finished or timeout.
func (h *Handler) WaitFinalize(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		h.finalizeWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}
}

// ServeHTTP routes chat completion requests into session handling and
// proxies everything else transparently.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/chat/completions") {
		h.m.Requests.WithLabelValues("passthrough").Inc()
		h.proxy.ServeHTTP(w, r)
		return
	}
	h.m.Requests.WithLabelValues("chat").Inc()

	body, tooLarge, err := readBody(r, int64(h.cfg.Limits.MaxRequestBody))
	if err != nil {
		http.Error(w, `{"error":{"message":"failed to read request body","type":"opgate_error","code":"body_read_failed"}}`, http.StatusBadRequest)
		return
	}
	if tooLarge {
		h.bodyTooLarge(w)
		return
	}

	st := &reqState{start: time.Now(), requestID: uuid.NewString()}
	h.prepare(r, body, st)

	// restore the body for the proxy (§32) and keep Content-Length correct
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	r.ContentLength = int64(len(body))
	r.Header.Set("Content-Length", strconv.Itoa(len(body)))

	ctx := context.WithValue(r.Context(), ctxKey{}, st)
	h.proxy.ServeHTTP(w, r.WithContext(ctx))
}

// prepare parses the chat request, resolves the session and injects the
// x-opencode-session header. When the body is not a usable chat request the
// gateway stays a transparent proxy (§53) and never touches the payload.
func (h *Handler) prepare(r *http.Request, body []byte, st *reqState) {
	msgs, ok := openai.ParseChatRequest(body)
	if !ok || len(msgs) == 0 {
		h.log.Debug("non-chat or empty body; transparent proxy",
			"request_id", st.requestID)
		return
	}
	canonMsgs := make([][]byte, 0, len(msgs))
	for _, m := range msgs {
		c, err := canon.Canonicalize(m)
		if err != nil {
			h.log.Debug("message canonicalization failed; transparent proxy",
				"request_id", st.requestID, "error", err)
			return
		}
		canonMsgs = append(canonMsgs, c)
	}

	st.canonMsgs = canonMsgs
	st.clientHash = clientIdentity(r)
	dec := h.resolver.Resolve(r.Context(), st.clientHash, canonMsgs, r.Header.Get("X-Opencode-Session"))
	st.sessionID = dec.SessionID
	st.source = dec.Source
	st.historyHash = dec.HistoryHash
	st.participate = true

	// single header; a client-supplied session is passed through unchanged
	r.Header.Set("X-Opencode-Session", dec.SessionID)

	h.log.Info("chat request",
		"request_id", st.requestID,
		"session_id", st.sessionID,
		"session_source", string(st.source),
		"state_hash", canon.Short(st.historyHash),
		"client_hash", canon.Short(st.clientHash),
		"messages", len(canonMsgs),
	)
}

// modifyResponse decides how the upstream response is treated.
func (h *Handler) modifyResponse(resp *http.Response) error {
	st, _ := resp.Request.Context().Value(ctxKey{}).(*reqState)
	if st == nil || !st.participate {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		// upstream error: no collection, no mapping (§22)
		h.log.Info("upstream error response; skipping collection",
			"request_id", st.requestID, "session_id", st.sessionID, "status", resp.StatusCode)
		return nil
	}
	ct := resp.Header.Get("Content-Type")
	switch {
	case strings.HasPrefix(ct, "text/event-stream"):
		st.collector = h.pool.NewCollector(st.requestID)
		resp.Body = &streamBody{rc: resp.Body, st: st, h: h}
	case strings.Contains(ct, "json"):
		st.jsonBuf = &bytes.Buffer{}
		resp.Body = &jsonBody{rc: resp.Body, st: st, h: h}
	default:
		h.log.Info("unexpected content type; passthrough",
			"request_id", st.requestID, "content_type", ct)
	}
	return nil
}

// streamBody tees upstream SSE bytes into the collector while letting
// ReverseProxy copy them to the client. The collector only appends to a
// small RAM buffer, so client-side latency is unaffected by Redis (§15/§16).
type streamBody struct {
	rc io.ReadCloser
	st *reqState
	h  *Handler
}

func (b *streamBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if n > 0 {
		b.st.collector.Write(p[:n])
	}
	if err == io.EOF {
		b.st.eof.Store(true)
		b.h.finish(b.st)
	}
	return n, err
}

func (b *streamBody) Close() error {
	err := b.rc.Close()
	b.h.finish(b.st)
	return err
}

// jsonBody buffers a non-streaming JSON response up to jsonBufferLimit.
type jsonBody struct {
	rc io.ReadCloser
	st *reqState
	h  *Handler
}

func (b *jsonBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if n > 0 && b.st.jsonBuf != nil {
		if b.st.jsonBuf.Len()+n <= jsonBufferLimit {
			b.st.jsonBuf.Write(p[:n])
		} else {
			// over budget: stop buffering entirely (free the RAM)
			b.st.jsonTrunc = true
			b.st.jsonBuf.Reset()
			b.st.jsonBuf = nil
		}
	}
	if err == io.EOF {
		b.st.eof.Store(true)
		b.h.finish(b.st)
	}
	return n, err
}

func (b *jsonBody) Close() error {
	err := b.rc.Close()
	b.h.finish(b.st)
	return err
}

// finish runs exactly once per request: on EOF it schedules finalize; on
// early body close it schedules a close-time finalize that inspects the
// collected data before deciding completion vs abort.
//
// Why the close path is not a plain abort: real SSE clients (RikkaHub, httpx,
// ...) close the connection immediately after reading "data: [DONE]" — the
// proxy then sees a client disconnect with no transport-level EOF. Whether
// the stream actually completed can only be decided from the data itself,
// which finalizeStreamOnClose does by checking for the [DONE] sentinel in
// the fully assembled payload (never guessed mid-stream, §23).
func (h *Handler) finish(st *reqState) {
	st.finisher.Do(func() {
		if st.eof.Load() {
			h.m.StreamCompleted.Inc()
			h.m.StreamDuration.Observe(time.Since(st.start).Seconds())
			switch {
			case st.collector != nil:
				h.finalizeWG.Add(1)
				go h.finalizeStream(st)
			case st.jsonBuf != nil:
				h.finalizeWG.Add(1)
				go h.finalizeJSON(st)
			}
			return
		}
		if st.collector != nil {
			h.finalizeWG.Add(1)
			go h.finalizeStreamOnClose(st)
			return
		}
		h.m.StreamAborted.Inc()
		h.log.Warn("stream closed before completion; no session mapping",
			"request_id", st.requestID, "session_id", st.sessionID)
	})
}

// finalizeStreamOnClose handles streams that ended without a transport-level
// EOF (typically: the client closed right after [DONE], or the upstream
// closed cleanly without EOF framing). Finalize assembles the full payload
// from Redis pages + RAM tail and checks the [DONE] sentinel there:
//   - [DONE] present → the stream is protocol-complete: map the session;
//   - [DONE] absent / cache failed → genuine abort: no mapping, cleanup.
func (h *Handler) finalizeStreamOnClose(st *reqState) {
	defer h.finalizeWG.Done()
	ctx, cancel := context.WithTimeout(context.Background(), finalizeTimeout)
	defer cancel()

	start := time.Now()
	assistant, stats, err := st.collector.Finalize(ctx)
	h.m.FinalizeDuration.Observe(time.Since(start).Seconds())
	h.m.ResponseBytes.Observe(float64(stats.Bytes))
	h.m.PageCount.Observe(float64(stats.Pages))

	if err != nil {
		h.m.StreamAborted.Inc()
		h.m.FinalizeError.Inc()
		h.log.Warn("stream closed without completion; no session mapping",
			"request_id", st.requestID,
			"session_id", st.sessionID,
			"state_hash", canon.Short(st.historyHash),
			"pages", stats.Pages,
			"bytes", stats.Bytes,
			"error", err,
		)
		return
	}

	// [DONE] was in the payload: protocol-complete despite the early close.
	h.m.StreamCompleted.Inc()
	h.m.StreamDuration.Observe(time.Since(st.start).Seconds())
	h.log.Info("stream completed at connection close ([DONE] in payload)",
		"request_id", st.requestID, "session_id", st.sessionID)
	h.putMapping(ctx, st, assistant)
}

func (h *Handler) finalizeStream(st *reqState) {
	defer h.finalizeWG.Done()
	ctx, cancel := context.WithTimeout(context.Background(), finalizeTimeout)
	defer cancel()

	start := time.Now()
	assistant, stats, err := st.collector.Finalize(ctx)
	h.m.FinalizeDuration.Observe(time.Since(start).Seconds())
	h.m.ResponseBytes.Observe(float64(stats.Bytes))
	h.m.PageCount.Observe(float64(stats.Pages))
	if err != nil {
		h.m.FinalizeError.Inc()
		h.log.Warn("finalize failed; session mapping not updated",
			"request_id", st.requestID,
			"session_id", st.sessionID,
			"state_hash", canon.Short(st.historyHash),
			"pages", stats.Pages,
			"bytes", stats.Bytes,
			"error", err,
		)
		return
	}
	h.putMapping(ctx, st, assistant)
}

func (h *Handler) finalizeJSON(st *reqState) {
	defer h.finalizeWG.Done()
	if st.jsonTrunc || st.jsonBuf == nil {
		h.m.FinalizeError.Inc()
		h.log.Warn("non-stream response too large to parse; session mapping not updated",
			"request_id", st.requestID)
		return
	}
	body := st.jsonBuf.Bytes()
	h.m.ResponseBytes.Observe(float64(len(body)))

	message, err := openai.ParseCompletionResponse(body)
	if err != nil {
		h.m.FinalizeError.Inc()
		h.log.Warn("non-stream response parse failed", "request_id", st.requestID, "error", err)
		return
	}
	assistant, err := openai.AssistantFromMessage(message)
	if err != nil {
		h.m.FinalizeError.Inc()
		h.log.Warn("assistant extraction failed", "request_id", st.requestID, "error", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), finalizeTimeout)
	defer cancel()
	h.putMapping(ctx, st, assistant)
}

// putMapping writes Hash(full conversation state) → sessionID (§7, §50).
// The session ID never participates in the hash.
func (h *Handler) putMapping(ctx context.Context, st *reqState, assistant []byte) {
	full := canon.FullState(st.canonMsgs, assistant)
	stateHash := canon.HashBytes(full)

	ctx, cancel := context.WithTimeout(ctx, time.Duration(h.cfg.Redis.Timeout))
	defer cancel()
	if err := h.sessions.PutSession(ctx, st.clientHash, stateHash, st.sessionID, time.Duration(h.cfg.Session.TTL)); err != nil {
		h.m.SessionMappingError.Inc()
		h.m.FinalizeError.Inc()
		h.log.Warn("session mapping write failed",
			"request_id", st.requestID,
			"session_id", st.sessionID,
			"state_hash", canon.Short(stateHash),
			"error", err,
		)
		return
	}
	h.m.SessionMappingCreated.Inc()
	h.m.FinalizeSuccess.Inc()
	h.log.Info("request completed",
		"request_id", st.requestID,
		"session_id", st.sessionID,
		"state_hash", canon.Short(stateHash),
		"client_hash", canon.Short(st.clientHash),
	)
}

func (h *Handler) abortCollector(st *reqState) {
	defer h.finalizeWG.Done()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(h.cfg.Redis.Timeout))
	defer cancel()
	st.collector.Abort(ctx)
}

func (h *Handler) errorHandler(w http.ResponseWriter, r *http.Request, err error) {
	fields := []any{"error", err}
	if st, _ := r.Context().Value(ctxKey{}).(*reqState); st != nil {
		fields = append(fields, "request_id", st.requestID, "session_id", st.sessionID)
	}
	h.log.Error("upstream request failed", fields...)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadGateway)
	_, _ = io.WriteString(w, `{"error":{"message":"upstream request failed","type":"opgate_error","code":"bad_gateway"}}`)
}

func (h *Handler) bodyTooLarge(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusRequestEntityTooLarge)
	_, _ = io.WriteString(w, `{"error":{"message":"request body too large","type":"opgate_error","code":"body_too_large"}}`)
}

// clientIdentity hashes the request credential for Redis key isolation
// (§9). The raw credential is never stored, logged or forwarded anywhere.
func clientIdentity(r *http.Request) string {
	cred := ""
	switch {
	case r.Header.Get("Authorization") != "":
		cred = r.Header.Get("Authorization")
	case r.Header.Get("X-Api-Key") != "":
		cred = r.Header.Get("X-Api-Key")
	case r.Header.Get("Api-Key") != "":
		cred = r.Header.Get("Api-Key")
	default:
		cred = "anonymous" // stable anonymous identity; never the IP
	}
	return canon.HashBytes([]byte(cred))
}

func readBody(r *http.Request, max int64) (body []byte, tooLarge bool, err error) {
	b, err := io.ReadAll(io.LimitReader(r.Body, max+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(b)) > max {
		return nil, true, nil
	}
	return b, false, nil
}
