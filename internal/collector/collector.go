// Package collector implements the streaming Response Collector:
//
//	SSE bytes → small RAM page buffer → (PAGE_SIZE reached) →
//	async Redis page write (fixed worker pool, bounded queue)
//
// Design invariants (DEVELOP.md §13–§26):
//
//   - Redis is never on the client's real-time SSE path: Write() only
//     appends to a small in-memory buffer and never blocks on Redis.
//   - Redis writes happen via a fixed worker pool consuming a bounded
//     queue; if the queue is full (Redis too slow), the collector marks
//     itself failed and discards further data instead of growing RAM.
//   - Finalize waits for all in-flight page flushes, then assembles
//     Redis pages + RAM tail, extracts the assistant message and deletes
//     the temporary data. It only succeeds for a complete stream.
//   - A failed finalize must never produce a state-hash → session mapping.
package collector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"ocgate/internal/metrics"
	"ocgate/internal/openai"
	"ocgate/internal/store"
)

var (
	// ErrCacheFailed means the temporary cache could not keep up (Redis
	// write error or queue saturation). The stream itself is unaffected.
	ErrCacheFailed = errors.New("collector: response cache failed")
	// ErrStreamIncomplete means the stream ended without a [DONE] sentinel.
	ErrStreamIncomplete = errors.New("collector: stream incomplete (no [DONE])")
	// ErrNoAssistantMessage means the stream was complete but carried no
	// assistant content that could be mapped.
	ErrNoAssistantMessage = errors.New("collector: no assistant message found")
)

// Config bundles the pool configuration.
type Config struct {
	PageSize        int           // RAM page size in bytes
	TTL             time.Duration // TTL of temporary Redis entries
	OpTimeout       time.Duration // per-Redis-operation timeout
	MaxPendingPages int           // bounded page queue capacity
}

// Pool owns the page flush workers and the bounded page queue. One pool is
// shared by all requests; a Collector is created per request.
type Pool struct {
	store store.TempPageStore
	cfg   Config
	m     *metrics.Metrics
	log   *slog.Logger

	queue chan pageJob

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type pageJob struct {
	requestID string
	seq       int
	data      []byte
	owner     *Collector
}

// NewPool builds a pool; call Start to spawn the workers.
func NewPool(st store.TempPageStore, cfg Config, m *metrics.Metrics, log *slog.Logger) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{
		store:  st,
		cfg:    cfg,
		m:      m,
		log:    log,
		queue:  make(chan pageJob, cfg.MaxPendingPages),
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start spawns the fixed number of page flush workers.
func (p *Pool) Start(workers int) {
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case job := <-p.queue:
			ctx, cancel := context.WithTimeout(context.Background(), p.cfg.OpTimeout)
			err := p.store.PutPage(ctx, job.requestID, job.seq, job.data, p.cfg.TTL)
			cancel()
			if err != nil {
				p.m.PageFlushError.Inc()
				p.log.Warn("page flush failed",
					"request_id", job.requestID, "seq", job.seq, "error", err)
				job.owner.fail()
			} else {
				p.m.PageFlushSuccess.Inc()
			}
			job.owner.inflight.Done()
		}
	}
}

// Drain waits up to timeout for the queue to be picked up by workers.
// Used during graceful shutdown while the workers are still running.
func (p *Pool) Drain(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(p.queue) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Stop cancels the workers and waits for them to exit.
func (p *Pool) Stop() {
	p.cancel()
	p.wg.Wait()
}

// Stats describes a finalized response.
type Stats struct {
	Pages int // number of pages flushed to Redis
	Bytes int // total assembled payload size
}

// Collector accumulates the raw SSE payload of one request. All Write calls
// come from the single goroutine that copies the upstream response, so no
// mutex is needed for the buffer (single-owner design).
type Collector struct {
	pool      *Pool
	requestID string
	pageSize  int
	tail      []byte // current RAM page buffer
	seq       int    // pages handed to the queue so far

	inflight sync.WaitGroup // pages enqueued but not yet written
	failed   atomic.Bool

	// incremental "data: [DONE]" detection (see Write)
	onDone  func()      // called once, from the copy goroutine
	scanBuf []byte      // trailing incomplete line carried across writes
	sawDone atomic.Bool
	sealed  atomic.Bool // set once finalized early on [DONE]
}

// NewCollector creates a request-local collector. onDone, if set, is called
// exactly once from the copy goroutine the moment a complete "data: [DONE]"
// line has been written — the earliest point where finalize can run without
// racing the client's next request.
func (p *Pool) NewCollector(requestID string, onDone ...func()) *Collector {
	c := &Collector{
		pool:      p,
		requestID: requestID,
		pageSize:  p.cfg.PageSize,
		tail:      make([]byte, 0, p.cfg.PageSize),
	}
	if len(onDone) > 0 {
		c.onDone = onDone[0]
	}
	return c
}

// RequestID returns the collector's request ID.
func (c *Collector) RequestID() string { return c.requestID }

// Write appends raw upstream response bytes. It never blocks on Redis and
// never returns an error; cache failures are recorded internally and
// surface at Finalize.
func (c *Collector) Write(p []byte) {
	if c.sealed.Load() || c.failed.Load() {
		return // finalized early, or resource protection
	}
	// data first: the [DONE]-time finalize must see this chunk
	c.tail = append(c.tail, p...)
	if len(c.tail) >= c.pageSize {
		c.submit(c.tail)
		c.tail = make([]byte, 0, c.pageSize)
	}
	c.watchDone(p)
	if c.sealed.Load() {
		// finalize already ran with the full payload; drop the tail
		// (bytes after [DONE] carry no information we need)
		c.tail = nil
	}
}

// watchDone incrementally scans written bytes for a complete SSE data line
// equal to "[DONE]". Only fully terminated lines count, so a message whose
// content merely mentions "[DONE]" can never trigger it (the content travels
// inside a JSON data line that differs as a whole).
func (c *Collector) watchDone(p []byte) {
	if c.onDone == nil || c.sawDone.Load() {
		return
	}
	c.scanBuf = append(c.scanBuf, p...)
	for {
		i := bytes.IndexByte(c.scanBuf, '\n')
		if i < 0 {
			break
		}
		line := bytes.TrimSuffix(c.scanBuf[:i], []byte("\r"))
		c.scanBuf = c.scanBuf[i+1:]
		if bytes.Equal(bytes.TrimSpace(line), []byte("data: [DONE]")) {
			c.sawDone.Store(true)
			c.sealed.Store(true)
			c.onDone()
			return
		}
	}
	// a single unterminated line longer than the cap cannot be the short
	// [DONE] sentinel; drop it to keep the scan buffer bounded
	if len(c.scanBuf) > 1<<20 {
		c.scanBuf = c.scanBuf[:0]
	}
}

// Done reports whether the [DONE] sentinel has been seen.
func (c *Collector) Done() bool { return c.sawDone.Load() }

// submit hands a full page to the queue. Called from the copy goroutine.
func (c *Collector) submit(page []byte) {
	c.seq++
	c.inflight.Add(1)
	select {
	case c.pool.queue <- pageJob{requestID: c.requestID, seq: c.seq, data: page, owner: c}:
	default:
		// bounded queue full: Redis cannot keep up. Protect resources,
		// mark failed; Finalize will refuse to build a mapping.
		c.inflight.Done()
		c.fail()
	}
}

func (c *Collector) fail() {
	if c.failed.CompareAndSwap(false, true) {
		c.pool.log.Warn("response cache failed; continuing without cache",
			"request_id", c.requestID)
	}
}

// Failed reports whether the cache failed for this request.
func (c *Collector) Failed() bool { return c.failed.Load() }

// Finalize is only called on normal stream completion. It:
//
//  1. seals the RAM tail (no further writes may happen),
//  2. waits for all in-flight page flushes (last-page race, §20),
//  3. reads the completed pages from Redis and sorts them by sequence,
//  4. assembles Redis pages + RAM tail (the last page is never forced to
//     Redis, §21),
//  5. deletes the temporary entry,
//  6. extracts the assistant message from the complete raw SSE payload.
//
// It returns an error (and the caller must not write any mapping) when the
// cache failed, the stream is incomplete, or no assistant message can be
// reconstructed.
func (c *Collector) Finalize(ctx context.Context) (assistant []byte, stats Stats, err error) {
	stats.Pages = c.seq
	if c.failed.Load() {
		return nil, stats, ErrCacheFailed
	}

	c.inflight.Wait() // wait for pending page flushes

	pages, gerr := c.pool.store.GetPages(ctx, c.requestID)
	if gerr != nil {
		return nil, stats, fmt.Errorf("collector: read temp pages: %w", gerr)
	}

	total := len(c.tail)
	for _, b := range pages {
		total += len(b)
	}
	payload := make([]byte, 0, total)
	seqs := make([]int, 0, len(pages))
	for seq := range pages {
		seqs = append(seqs, seq)
	}
	sort.Ints(seqs)
	for _, seq := range seqs {
		payload = append(payload, pages[seq]...)
	}
	payload = append(payload, c.tail...)
	stats.Bytes = len(payload)

	// temp data is no longer needed; cleanup is best effort
	if derr := c.pool.store.DeletePages(ctx, c.requestID); derr != nil {
		c.pool.log.Warn("delete temp pages failed", "request_id", c.requestID, "error", derr)
	}

	asst, sawDone, perr := openai.AssistantFromSSE(payload)
	if perr != nil {
		if errors.Is(perr, openai.ErrNoAssistantContent) {
			return nil, stats, ErrNoAssistantMessage
		}
		return nil, stats, fmt.Errorf("collector: %w", perr)
	}
	if !sawDone {
		return nil, stats, ErrStreamIncomplete
	}
	if asst == nil {
		return nil, stats, ErrNoAssistantMessage
	}
	return asst, stats, nil
}

// Abort cleans up temporary data after an abnormal stream end. Best effort.
func (c *Collector) Abort(ctx context.Context) {
	c.fail()
	if err := c.pool.store.DeletePages(ctx, c.requestID); err != nil {
		c.pool.log.Warn("delete temp pages after abort failed",
			"request_id", c.requestID, "error", err)
	}
}
