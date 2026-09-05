package collector

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ocgate/internal/metrics"
	"ocgate/internal/store"
)

// fakeStore is an in-memory TempPageStore with fault injection.
type fakeStore struct {
	mu        sync.Mutex
	pages     map[string]map[int][]byte
	puts      int
	putDelay  time.Duration
	failAfter int // fail the Nth put and beyond (1-based); 0 = never
	deletes   int
}

func newFakeStore() *fakeStore {
	return &fakeStore{pages: make(map[string]map[int][]byte)}
}

func (f *fakeStore) PutPage(_ context.Context, requestID string, seq int, data []byte, _ time.Duration) error {
	f.mu.Lock()
	f.puts++
	puts := f.puts
	fail := f.failAfter > 0 && puts >= f.failAfter
	delay := f.putDelay
	f.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	if fail {
		return errors.New("injected put failure")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	m := f.pages[requestID]
	if m == nil {
		m = make(map[int][]byte)
		f.pages[requestID] = m
	}
	m[seq] = append([]byte(nil), data...)
	return nil
}

func (f *fakeStore) GetPages(_ context.Context, requestID string) (map[int][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[int][]byte, len(f.pages[requestID]))
	for k, v := range f.pages[requestID] {
		out[k] = append([]byte(nil), v...)
	}
	return out, nil
}

func (f *fakeStore) DeletePages(_ context.Context, requestID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.pages, requestID)
	f.deletes++
	return nil
}

func (f *fakeStore) pagecount(requestID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pages[requestID])
}

func newTestPool(t *testing.T, st store.TempPageStore, cfg Config, workers int) *Pool {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	p := NewPool(st, cfg, metrics.New(), log)
	p.Start(workers)
	t.Cleanup(p.Stop)
	return p
}

// ssePayload builds a complete raw SSE stream whose assistant content is
// content repeated n times.
func ssePayload(content string, n int) []byte {
	var b bytes.Buffer
	b.WriteString(`data: {"choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}` + "\n\n")
	b.WriteString(`data: {"choices":[{"index":0,"delta":{"content":"` + strings.Repeat(content, n) + `"}}]}` + "\n\n")
	b.WriteString("data: [DONE]\n\n")
	return b.Bytes()
}

// writeInChunks feeds payload to the collector in chunks of chunk bytes.
func writeInChunks(c *Collector, payload []byte, chunk int) {
	for off := 0; off < len(payload); off += chunk {
		end := off + chunk
		if end > len(payload) {
			end = len(payload)
		}
		c.Write(payload[off:end])
	}
}

// TestPagesAndTailAssembly covers DEVELOP.md §20/§21/§48: pages are stored
// in order, the RAM tail is appended last, and the temporary entry is
// deleted after finalize.
func TestPagesAndTailAssembly(t *testing.T) {
	st := newFakeStore()
	p := newTestPool(t, st, Config{PageSize: 100, TTL: time.Minute, OpTimeout: time.Second, MaxPendingPages: 8}, 2)

	payload := ssePayload("x", 250) // 250 + framing > 400 bytes
	c := p.NewCollector("req-1")
	writeInChunks(c, payload, 100)

	assistant, stats, err := c.Finalize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(assistant, []byte(strings.Repeat("x", 250))) {
		t.Fatalf("assistant content incomplete: %s...", assistant[:40])
	}
	wantPages := len(payload) / 100
	if stats.Pages != wantPages {
		t.Fatalf("stats.Pages = %d, want %d", stats.Pages, wantPages)
	}
	if stats.Bytes != len(payload) {
		t.Fatalf("stats.Bytes = %d, want %d", stats.Bytes, len(payload))
	}
	if got := st.pagecount("req-1"); got != 0 {
		t.Fatalf("temp pages not deleted: %d remain", got)
	}
	if st.deleted() != 1 {
		t.Fatalf("DeletePages called %d times", st.deleted())
	}
}

// deleted returns the number of DeletePages calls.
func (f *fakeStore) deleted() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deletes
}

// TestFinalizeWaitsForPendingFlush covers DEVELOP.md Test 8: a page flush
// still in flight when the stream ends must complete before HGETALL.
func TestFinalizeWaitsForPendingFlush(t *testing.T) {
	st := newFakeStore()
	st.putDelay = 300 * time.Millisecond
	p := newTestPool(t, st, Config{PageSize: 100, TTL: time.Minute, OpTimeout: time.Second, MaxPendingPages: 8}, 2)

	payload := ssePayload("y", 200)
	c := p.NewCollector("req-2")
	writeInChunks(c, payload, 100) // several pages + tail

	start := time.Now()
	assistant, stats, err := c.Finalize(context.Background())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed < 200*time.Millisecond {
		t.Fatalf("finalize did not wait for pending flushes (took %s)", elapsed)
	}
	if !bytes.Contains(assistant, []byte(strings.Repeat("y", 200))) {
		t.Fatal("assembled payload lost data (page race)")
	}
	if st.pagecount("req-2") != 0 {
		t.Fatal("temp pages not deleted")
	}
	_ = stats
}

// TestCacheFailureNeverMaps covers DEVELOP.md Test 10 / §42: a page write
// failure marks the collector failed; the caller gets an error and must not
// write a session mapping.
func TestCacheFailureNoMapping(t *testing.T) {
	st := newFakeStore()
	st.failAfter = 1
	p := newTestPool(t, st, Config{PageSize: 100, TTL: time.Minute, OpTimeout: time.Second, MaxPendingPages: 8}, 2)

	payload := ssePayload("z", 300)
	c := p.NewCollector("req-3")
	writeInChunks(c, payload, 100)

	// give the worker time to fail
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !c.Failed() {
		time.Sleep(5 * time.Millisecond)
	}
	if !c.Failed() {
		t.Fatal("collector did not detect the page flush failure")
	}

	_, _, err := c.Finalize(context.Background())
	if !errors.Is(err, ErrCacheFailed) {
		t.Fatalf("got %v, want ErrCacheFailed", err)
	}
}

// TestQueueSaturationProtectsRAM covers DEVELOP.md §18/§19: when Redis
// cannot keep up and the bounded queue is full, the collector fails instead
// of growing RAM or blocking the client path.
func TestQueueSaturationProtectsRAM(t *testing.T) {
	st := newFakeStore()
	st.putDelay = 500 * time.Millisecond
	p := newTestPool(t, st, Config{PageSize: 100, TTL: time.Minute, OpTimeout: time.Second, MaxPendingPages: 1}, 1)

	c := p.NewCollector("req-4")
	// worker picks page 1 immediately and sleeps; page 2 fills the queue;
	// page 3 must hit the full queue and fail.
	page := make([]byte, 150)
	c.Write(page)
	c.Write(page)
	c.Write(page)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !c.Failed() {
		time.Sleep(5 * time.Millisecond)
	}
	if !c.Failed() {
		t.Fatal("queue saturation did not fail the collector")
	}
	_, _, err := c.Finalize(context.Background())
	if !errors.Is(err, ErrCacheFailed) {
		t.Fatalf("got %v, want ErrCacheFailed", err)
	}
}

// TestIncompleteStream covers DEVELOP.md §22: a stream without [DONE] must
// fail finalize.
func TestIncompleteStream(t *testing.T) {
	st := newFakeStore()
	p := newTestPool(t, st, Config{PageSize: 100, TTL: time.Minute, OpTimeout: time.Second, MaxPendingPages: 8}, 2)

	payload := []byte(`data: {"choices":[{"index":0,"delta":{"content":"hi"}}]}` + "\n\n")
	c := p.NewCollector("req-5")
	writeInChunks(c, payload, 30)

	_, _, err := c.Finalize(context.Background())
	if !errors.Is(err, ErrStreamIncomplete) {
		t.Fatalf("got %v, want ErrStreamIncomplete", err)
	}
}

// TestNoAssistantMessage: complete stream, but nothing mappable.
func TestNoAssistantMessage(t *testing.T) {
	st := newFakeStore()
	p := newTestPool(t, st, Config{PageSize: 100, TTL: time.Minute, OpTimeout: time.Second, MaxPendingPages: 8}, 2)

	payload := []byte("data: [DONE]\n\n")
	c := p.NewCollector("req-6")
	writeInChunks(c, payload, 10)

	_, _, err := c.Finalize(context.Background())
	if !errors.Is(err, ErrNoAssistantMessage) {
		t.Fatalf("got %v, want ErrNoAssistantMessage", err)
	}
}

// TestAbortDeletesTempPages covers the abort path (§22/§38).
func TestAbortDeletesTempPages(t *testing.T) {
	st := newFakeStore()
	p := newTestPool(t, st, Config{PageSize: 100, TTL: time.Minute, OpTimeout: time.Second, MaxPendingPages: 8}, 2)

	payload := ssePayload("a", 100)
	c := p.NewCollector("req-7")
	writeInChunks(c, payload, 100)

	// wait for the queued pages to land, then abort
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && st.pagecount("req-7") == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	c.Abort(context.Background())
	if got := st.pagecount("req-7"); got != 0 {
		t.Fatalf("abort did not delete temp pages: %d remain", got)
	}
}

// [DONE]-time detection: onDone fires the moment the sentinel line is fully
// written, content mentioning "[DONE]" never triggers it, and post-[DONE]
// writes are discarded (finalize already ran with the full payload).
func TestDoneDetectionAndEarlySeal(t *testing.T) {
	st := newFakeStore()
	p := newTestPool(t, st, Config{PageSize: 100, TTL: time.Minute, OpTimeout: time.Second, MaxPendingPages: 8}, 2)

	var fired int32
	payload := ssePayload("hi", 3) // ends with a [DONE] event
	c := p.NewCollector("req-done", func() {
		atomic.AddInt32(&fired, 1)
	})

	writeInChunks(c, payload, 17)
	if atomic.LoadInt32(&fired) != 1 {
		t.Fatalf("onDone fired %d times, want 1", fired)
	}
	if !c.Done() {
		t.Fatal("Done() must be true after the sentinel")
	}
	// post-[DONE] bytes are discarded
	before := len(st.pages["req-done"])
	c.Write([]byte("garbage after done\n\n"))
	if got := len(st.pages["req-done"]); got != before {
		t.Fatal("post-[DONE] writes must not flush pages")
	}

	// a data line whose JSON content merely mentions [DONE] must NOT trigger:
	// the tricky stream ends with a genuine [DONE], so exactly one legal
	// onDone (+1000) is expected and zero false positives
	c2 := p.NewCollector("req-false", func() { atomic.AddInt32(&fired, 1000) })
	tricky := "data: {\"choices\":[{\"delta\":{\"content\":\"see [DONE] marker\"}}]}\n\ndata: [DONE]\n\n"
	writeInChunks(c2, []byte(tricky), 5)
	if got := atomic.LoadInt32(&fired); got != 1001 {
		t.Fatalf("false-positive detection: fired=%d, want exactly 1001 (one legal trigger)", got)
	}
}
