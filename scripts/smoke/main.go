// OPGate smoke test: runs the real binary against a real TCP miniredis
// and a mock upstream, then drives it over actual HTTP.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func fatal(v any) {
	fmt.Println("SMOKE FAIL:", v)
	os.Exit(1)
}

type upstream struct {
	mu       sync.Mutex
	sessions []string
}

func main() {
	// 1. real Redis on a real TCP port
	mr, err := miniredis.Run()
	if err != nil {
		fatal(err)
	}
	defer mr.Close()

	// 2. mock OpenCode upstream (SSE)
	up := &upstream{}
	upMux := http.NewServeMux()
	upMux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = body
		up.mu.Lock()
		up.sessions = append(up.sessions, r.Header.Get("X-Opencode-Session"))
		up.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		for _, c := range []string{"mock ", "smoke ", "ok"} {
			fmt.Fprintf(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"%s\"}}]}\n\n", c)
			f.Flush()
			time.Sleep(10 * time.Millisecond)
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
		f.Flush()
	})
	upSrv := &http.Server{Addr: "127.0.0.1:18099", Handler: upMux}
	go upSrv.ListenAndServe()
	defer upSrv.Close()

	// 3. write config
	cfg := fmt.Sprintf(`server:
  listen: "127.0.0.1:18098"
upstream:
  base_url: "http://127.0.0.1:18099/v1"
redis:
  addr: "%s"
  db: 0
  timeout: 2s
session:
  ttl: 168h
response_cache:
  page_size: 4KiB
  ttl: 30m
  workers: 2
  max_pending_pages: 4
limits:
  max_request_body: 32MiB
logging:
  level: info
`, mr.Addr())
	if err := os.WriteFile("/data/data/com.termux/files/home/ocgate-smoke/config.yml", []byte(cfg), 0o644); err != nil {
		fatal(err)
	}

	// 4. start the real binary
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, "/data/data/com.termux/files/home/ocgate/ocgate", "-config", "/data/data/com.termux/files/home/ocgate-smoke/config.yml")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()
	time.Sleep(500 * time.Millisecond)

	client := &http.Client{Timeout: 10 * time.Second}

	// health
	resp, err := client.Get("http://127.0.0.1:18098/healthz")
	if err != nil || resp.StatusCode != 200 {
		fatal(fmt.Sprintf("healthz: %v %v", err, resp))
	}
	resp.Body.Close()

	// two chat rounds (client uses /chat/completions; base_url supplies /v1)
	post := func(body string) string {
		r, err := client.Post("http://127.0.0.1:18098/chat/completions", "application/json", strings.NewReader(body))
		if err != nil {
			fatal(err)
		}
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		if r.StatusCode != 200 {
			fatal(fmt.Sprintf("chat status %d: %s", r.StatusCode, b))
		}
		return string(b)
	}

	b1 := post(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	b2 := post(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"mock smoke ok"},{"role":"user","content":"again"}]}`)

	for _, b := range []string{b1, b2} {
		if !bytes.Contains([]byte(b), []byte("[DONE]")) {
			fatal("stream incomplete: " + b)
		}
	}
	time.Sleep(500 * time.Millisecond) // let finalize finish

	up.mu.Lock()
	sessions := append([]string(nil), up.sessions...)
	up.mu.Unlock()
	fmt.Println("upstream sessions:", sessions)
	if len(sessions) != 2 {
		fatal("expected 2 upstream requests")
	}
	if sessions[0] == "" || sessions[0] != sessions[1] {
		fatal("session affinity broken: " + strings.Join(sessions, ","))
	}

	// metrics
	resp, err = client.Get("http://127.0.0.1:18098/metrics")
	if err != nil {
		fatal(err)
	}
	mb, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	metricsText := string(mb)
	for _, want := range []string{
		"opgate_session_resolve_hit_total",
		"opgate_session_mapping_created_total",
		"opgate_page_flush_success_total",
	} {
		if !strings.Contains(metricsText, want) {
			fatal("metrics missing " + want)
		}
	}

	// 5. graceful shutdown via SIGTERM
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		fmt.Println("process exited:", err)
	case <-time.After(45 * time.Second):
		fatal("graceful shutdown timed out")
	}

	fmt.Println("SMOKE OK")
}
