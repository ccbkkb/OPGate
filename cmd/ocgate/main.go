// OPGate — OpenCode Session Gateway.
//
// A lightweight reverse proxy in front of the OpenCode API that injects and
// maintains a stable x-opencode-session header for clients that cannot send
// one themselves, keyed by conversation state hashes, with a Redis-backed
// spill cache for long SSE responses.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ocgate/internal/collector"
	"ocgate/internal/config"
	"ocgate/internal/metrics"
	"ocgate/internal/proxy"
	"ocgate/internal/session"
	"ocgate/internal/store"
)

// version is overridable at build time via:
//   -ldflags "-X main.version=v1.2.3"
var version = "dev"

func buildLogger(level string) *slog.Logger {
	lvl := slog.LevelInfo
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	}
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
	logger := slog.New(h)
	slog.SetDefault(logger)
	return logger
}

func main() {
	configPath := flag.String("config", "config.yml", "path to the YAML config file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("opgate " + version)
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	logger := buildLogger(cfg.Logging.Level)

	if time.Duration(cfg.ResponseCache.TTL) >= time.Duration(cfg.Session.TTL) {
		logger.Warn("response_cache.ttl should be clearly smaller than session.ttl",
			"response_cache_ttl", time.Duration(cfg.ResponseCache.TTL),
			"session_ttl", time.Duration(cfg.Session.TTL))
	}

	m := metrics.New()

	// SessionStore and TempPageStore: Redis is the first implementation.
	rs := store.NewRedisStore(cfg.Redis.Addr, cfg.Redis.DB, time.Duration(cfg.Redis.Timeout), m)
	pingCtx, cancelPing := context.WithTimeout(context.Background(), time.Duration(cfg.Redis.Timeout))
	if err := rs.Ping(pingCtx); err != nil {
		logger.Warn("redis is not reachable (the gateway keeps running; "+
			"session resolution and page cache will degrade)", "error", err)
	}
	cancelPing()

	pool := collector.NewPool(rs, collector.Config{
		PageSize:        int(cfg.ResponseCache.PageSize),
		TTL:             time.Duration(cfg.ResponseCache.TTL),
		OpTimeout:       time.Duration(cfg.Redis.Timeout),
		MaxPendingPages: cfg.ResponseCache.MaxPendingPages,
	}, m, logger)
	pool.Start(cfg.ResponseCache.Workers)

	resolver := session.NewResolver(rs, time.Duration(cfg.Redis.Timeout), m, logger)

	upstream, err := url.Parse(cfg.Upstream.BaseURL)
	if err != nil {
		logger.Error("invalid upstream url", "error", err)
		os.Exit(1)
	}
	handler := proxy.NewHandler(cfg, upstream, rs, pool, resolver, m, logger)

	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("/", handler)

	srv := &http.Server{
		Addr:              cfg.Server.Listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	logger.Info("opgate listening",
		"version", version,
		"addr", cfg.Server.Listen,
		"upstream", cfg.Upstream.BaseURL,
		"redis", cfg.Redis.Addr,
		"page_size", int64(cfg.ResponseCache.PageSize),
		"workers", cfg.ResponseCache.Workers,
	)

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
		return
	case <-ctx.Done():
	}

	// Graceful shutdown (DEVELOP.md §56):
	//  1. stop accepting new requests and wait for existing ones,
	//  2. let the page flush queue drain (workers still running),
	//  3. wait for detached finalize goroutines,
	//  4. stop the workers and close Redis.
	// Leftover temporary Redis entries expire via TTL.
	logger.Info("shutdown signal received; draining")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http shutdown incomplete", "error", err)
	}
	pool.Drain(10 * time.Second)
	handler.WaitFinalize(30 * time.Second)
	pool.Stop()
	_ = rs.Close()
	logger.Info("shutdown complete")
}
