package config

import (
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func parseNode(t *testing.T, v string) *yaml.Node {
	t.Helper()
	var n yaml.Node
	if err := yaml.Unmarshal([]byte(v), &n); err != nil {
		t.Fatal(err)
	}
	// yaml.Unmarshal on a scalar produces a document node; use its content
	if n.Kind == 1 && len(n.Content) > 0 {
		return n.Content[0]
	}
	return &n
}

func TestByteSizeParsing(t *testing.T) {
	cases := map[string]int64{
		"4MiB":    4 << 20,
		"512KiB":  512 << 10,
		"1MiB":    1 << 20,
		"2MB":     2 << 20,
		"33554432": 33554432,
		"1.5MiB":  1572864,
		"32MiB":   32 << 20,
		"64":      64,
	}
	for in, want := range cases {
		var b ByteSize
		if err := b.UnmarshalYAML(parseNode(t, in)); err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if int64(b) != want {
			t.Fatalf("%s: got %d want %d", in, b, want)
		}
	}
	var bad ByteSize
	if err := bad.UnmarshalYAML(parseNode(t, "12LiB")); err == nil {
		t.Fatal("expected error for unknown suffix")
	}
}

func TestDurationParsing(t *testing.T) {
	cases := map[string]time.Duration{
		"168h":  168 * time.Hour,
		"30m":   30 * time.Minute,
		"2s":    2 * time.Second,
		"500ms": 500 * time.Millisecond,
		"30":    30 * time.Second,
	}
	for in, want := range cases {
		var d Duration
		if err := d.UnmarshalYAML(parseNode(t, in)); err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if time.Duration(d) != want {
			t.Fatalf("%s: got %s want %s", in, time.Duration(d), want)
		}
	}
}

func TestDefaults(t *testing.T) {
	cfg := Default()
	if cfg.Server.Listen != ":8080" {
		t.Fatalf("listen: %s", cfg.Server.Listen)
	}
	if time.Duration(cfg.Session.TTL) != 168*time.Hour {
		t.Fatalf("session ttl: %s", time.Duration(cfg.Session.TTL))
	}
	if int64(cfg.ResponseCache.PageSize) != 4<<20 {
		t.Fatalf("page size: %d", cfg.ResponseCache.PageSize)
	}
	if cfg.ResponseCache.Workers != 4 || cfg.ResponseCache.MaxPendingPages != 8 {
		t.Fatalf("workers/pending: %d/%d", cfg.ResponseCache.Workers, cfg.ResponseCache.MaxPendingPages)
	}
	if int64(cfg.Limits.MaxRequestBody) != 32<<20 {
		t.Fatalf("max body: %d", cfg.Limits.MaxRequestBody)
	}
}

func TestValidate(t *testing.T) {
	cfg := Default()
	cfg.Upstream.BaseURL = "https://opencode.example.com/v1"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	missing := Default()
	if err := missing.Validate(); err == nil {
		t.Fatal("expected error for missing base_url")
	}
	cfg.Upstream.BaseURL = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for missing base_url")
	}
	cfg.Upstream.BaseURL = "ftp://nope"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for non-http base_url")
	}
}
