// Package config loads the OPGate YAML configuration.
//
// Only a small set of options is supported (see DEVELOP.md §55); unknown
// fields in the file are ignored.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that accepts Go duration strings ("168h",
// "30m", "2s") or a plain number of seconds.
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	switch node.Tag {
	case "!!int":
		secs, err := strconv.ParseInt(node.Value, 10, 64)
		if err != nil {
			return fmt.Errorf("config: invalid duration %q: %w", node.Value, err)
		}
		*d = Duration(time.Duration(secs) * time.Second)
		return nil
	default:
		v, err := time.ParseDuration(strings.TrimSpace(node.Value))
		if err != nil {
			return fmt.Errorf("config: invalid duration %q: %w", node.Value, err)
		}
		*d = Duration(v)
		return nil
	}
}

// ByteSize is a byte count that accepts plain integers (bytes) or strings
// with binary-size suffixes: B, K/KB/KiB, M/MB/MiB, G/GB/GiB, T/TB/TiB.
type ByteSize int64

// UnmarshalYAML implements yaml.Unmarshaler.
func (b *ByteSize) UnmarshalYAML(node *yaml.Node) error {
	if node.Tag == "!!int" {
		v, err := strconv.ParseInt(node.Value, 10, 64)
		if err != nil {
			return fmt.Errorf("config: invalid byte size %q: %w", node.Value, err)
		}
		*b = ByteSize(v)
		return nil
	}
	s := strings.ToUpper(strings.TrimSpace(node.Value))
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
		i++
	}
	numStr, suffix := s[:i], strings.TrimSpace(s[i:])
	if numStr == "" {
		return fmt.Errorf("config: invalid byte size %q", node.Value)
	}
	var mult float64 = 1
	switch suffix {
	case "", "B":
		mult = 1
	case "K", "KB", "KIB":
		mult = 1 << 10
	case "M", "MB", "MIB":
		mult = 1 << 20
	case "G", "GB", "GIB":
		mult = 1 << 30
	case "T", "TB", "TIB":
		mult = 1 << 40
	default:
		return fmt.Errorf("config: unknown size suffix %q in %q", suffix, node.Value)
	}
	f, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return fmt.Errorf("config: invalid byte size %q: %w", node.Value, err)
	}
	*b = ByteSize(int64(f * mult))
	return nil
}

type Server struct {
	Listen string `yaml:"listen"`
}

type Upstream struct {
	BaseURL string `yaml:"base_url"`
}

type Redis struct {
	Addr    string   `yaml:"addr"`
	DB      int      `yaml:"db"`
	Timeout Duration `yaml:"timeout"`
}

type Session struct {
	TTL Duration `yaml:"ttl"`
}

type ResponseCache struct {
	PageSize        ByteSize `yaml:"page_size"`
	TTL             Duration `yaml:"ttl"`
	Workers         int      `yaml:"workers"`
	MaxPendingPages int      `yaml:"max_pending_pages"`
}

type Limits struct {
	MaxRequestBody ByteSize `yaml:"max_request_body"`
}

type Logging struct {
	Level string `yaml:"level"`
}

// Config is the root configuration.
type Config struct {
	Server        Server        `yaml:"server"`
	Upstream      Upstream      `yaml:"upstream"`
	Redis         Redis         `yaml:"redis"`
	Session       Session       `yaml:"session"`
	ResponseCache ResponseCache `yaml:"response_cache"`
	Limits        Limits        `yaml:"limits"`
	Logging       Logging       `yaml:"logging"`
}

// Default returns the default configuration.
func Default() *Config {
	return &Config{
		Server:        Server{Listen: ":8080"},
		Redis:         Redis{Addr: "127.0.0.1:6379", DB: 0, Timeout: Duration(2 * time.Second)},
		Session:       Session{TTL: Duration(168 * time.Hour)}, // 7 days
		ResponseCache: ResponseCache{PageSize: 4 << 20, TTL: Duration(30 * time.Minute), Workers: 4, MaxPendingPages: 8},
		Limits:        Limits{MaxRequestBody: 32 << 20},
		Logging:       Logging{Level: "info"},
	}
}

// Load reads and validates the YAML config file on top of the defaults.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	cfg := Default()
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate checks required fields and value ranges.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.Upstream.BaseURL) == "" {
		return fmt.Errorf("config: upstream.base_url is required")
	}
	u, err := url.Parse(c.Upstream.BaseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("config: upstream.base_url %q is not a valid http(s) URL", c.Upstream.BaseURL)
	}
	if strings.TrimSpace(c.Redis.Addr) == "" {
		return fmt.Errorf("config: redis.addr is required")
	}
	if c.Redis.Timeout <= 0 {
		return fmt.Errorf("config: redis.timeout must be positive")
	}
	if c.Session.TTL <= 0 {
		return fmt.Errorf("config: session.ttl must be positive")
	}
	if c.ResponseCache.PageSize <= 0 {
		return fmt.Errorf("config: response_cache.page_size must be positive")
	}
	if c.ResponseCache.TTL <= 0 {
		return fmt.Errorf("config: response_cache.ttl must be positive")
	}
	if c.ResponseCache.Workers <= 0 {
		return fmt.Errorf("config: response_cache.workers must be >= 1")
	}
	if c.ResponseCache.MaxPendingPages <= 0 {
		return fmt.Errorf("config: response_cache.max_pending_pages must be >= 1")
	}
	if c.Limits.MaxRequestBody <= 0 {
		return fmt.Errorf("config: limits.max_request_body must be positive")
	}
	return nil
}
