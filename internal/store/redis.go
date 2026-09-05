package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"ocgate/internal/metrics"
)

// KeyVersion is the schema version prefix required on every key
// (DEVELOP.md §41). Bump to oc:v2 when the key schema changes.
const KeyVersion = "oc:v1"

// SessionKey builds oc:v1:session:{clientHash}:{stateHash}.
func SessionKey(clientHash, stateHash string) string {
	return KeyVersion + ":session:" + clientHash + ":" + stateHash
}

// TempKey builds oc:v1:tmp:{requestID}.
func TempKey(requestID string) string {
	return KeyVersion + ":tmp:" + requestID
}

// PageField renders a page sequence number ("000001", "000002", ...).
func PageField(seq int) string {
	return fmt.Sprintf("%06d", seq)
}

// RedisStore implements both SessionStore and TempPageStore on top of Redis.
// Every operation is bounded by a timeout; a Redis failure never propagates
// into the client's SSE path.
type RedisStore struct {
	rdb     *redis.Client
	timeout time.Duration
	m       *metrics.Metrics
}

// NewRedisStore creates the store. It does not fail when Redis is
// unreachable right now; operations will report errors instead.
func NewRedisStore(addr string, db int, timeout time.Duration, m *metrics.Metrics) *RedisStore {
	return &RedisStore{
		rdb: redis.NewClient(&redis.Options{
			Addr:         addr,
			DB:           db,
			DialTimeout:  timeout,
			ReadTimeout:  timeout,
			WriteTimeout: timeout,
		}),
		timeout: timeout,
		m:       m,
	}
}

// Ping checks connectivity (used at startup for a warning only).
func (s *RedisStore) Ping(ctx context.Context) error {
	return s.rdb.Ping(ctx).Err()
}

// Close closes the underlying client.
func (s *RedisStore) Close() error {
	return s.rdb.Close()
}

func (s *RedisStore) observe(op string, start time.Time, err error) {
	s.m.RedisLatency.WithLabelValues(op).Observe(time.Since(start).Seconds())
	if err != nil && !errors.Is(err, redis.Nil) {
		s.m.SessionStoreErrorInc(op)
	}
}

func (s *RedisStore) ctx(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, s.timeout)
}

// GetSession implements SessionStore.
func (s *RedisStore) GetSession(parent context.Context, clientHash, stateHash string) (string, bool, error) {
	ctx, cancel := s.ctx(parent)
	defer cancel()
	start := time.Now()
	val, err := s.rdb.Get(ctx, SessionKey(clientHash, stateHash)).Result()
	s.observe("session_get", start, err)
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return val, true, nil
}

// PutSession implements SessionStore.
func (s *RedisStore) PutSession(parent context.Context, clientHash, stateHash, sessionID string, ttl time.Duration) error {
	ctx, cancel := s.ctx(parent)
	defer cancel()
	start := time.Now()
	err := s.rdb.Set(ctx, SessionKey(clientHash, stateHash), sessionID, ttl).Err()
	s.observe("session_put", start, err)
	return err
}

// PutPage implements TempPageStore. It is a single round trip that writes
// the page and refreshes the entry TTL.
func (s *RedisStore) PutPage(parent context.Context, requestID string, seq int, data []byte, ttl time.Duration) error {
	ctx, cancel := s.ctx(parent)
	defer cancel()
	key := TempKey(requestID)
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, key, PageField(seq), data)
	pipe.Expire(ctx, key, ttl)
	start := time.Now()
	_, err := pipe.Exec(ctx)
	s.observe("page_put", start, err)
	return err
}

// GetPages implements TempPageStore. A missing entry yields an empty map.
func (s *RedisStore) GetPages(parent context.Context, requestID string) (map[int][]byte, error) {
	ctx, cancel := s.ctx(parent)
	defer cancel()
	start := time.Now()
	vals, err := s.rdb.HGetAll(ctx, TempKey(requestID)).Result()
	s.observe("page_get", start, err)
	if err != nil {
		return nil, err
	}
	out := make(map[int][]byte, len(vals))
	for f, v := range vals {
		seq, perr := strconv.Atoi(f)
		if perr != nil {
			continue // not a page field; ignore
		}
		out[seq] = []byte(v)
	}
	return out, nil
}

// DeletePages implements TempPageStore.
func (s *RedisStore) DeletePages(parent context.Context, requestID string) error {
	ctx, cancel := s.ctx(parent)
	defer cancel()
	start := time.Now()
	err := s.rdb.Del(ctx, TempKey(requestID)).Err()
	s.observe("page_del", start, err)
	return err
}
