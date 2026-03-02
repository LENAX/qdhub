// Package cache provides in-memory implementation of CommonDataCache.
package cache

import (
	"context"
	"sync"
	"time"

	data_sync "qdhub/internal/domain/sync"
)

const defaultTTL = 24 * time.Hour

// MemoryCommonDataCache 内存实现的公共数据缓存，带 TTL（默认 24h）。
// 适用于单进程；后续可替换为 Redis 实现同一接口。
type MemoryCommonDataCache struct {
	mu    sync.RWMutex
	store map[string]cacheEntry
	ttl   time.Duration
}

type cacheEntry struct {
	data     []map[string]any
	expireAt time.Time
}

// NewMemoryCommonDataCache 创建内存缓存，ttl 为默认过期时间（0 则用 24h）。
func NewMemoryCommonDataCache(ttl time.Duration) *MemoryCommonDataCache {
	if ttl <= 0 {
		ttl = defaultTTL
	}
	return &MemoryCommonDataCache{
		store: make(map[string]cacheEntry),
		ttl:   ttl,
	}
}

// Get 实现 CommonDataCache.Get。若已过期则视为未命中并删除。
func (c *MemoryCommonDataCache) Get(ctx context.Context, key string) ([]map[string]any, bool) {
	c.mu.RLock()
	ent, ok := c.store[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if time.Now().After(ent.expireAt) {
		c.mu.Lock()
		delete(c.store, key)
		c.mu.Unlock()
		return nil, false
	}
	return ent.data, true
}

// Set 实现 CommonDataCache.Set。
func (c *MemoryCommonDataCache) Set(ctx context.Context, key string, data []map[string]any, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = c.ttl
	}
	c.mu.Lock()
	c.store[key] = cacheEntry{data: data, expireAt: time.Now().Add(ttl)}
	c.mu.Unlock()
	return nil
}

var _ data_sync.CommonDataCache = (*MemoryCommonDataCache)(nil)
