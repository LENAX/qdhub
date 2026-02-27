// Package duckdb provides DuckDB adapter and factory for QuantDB interface.
package duckdb

import (
	"context"
	"log"
	"path/filepath"
	"sync"

	"qdhub/internal/domain/datastore"
)

// Factory creates DuckDB QuantDB instances by storage path.
// It caches adapters by path so the same path is reused (avoids repeated Connect).
type Factory struct {
	mu    sync.RWMutex
	cache map[string]datastore.QuantDB
}

// NewFactory creates a new DuckDB QuantDB factory.
func NewFactory() *Factory {
	return &Factory{cache: make(map[string]datastore.QuantDB)}
}

// Create returns a QuantDB adapter for the given config.
// For DuckDB type, uses config.StoragePath. Caches by path and reuses connection.
func (f *Factory) Create(config datastore.QuantDBConfig) (datastore.QuantDB, error) {
	if config.Type != datastore.DataStoreTypeDuckDB {
		return nil, datastore.ErrUnsupportedQuantDBType(config.Type)
	}
	path := config.StoragePath
	if path == "" {
		path = config.DSN
	}
	if path == "" {
		return nil, datastore.ErrQuantDBPathRequired
	}
	// 将相对路径解析为绝对路径，避免因进程 CWD 不同导致建表写入到错误文件
	if !filepath.IsAbs(path) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		path = abs
	}

	f.mu.RLock()
	if adapter, ok := f.cache[path]; ok {
		f.mu.RUnlock()
		if err := adapter.Ping(context.Background()); err == nil {
			return adapter, nil
		}
		// Stale, will recreate below with write lock
		f.mu.Lock()
		if a, ok := f.cache[path]; ok {
			_ = a.Close()
			delete(f.cache, path)
		}
		f.mu.Unlock()
	} else {
		f.mu.RUnlock()
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if adapter, ok := f.cache[path]; ok {
		if err := adapter.Ping(context.Background()); err == nil {
			return adapter, nil
		}
		_ = adapter.Close()
		delete(f.cache, path)
	}

	adapter := NewAdapter(path)
	if err := adapter.Connect(context.Background()); err != nil {
		return nil, err
	}
	log.Printf("[DuckDB Factory] open path (resolved): %s", path)
	f.cache[path] = adapter
	return adapter, nil
}

// Close closes all cached connections. Call when shutting down.
func (f *Factory) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for path, adapter := range f.cache {
		_ = adapter.Close()
		delete(f.cache, path)
	}
	return nil
}
