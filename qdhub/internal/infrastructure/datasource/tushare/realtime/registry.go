package realtime

import "sync"

// DefaultRegistry 默认的 RealtimeAdapter 注册表（内存 map，线程安全）
type DefaultRegistry struct {
	mu       sync.RWMutex
	adapters map[string]RealtimeAdapter
}

// NewDefaultRegistry 创建空注册表
func NewDefaultRegistry() *DefaultRegistry {
	return &DefaultRegistry{adapters: make(map[string]RealtimeAdapter)}
}

// Get 按 src 获取 adapter
func (r *DefaultRegistry) Get(src string) (RealtimeAdapter, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.adapters[src]
	return a, ok
}

// Register 注册 adapter
func (r *DefaultRegistry) Register(src string, adapter RealtimeAdapter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.adapters == nil {
		r.adapters = make(map[string]RealtimeAdapter)
	}
	r.adapters[src] = adapter
}

// NewRegistryWithDefaults 创建并注册新浪、东财 adapter 的注册表（供 Tushare 实时 Collector 使用）
func NewRegistryWithDefaults() *DefaultRegistry {
	r := NewDefaultRegistry()
	r.Register("sina", NewSinaRealtimeAdapter())
	r.Register("eastmoney", NewEastmoneyRealtimeAdapter())
	r.Register("dc", NewEastmoneyRealtimeAdapter()) // 与 Tushare 文档中 src=dc 对应
	return r
}
