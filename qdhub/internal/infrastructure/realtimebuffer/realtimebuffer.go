// Package realtimebuffer 提供实时同步 DataCollector → Handler 的数据 buffer，与 taskengine、jobs 无循环依赖。
package realtimebuffer

import (
	"sync"
)

// Batch 实时数据批次，带来源与 API 名便于落库
type Batch struct {
	Data    []map[string]interface{}
	Source  string
	APIName string
}

// Buffer 单个工作流实例的实时数据 buffer，供多个 Collector Push、一个 Handler 消费
type Buffer interface {
	Push(batch Batch)
	Recv() <-chan Batch
	Close()
}

// Registry 按工作流实例 ID 管理 buffer
type Registry interface {
	GetOrCreate(instanceID string) Buffer
	Get(instanceID string) (Buffer, bool)
	CloseAndRemove(instanceID string)
}

// DefaultBuffer 基于 channel 的 buffer 实现
type DefaultBuffer struct {
	ch     chan Batch
	closed bool
	mu     sync.Mutex
}

// NewDefaultBuffer 创建 buffer，cap 为 channel 容量
func NewDefaultBuffer(cap int) *DefaultBuffer {
	if cap <= 0 {
		cap = 256
	}
	return &DefaultBuffer{ch: make(chan Batch, cap)}
}

func (b *DefaultBuffer) Push(batch Batch) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	select {
	case b.ch <- batch:
	default:
	}
}

func (b *DefaultBuffer) Recv() <-chan Batch {
	return b.ch
}

func (b *DefaultBuffer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.closed = true
		close(b.ch)
	}
}

// DefaultRegistry 内存 map 实现的 Registry
type DefaultRegistry struct {
	mu      sync.RWMutex
	buffers map[string]Buffer
	cap     int
}

// NewDefaultRegistry 创建 Registry
func NewDefaultRegistry(channelCap int) *DefaultRegistry {
	if channelCap <= 0 {
		channelCap = 256
	}
	return &DefaultRegistry{buffers: make(map[string]Buffer), cap: channelCap}
}

func (r *DefaultRegistry) GetOrCreate(instanceID string) Buffer {
	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok := r.buffers[instanceID]; ok {
		return b
	}
	b := NewDefaultBuffer(r.cap)
	r.buffers[instanceID] = b
	return b
}

func (r *DefaultRegistry) Get(instanceID string) (Buffer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.buffers[instanceID]
	return b, ok
}

func (r *DefaultRegistry) CloseAndRemove(instanceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok := r.buffers[instanceID]; ok {
		b.Close()
		delete(r.buffers, instanceID)
	}
}
