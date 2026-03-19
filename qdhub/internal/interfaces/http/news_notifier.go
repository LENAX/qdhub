package http

import (
	"sync"

	"github.com/sirupsen/logrus"
)

// NewsUpdateNotifier 用于在新闻数据写入后通知 SSE 等订阅方立即推送。
// 实现方应在 Notify 时非阻塞地通知所有订阅者。
type NewsUpdateNotifier interface {
	Notify()
	Subscribe() (notifyCh <-chan struct{}, unsubscribe func())
}

// NewsUpdateBroadcaster 是 NewsUpdateNotifier 的进程内广播实现，供 SSE 与 WriteQueue 回调共用。
// 使用 map + Mutex 保证 Notify 和 unsubscribe 之间不会出现 send on closed channel。
type NewsUpdateBroadcaster struct {
	mu     sync.Mutex
	subs   map[uint64]chan struct{}
	nextID uint64
}

func NewNewsUpdateBroadcaster() *NewsUpdateBroadcaster {
	return &NewsUpdateBroadcaster{subs: make(map[uint64]chan struct{})}
}

// Notify 在持锁期间完成非阻塞发送，确保不会往已关闭的 channel 写入。
func (b *NewsUpdateBroadcaster) Notify() {
	b.mu.Lock()
	var sent, skipped int
	total := len(b.subs)
	for _, ch := range b.subs {
		select {
		case ch <- struct{}{}:
			sent++
		default:
			skipped++
		}
	}
	b.mu.Unlock()
	if total > 0 {
		logrus.Infof("[NewsNotifier] Notify: subscribers=%d, sent=%d, skipped=%d", total, sent, skipped)
	}
}

func (b *NewsUpdateBroadcaster) Subscribe() (notifyCh <-chan struct{}, unsubscribe func()) {
	ch := make(chan struct{}, 2)
	b.mu.Lock()
	id := b.nextID
	b.nextID++
	b.subs[id] = ch
	b.mu.Unlock()
	var once sync.Once
	unsubscribe = func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subs, id)
			close(ch)
			b.mu.Unlock()
		})
	}
	return ch, unsubscribe
}
