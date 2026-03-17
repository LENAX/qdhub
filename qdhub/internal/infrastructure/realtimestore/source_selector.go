package realtimestore

import "sync"

const (
	SourceTushareWS = "tushare_ws"
	SourceSina      = "sina"

	HealthHealthy     = "healthy"
	HealthDegraded    = "degraded"
	HealthUnavailable = "unavailable"
)

// RealtimeSourceSelector 维护当前选中的实时数据源，以及各源健康度与最近故障原因。
// 生产环境双 Collector 同时运行时，仅当前选中源写入 LatestQuoteStore；故障时 SwitchTo 另一源。
type RealtimeSourceSelector struct {
	mu     sync.RWMutex
	active string // SourceTushareWS | SourceSina，默认 tushare_ws

	// 各源最近一次错误文案，空表示无故障
	sourceErrors map[string]string
}

// NewRealtimeSourceSelector 创建 Selector，默认选中 tushare_ws。
func NewRealtimeSourceSelector() *RealtimeSourceSelector {
	return &RealtimeSourceSelector{
		active:       SourceTushareWS,
		sourceErrors: make(map[string]string),
	}
}

// ShouldWriteToStore 仅当 source 为当前选中源时返回 true，用于写 LatestQuoteStore 前检查。
func (s *RealtimeSourceSelector) ShouldWriteToStore(source string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active == source
}

// SwitchTo 切换到另一数据源（故障时调用）。
func (s *RealtimeSourceSelector) SwitchTo(source string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if source == SourceTushareWS || source == SourceSina {
		s.active = source
	}
}

// CurrentSource 返回当前选中源。
func (s *RealtimeSourceSelector) CurrentSource() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active
}

// RecordSourceError 记录某源的最近故障原因（Sync 层在 execution 失败时调用）。
func (s *RealtimeSourceSelector) RecordSourceError(source, errMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if source == "" {
		return
	}
	s.sourceErrors[source] = errMsg
}

// SourcesHealth 返回各源健康度：有 RecordSourceError 且未恢复的为 unavailable，否则 healthy。
func (s *RealtimeSourceSelector) SourcesHealth() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.sourceErrors)+2)
	for _, src := range []string{SourceTushareWS, SourceSina} {
		if _, hasErr := s.sourceErrors[src]; hasErr {
			out[src] = HealthUnavailable
		} else {
			out[src] = HealthHealthy
		}
	}
	return out
}

// SourcesError 返回各源最近一次故障原因，无则空字符串。
func (s *RealtimeSourceSelector) SourcesError() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.sourceErrors))
	for k, v := range s.sourceErrors {
		out[k] = v
	}
	// 保证至少返回两个 key，便于前端统一处理
	if _, ok := out[SourceTushareWS]; !ok {
		out[SourceTushareWS] = ""
	}
	if _, ok := out[SourceSina]; !ok {
		out[SourceSina] = ""
	}
	return out
}
