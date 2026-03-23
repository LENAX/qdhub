package realtimestore

import "sync"

// SinaRealtimeDisabled 为 true 时不使用新浪作为实时行情 Pull 源、不并行启动新浪 workflow、禁止从控制台 connect 新浪源。
// 与「仅 ts_proxy 主源」策略一致；若需恢复新浪，改为 false 并重新部署。
const SinaRealtimeDisabled = true

// TushareWSRealtimeDisabled 为 true 时不使用直连 Tushare 官方 WebSocket（ts_realtime_mkt_tick）、禁止从控制台 connect 该源。
// 仅保留内地 ts_proxy 转发；若需恢复直连，改为 false 并重新部署。
const TushareWSRealtimeDisabled = true

const (
	SourceTushareWS    = "tushare_ws"
	SourceTushareProxy = "tushare_proxy" // 从 ts_proxy 转发端接收
	SourceSina           = "sina"
	SourceEastmoney      = "eastmoney" // 东财
	SourceNews           = "news"      // 新闻快讯，pull-based

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

// NewRealtimeSourceSelector 创建 Selector。禁用直连 WS 时默认 tushare_proxy，否则默认 tushare_ws。
func NewRealtimeSourceSelector() *RealtimeSourceSelector {
	active := SourceTushareWS
	if TushareWSRealtimeDisabled {
		active = SourceTushareProxy
	}
	return &RealtimeSourceSelector{
		active:       active,
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
	if source == SourceTushareWS || source == SourceSina || source == SourceTushareProxy || source == SourceEastmoney {
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
	out := make(map[string]string, len(s.sourceErrors)+5)
	for _, src := range []string{SourceTushareWS, SourceTushareProxy, SourceSina, SourceEastmoney, SourceNews} {
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
	// 保证至少返回各源 key，便于前端统一处理
	for _, src := range []string{SourceTushareWS, SourceTushareProxy, SourceSina, SourceEastmoney, SourceNews} {
		if _, ok := out[src]; !ok {
			out[src] = ""
		}
	}
	return out
}

// FilterOutSinaSources 从 Pull 源列表中移除新浪（当 SinaRealtimeDisabled 时）。
func FilterOutSinaSources(src []string) []string {
	if !SinaRealtimeDisabled || len(src) == 0 {
		return src
	}
	out := make([]string, 0, len(src))
	for _, s := range src {
		if s != SourceSina {
			out = append(out, s)
		}
	}
	return out
}
