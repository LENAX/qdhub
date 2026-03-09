// Package realtime 提供 Tushare 实时行情相关接口与实现（新浪、东财等爬虫源）。
// 与 Tushare Pro API 分离，数据不经过 Tushare 服务器，仅供学习研究使用。
package realtime

import "context"

// RealtimeMode 实时数据获取模式
type RealtimeMode string

const (
	RealtimeModePull RealtimeMode = "pull"
	RealtimeModePush RealtimeMode = "push"
)

// RealtimeAdapter 实时数据接口，支持 Pull（HTTP 轮询）与 Push（WebSocket/MQ 消费）
type RealtimeAdapter interface {
	// Source 返回数据源标识，如 sina、eastmoney、jqdata、qmt
	Source() string

	// SupportedAPIs 返回支持的 API 列表
	SupportedAPIs() []string

	// Supports 检查是否支持某 API
	Supports(apiName string) bool

	// SupportedModes 返回某 API 支持的模式（pull、push 或两者）
	SupportedModes(apiName string) []RealtimeMode

	// ---------- Pull 模式（HTTP 轮询） ----------

	// Fetch 单次拉取；params 含 ts_code、freq、src 等；返回字段与 Tushare 文档一致（[]map[string]interface{}）
	Fetch(ctx context.Context, apiName string, params map[string]interface{}) ([]map[string]interface{}, error)

	// ---------- Push 模式（WebSocket/MQ 消费） ----------

	// SupportsPush 某 API 是否支持 Push 模式
	SupportsPush(apiName string) bool

	// StartStream 以 Push 模式订阅行情流；params 含 endpoint、topic、auth 等连接配置；
	// onBatch 每收到一批数据回调，Collector 在回调中 publish
	StartStream(ctx context.Context, apiName string, params map[string]interface{}, onBatch func([]map[string]interface{}) error) error
}

// RealtimeAdapterRegistry 按 src 注册/查找 adapter
type RealtimeAdapterRegistry interface {
	Get(src string) (RealtimeAdapter, bool)
	Register(src string, adapter RealtimeAdapter)
}
