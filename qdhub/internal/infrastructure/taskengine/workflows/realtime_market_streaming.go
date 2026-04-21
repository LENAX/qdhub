package workflows

import (
	"fmt"
	"time"

	"github.com/LENAX/task-engine/pkg/core/builder"
	taskrealtime "github.com/LENAX/task-engine/pkg/core/realtime"
	"github.com/LENAX/task-engine/pkg/core/task"
	"github.com/LENAX/task-engine/pkg/core/workflow"

	"qdhub/internal/domain/shared"
)

// RealtimeMarketStreamingParams 定义实时行情 Streaming Workflow 所需的业务参数。
// 与 RealtimeSyncParams/RealtimeDataSyncRequest 保持一致，以便在 SyncPlan 层复用：
// - DataSourceName / Token / TargetDBPath：数据源与目标 DuckDB；
// - APINames：需要同步的实时 API（当前聚焦 realtime_quote，后续可扩展）；
// - TsCodes / IndexCodes：从 stock_basic / index_basic 解析出的 ts_code 列表，供策略分片用；
// - PullIntervalSecs：Pull 模式下的轮询间隔（秒）。
type RealtimeMarketStreamingParams struct {
	DataSourceID     shared.ID
	DataSourceName   string
	Token            string
	TargetDBPath     string
	APINames         []string
	TsCodes          []string
	IndexCodes       []string
	PullIntervalSecs int
}

// RealtimeMarketStreamingBuilder 基于 Task Engine Streaming 能力构建实时行情 Workflow：
// - 使用 WorkflowBuilder.WithStreamingMode；
// - 使用 RealtimeTaskBuilder.WithContinuousMode + TaskTypeDataCollector/TaskTypeStreamProcessor；
// - 通过 WorkflowBuilder.WithDataCollector 注册 DataCollector 实现。
// 诊断与限流：DataCollector（如 QuotePullCollector）内建每 3 秒打印工作状态与已获取数据条数；
// 若实时接口被 ban/多 IP 限制，Collector 返回错误，工作流自动取消并返回错误信息。
// 全市场 tick（ts_realtime_mkt_tick）经 ts_proxy 时由 WorkflowExecutor 使用 TushareWSStreamingBuilder，
// 采集器为 ForwardTickCollector（Run 内自持断线重连）；本 Builder 服务 realtime_quote / realtime_tick 等路径。
type RealtimeMarketStreamingBuilder struct {
	registry task.FunctionRegistry
	params   RealtimeMarketStreamingParams

	// collectorName 由调用方约定，用于 RealtimeTaskBuilder.WithCollector 与 WorkflowBuilder.WithDataCollector 关联。
	collectorName string
	// collector 为具体的 DataCollector 实现（如 QuotePullCollector），由上层注入。
	collector taskrealtime.DataCollector
}

// NewRealtimeMarketStreamingBuilder 创建 Streaming Workflow 构建器。
func NewRealtimeMarketStreamingBuilder(
	registry task.FunctionRegistry,
	params RealtimeMarketStreamingParams,
	collectorName string,
	collector taskrealtime.DataCollector,
) *RealtimeMarketStreamingBuilder {
	return &RealtimeMarketStreamingBuilder{
		registry:      registry,
		params:        params,
		collectorName: collectorName,
		collector:     collector,
	}
}

// Build 构建基于 Streaming 的实时行情工作流。
// 返回的 Workflow ID/name 固定为 "realtime_market_sync"，供 WorkflowExecutor 统一使用。
func (b *RealtimeMarketStreamingBuilder) Build() (*workflow.Workflow, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}

	apiName := "realtime_quote"
	if len(b.params.APINames) > 0 && b.params.APINames[0] != "" {
		apiName = b.params.APINames[0]
	}

	collectorMode := taskrealtime.CollectorModePull
	if apiName == "realtime_tick" {
		collectorMode = taskrealtime.CollectorModePush
	}

	// RealtimeTask：行情采集（Continuous DataCollector）
	quoteCollectorB := builder.NewRealtimeTaskBuilder("quote_collector", "实时行情采集", b.registry).
		WithContinuousMode().
		WithTaskType(taskrealtime.TaskTypeDataCollector).
		// 指定内置 Job 函数名称，避免底层基础任务校验 JobFunctionName 为空
		WithJobFunction("RealtimeDataCollector", map[string]interface{}{}).
		WithCollector(b.collectorName).
		WithMode(collectorMode).
		WithEndpoint("tushare_realtime", "http").
		// FlushInterval 由 SyncPlan 的 pull_interval_seconds 映射，DataCollector 内部可按需使用。
		WithFlushInterval(time.Duration(b.effectivePullInterval()) * time.Second)
	if collectorMode == taskrealtime.CollectorModePush {
		// Push 长连接：与引擎默认一致（0=无限次），采集器侧仍负责实际重连策略。
		quoteCollectorB = quoteCollectorB.WithReconnect(true, 0)
	}
	quoteCollectorTask, err := quoteCollectorB.Build()
	if err != nil {
		return nil, fmt.Errorf("build quote collector task: %w", err)
	}

	// RealtimeTask：流处理（Continuous StreamProcessor），DataHandler 负责写 DuckDB。
	quoteStreamTask, err := builder.NewRealtimeTaskBuilder("quote_stream_processor", "实时行情流处理", b.registry).
		WithContinuousMode().
		WithTaskType(taskrealtime.TaskTypeStreamProcessor).
		WithJobFunction("RealtimeQuoteStreamHandler", map[string]interface{}{
			"target_db_path":   b.params.TargetDBPath,
			"data_source_name": b.params.DataSourceName,
			"api_name":         apiName,
		}).
		Build()
	if err != nil {
		return nil, fmt.Errorf("build quote stream processor task: %w", err)
	}

	// Streaming Workflow：注册 DataCollector，并挂载两个 RealtimeTask。
	wfBuilder := builder.NewWorkflowBuilder("realtime_market_sync", "实时行情 Streaming Workflow").
		WithStreamingMode().
		WithDataCollector(b.collectorName, b.collector).
		WithRealtimeTask(quoteCollectorTask).
		WithRealtimeTask(quoteStreamTask)

	wf, err := wfBuilder.Build()
	if err != nil {
		return nil, fmt.Errorf("build streaming workflow: %w", err)
	}
	return wf, nil
}

func (b *RealtimeMarketStreamingBuilder) validate() error {
	p := b.params
	if p.DataSourceName == "" || p.Token == "" || p.TargetDBPath == "" {
		return fmt.Errorf("realtime market streaming: data_source_name/token/target_db_path are required")
	}
	if len(p.APINames) == 0 {
		return fmt.Errorf("realtime market streaming: apis are required")
	}
	if b.collector == nil || b.collectorName == "" {
		return fmt.Errorf("realtime market streaming: collector and collectorName are required")
	}
	return nil
}

func (b *RealtimeMarketStreamingBuilder) effectivePullInterval() int {
	secs := b.params.PullIntervalSecs
	if secs <= 0 {
		secs = 60
	}
	return secs
}
