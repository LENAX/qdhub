package workflows

import (
	"fmt"
	"time"

	"github.com/LENAX/task-engine/pkg/core/builder"
	taskrealtime "github.com/LENAX/task-engine/pkg/core/realtime"
	"github.com/LENAX/task-engine/pkg/core/task"
	"github.com/LENAX/task-engine/pkg/core/workflow"
)

// TushareWSStreamingParams defines params for tushare ws realtime workflow.
type TushareWSStreamingParams struct {
	DataSourceName string
	Token          string
	TargetDBPath   string
	APIName        string
}

// TushareWSStreamingBuilder builds SPMC streaming workflow for tushare ws tick.
type TushareWSStreamingBuilder struct {
	registry      task.FunctionRegistry
	params        TushareWSStreamingParams
	collectorName string
	collector     taskrealtime.DataCollector
}

func NewTushareWSStreamingBuilder(
	registry task.FunctionRegistry,
	params TushareWSStreamingParams,
	collectorName string,
	collector taskrealtime.DataCollector,
) *TushareWSStreamingBuilder {
	return &TushareWSStreamingBuilder{
		registry:      registry,
		params:        params,
		collectorName: collectorName,
		collector:     collector,
	}
}

func (b *TushareWSStreamingBuilder) Build() (*workflow.Workflow, error) {
	if b.params.DataSourceName == "" || b.params.Token == "" || b.params.TargetDBPath == "" {
		return nil, fmt.Errorf("tushare ws streaming: data_source_name/token/target_db_path are required")
	}
	if b.params.APIName == "" {
		return nil, fmt.Errorf("tushare ws streaming: api_name is required")
	}
	if b.collector == nil || b.collectorName == "" {
		return nil, fmt.Errorf("tushare ws streaming: collector and collectorName are required")
	}

	collectorTask, err := builder.NewRealtimeTaskBuilder("tushare_ws_collector", "tushare WS 实时采集", b.registry).
		WithContinuousMode().
		WithTaskType(taskrealtime.TaskTypeDataCollector).
		WithCollector(b.collectorName).
		WithMode(taskrealtime.CollectorModePush).
		WithEndpoint("wss://ws.tushare.pro/listening", "ws").
		WithJobFunction("RealtimeDataCollector", map[string]interface{}{}).
		Build()
	if err != nil {
		return nil, fmt.Errorf("build collector task: %w", err)
	}

	dbSinkTask, err := builder.NewRealtimeTaskBuilder("tushare_db_sink", "tushare 实时数据落库", b.registry).
		WithContinuousMode().
		WithTaskType(taskrealtime.TaskTypeStreamProcessor).
		WithSubscriberName("db_sink").
		WithBufferPolicyBlocking(2000).
		WithDataHandlerMaxRetries(3).
		WithFlushInterval(500 * time.Millisecond).
		WithJobFunction("TushareTickDBBatchWrite", map[string]interface{}{
			"target_db_path":   b.params.TargetDBPath,
			"data_source_name": b.params.DataSourceName,
			"api_name":         b.params.APIName,
		}).
		Build()
	if err != nil {
		return nil, fmt.Errorf("build db sink task: %w", err)
	}

	wf, err := builder.NewWorkflowBuilder("tushare_ws_realtime", "tushare WS 全市场 tick").
		WithStreamingMode().
		WithBroadcastEnabled(true).
		WithWalEnabled(false).
		WithDataCollector(b.collectorName, b.collector).
		WithRealtimeTask(collectorTask).
		WithRealtimeTask(dbSinkTask).
		Build()
	if err != nil {
		return nil, fmt.Errorf("build tushare ws streaming workflow: %w", err)
	}
	return wf, nil
}
