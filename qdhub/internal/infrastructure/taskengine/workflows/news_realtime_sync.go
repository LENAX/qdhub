// Package workflows provides built-in workflow definitions for QDHub.
// news_realtime_sync.go 实现新闻实时同步工作流：按 news_sync_checkpoint 增量拉取 Tushare 新闻快讯。
package workflows

import (
	"encoding/json"
	"log"

	"github.com/LENAX/task-engine/pkg/core/builder"
	"github.com/LENAX/task-engine/pkg/core/task"
	"github.com/LENAX/task-engine/pkg/core/workflow"
)

// 实时新闻仅拉取同花顺（10jqka），覆盖面广且延迟低
var newsSrcValues = []string{"cls", "sina", "wallstreetcn", "eastmoney"}

// NewsRealtimeSyncParams 新闻实时同步工作流参数
type NewsRealtimeSyncParams struct {
	DataSourceName string // 数据源名称（必填）
	Token          string // API Token（必填）
	TargetDBPath   string // 目标 DuckDB 路径（必填）
	// Freq 时间窗口步长，如 "1MIN"、"1H"、"D"，默认 "1H"
	Freq string
}

// Validate 验证参数
func (p *NewsRealtimeSyncParams) Validate() error {
	if p.DataSourceName == "" {
		return ErrEmptyDataSourceName
	}
	if p.Token == "" {
		return ErrEmptyToken
	}
	if p.TargetDBPath == "" {
		return ErrEmptyTargetDBPath
	}
	return nil
}

// NewsRealtimeSyncWorkflowBuilder 新闻实时同步工作流构建器
type NewsRealtimeSyncWorkflowBuilder struct {
	registry task.FunctionRegistry
	params   NewsRealtimeSyncParams
}

// NewNewsRealtimeSyncWorkflowBuilder 创建构建器
func NewNewsRealtimeSyncWorkflowBuilder(registry task.FunctionRegistry) *NewsRealtimeSyncWorkflowBuilder {
	return &NewsRealtimeSyncWorkflowBuilder{
		registry: registry,
		params:   NewsRealtimeSyncParams{Freq: "1H"},
	}
}

// WithParams 设置工作流参数
func (b *NewsRealtimeSyncWorkflowBuilder) WithParams(params NewsRealtimeSyncParams) *NewsRealtimeSyncWorkflowBuilder {
	b.params = params
	return b
}

// WithDataSource 设置数据源和 Token
func (b *NewsRealtimeSyncWorkflowBuilder) WithDataSource(name, token string) *NewsRealtimeSyncWorkflowBuilder {
	b.params.DataSourceName = name
	b.params.Token = token
	return b
}

// WithTargetDB 设置目标数据库路径
func (b *NewsRealtimeSyncWorkflowBuilder) WithTargetDB(path string) *NewsRealtimeSyncWorkflowBuilder {
	b.params.TargetDBPath = path
	return b
}

// WithFreq 设置时间窗口步长（如 "1MIN", "1H", "D"）
func (b *NewsRealtimeSyncWorkflowBuilder) WithFreq(freq string) *NewsRealtimeSyncWorkflowBuilder {
	b.params.Freq = freq
	return b
}

// Build 构建新闻实时同步工作流
//
// 流程：GetNewsSyncRange → GenerateNewsDatetimeRange → Sync_news（GenerateTimeWindowSubTasks）→ UpdateNewsCheckpoint → FlushTargetDB
// - GetNewsSyncRange 从 news_sync_checkpoint 读上次时间，输出 start_datetime/end_datetime
// - GenerateDatetimeRange 从 _cached_GetNewsSyncRange 取 start/end，按 freq 切窗口
// - Sync_news 按窗口 + src 生成 SyncAPIData 子任务
// - UpdateNewsCheckpoint 在 Sync_news 完成后写 end_datetime 到 checkpoint
// - FlushTargetDB 工作流完成后立刻将目标库 WriteQueue 缓冲刷盘到数据库
func (b *NewsRealtimeSyncWorkflowBuilder) Build() (*workflow.Workflow, error) {
	params := b.params

	usePlaceholders := params.DataSourceName == "" && params.Token == "" && params.TargetDBPath == ""
	if !usePlaceholders {
		if err := params.Validate(); err != nil {
			return nil, err
		}
	}

	dataSourceName := params.DataSourceName
	if dataSourceName == "" {
		dataSourceName = "${data_source_name}"
	}
	token := params.Token
	if token == "" {
		token = "${token}"
	}
	targetDBPath := params.TargetDBPath
	if targetDBPath == "" {
		targetDBPath = "${target_db_path}"
	}
	freq := params.Freq
	if freq == "" {
		freq = "1H"
	}

	baseParams := map[string]interface{}{
		"data_source_name": dataSourceName,
		"token":            token,
		"target_db_path":   targetDBPath,
	}

	var tasks []*task.Task

	// 1. GetNewsSyncRange：读 checkpoint，输出 start_datetime / end_datetime
	getRangeTask, err := builder.NewTaskBuilder("GetNewsSyncRange", "获取新闻同步时间范围", b.registry).
		WithJobFunction("GetNewsSyncRange", map[string]interface{}{
			"target_db_path":       targetDBPath,
			"force_backfill_check": "${force_backfill_check}",
		}).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		Build()
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, getRangeTask)

	// 2. GenerateNewsDatetimeRange：依赖 GetNewsSyncRange，start/end 由 job 内从 _cached_GetNewsSyncRange 读取
	rangeTaskName := "GenerateNewsDatetimeRange"
	rangeParams := map[string]interface{}{
		"freq":       freq,
		"inclusive":  "both",
		"as_windows": true,
	}
	rangeTask, err := builder.NewTaskBuilder(rangeTaskName, "生成新闻同步时间窗口", b.registry).
		WithJobFunction("GenerateDatetimeRange", rangeParams).
		WithDependency("GetNewsSyncRange").
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		Build()
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, rangeTask)

	// 3. Sync_news：GenerateTimeWindowSubTasks，api_name=news，按窗口+src 生成子任务
	templateParams := mergeParams(baseParams, map[string]interface{}{
		"api_name":      "news",
		"upstream_task": rangeTaskName,
		"window_field":  "windows",
		"max_sub_tasks": 0,
	})
	if svJSON, err := json.Marshal(newsSrcValues); err == nil {
		templateParams["src_values"] = string(svJSON)
	}
	syncNewsTask, err := builder.NewTaskBuilder("Sync_news", "同步新闻快讯（时间窗口+来源）", b.registry).
		WithJobFunction("GenerateTimeWindowSubTasks", templateParams).
		WithDependency(rangeTaskName).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		WithTemplate(true).
		WithCompensationFunction("CompensateSyncData").
		Build()
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, syncNewsTask)

	// 4. UpdateNewsCheckpoint：依赖 Sync_news，写 end_datetime 到 news_sync_checkpoint（从 _cached_GetNewsSyncRange 取 end_datetime）
	updateCheckpointTask, err := builder.NewTaskBuilder("UpdateNewsCheckpoint", "更新新闻同步检查点", b.registry).
		WithJobFunction("UpdateNewsCheckpoint", map[string]interface{}{
			"target_db_path": targetDBPath,
		}).
		WithDependency("Sync_news").
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		Build()
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, updateCheckpointTask)

	// 5. FlushTargetDB：工作流完成后立刻将目标库 WriteQueue 缓冲刷盘到数据库
	flushTask, err := builder.NewTaskBuilder("FlushTargetDB", "刷盘目标 DuckDB", b.registry).
		WithJobFunction("FlushTargetDB", map[string]interface{}{
			"target_db_path": targetDBPath,
		}).
		WithDependency("UpdateNewsCheckpoint").
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		Build()
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, flushTask)

	wfBuilder := builder.NewWorkflowBuilder("NewsRealtimeSync", "新闻实时同步：按 news_sync_checkpoint 增量拉取 Tushare 新闻快讯")
	for _, t := range tasks {
		wfBuilder.WithTask(t)
	}
	wf, err := wfBuilder.Build()
	if err != nil {
		return nil, err
	}
	log.Printf("✅ [NewsRealtimeSync] 工作流构建完成: %d 个任务", len(tasks))
	return wf, nil
}
