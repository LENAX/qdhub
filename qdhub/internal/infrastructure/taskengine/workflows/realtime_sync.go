// Package workflows provides built-in workflow definitions for QDHub.
//
// realtime_sync.go 实现实时数据同步的流式工作流构建：按 API sync strategy 生成多个 DataCollector 任务 + 一个 StreamProcessor，
// 与 batch_data_sync.go 分离，供 PlanMode=realtime 的 SyncPlan 使用。
package workflows

import (
	"context"
	"fmt"
	"strings"

	"qdhub/internal/domain/shared"

	"github.com/LENAX/task-engine/pkg/core/builder"
	"github.com/LENAX/task-engine/pkg/core/task"
	"github.com/LENAX/task-engine/pkg/core/workflow"
)

// RealtimeSyncParams 实时流式同步工作流参数
type RealtimeSyncParams struct {
	DataSourceID     shared.ID // 数据源 ID（用于 StrategyProvider 拉取策略）
	DataSourceName   string    // 数据源名称（必填）
	Token            string    // API Token（必填）
	TargetDBPath     string    // 目标数据库路径（必填）
	APINames         []string  // 需要同步的 API 列表（必填，应为白名单内 API）
	TsCodes          []string  // 股票代码列表（从 stock_basic 或前置步骤获取，按策略分片用）
	IndexCodes       []string  // 指数代码列表（从 index_basic 获取，供 rt_idx_min 等用）
	PullIntervalSecs int       // Pull 模式拉取间隔（秒），0 表示使用默认（如 60）
}

// RealtimeSyncWorkflowBuilder 实时流式同步工作流构建器
// 通过 StrategyProvider 从 DB 获取策略，按策略生成 DataCollector 任务 + 一个 RealtimeSyncDataHandler 任务
type RealtimeSyncWorkflowBuilder struct {
	registry         task.FunctionRegistry
	params           RealtimeSyncParams
	strategyProvider APISyncStrategyProvider
	strategyCache    map[string]*APISyncStrategy
}

// NewRealtimeSyncWorkflowBuilder 创建实时流式同步工作流构建器
func NewRealtimeSyncWorkflowBuilder(registry task.FunctionRegistry) *RealtimeSyncWorkflowBuilder {
	return &RealtimeSyncWorkflowBuilder{
		registry: registry,
		params:   RealtimeSyncParams{},
	}
}

// WithParams 设置工作流参数
func (b *RealtimeSyncWorkflowBuilder) WithParams(params RealtimeSyncParams) *RealtimeSyncWorkflowBuilder {
	b.params = params
	return b
}

// WithDataSource 设置数据源名称与 Token
func (b *RealtimeSyncWorkflowBuilder) WithDataSource(name, token string) *RealtimeSyncWorkflowBuilder {
	b.params.DataSourceName = name
	b.params.Token = token
	return b
}

// WithTargetDB 设置目标数据库路径
func (b *RealtimeSyncWorkflowBuilder) WithTargetDB(path string) *RealtimeSyncWorkflowBuilder {
	b.params.TargetDBPath = path
	return b
}

// WithAPIs 设置需要同步的 API 列表
func (b *RealtimeSyncWorkflowBuilder) WithAPIs(apis ...string) *RealtimeSyncWorkflowBuilder {
	b.params.APINames = apis
	return b
}

// WithTsCodes 设置股票代码列表（用于需要 ts_code 的 API 分片）
func (b *RealtimeSyncWorkflowBuilder) WithTsCodes(codes []string) *RealtimeSyncWorkflowBuilder {
	b.params.TsCodes = codes
	return b
}

// WithIndexCodes 设置指数代码列表（用于 rt_idx_min 等）
func (b *RealtimeSyncWorkflowBuilder) WithIndexCodes(codes []string) *RealtimeSyncWorkflowBuilder {
	b.params.IndexCodes = codes
	return b
}

// WithPullInterval 设置 Pull 模式拉取间隔（秒）
func (b *RealtimeSyncWorkflowBuilder) WithPullInterval(seconds int) *RealtimeSyncWorkflowBuilder {
	b.params.PullIntervalSecs = seconds
	return b
}

// WithStrategyProvider 设置策略提供者与数据源 ID
func (b *RealtimeSyncWorkflowBuilder) WithStrategyProvider(provider APISyncStrategyProvider, dataSourceID shared.ID) *RealtimeSyncWorkflowBuilder {
	b.strategyProvider = provider
	b.params.DataSourceID = dataSourceID
	return b
}

// Build 构建实时流式同步工作流
//
// 流程：对每个 API 通过 StrategyProvider 获取策略；按策略生成 DataCollector 任务（ts_code 分片或 IterateParams）；
// 最后添加 RealtimeSyncDataHandler（消费 buffer 落库）与 RealtimeCloseBuffer（依赖所有 Collector，关闭 buffer）。
func (b *RealtimeSyncWorkflowBuilder) Build() (*workflow.Workflow, error) {
	p := &b.params
	if err := b.validateParams(p); err != nil {
		return nil, err
	}
	if err := b.loadStrategies(context.Background(), p.APINames); err != nil {
		return nil, fmt.Errorf("load strategies: %w", err)
	}

	baseParams := b.buildBaseParams(p)
	tasks, collectorNames, err := b.buildAllCollectorTasks(p, baseParams)
	if err != nil {
		return nil, err
	}

	handlerTask, err := b.buildHandlerTask(p)
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, handlerTask)

	closeTask, err := b.buildCloseBufferTask(collectorNames)
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, closeTask)

	return b.assembleWorkflow(tasks)
}

func (b *RealtimeSyncWorkflowBuilder) validateParams(p *RealtimeSyncParams) error {
	if p.DataSourceName == "" || p.Token == "" || p.TargetDBPath == "" {
		return ErrEmptyDataSourceName
	}
	if len(p.APINames) == 0 {
		return ErrEmptyAPINames
	}
	return nil
}

func (b *RealtimeSyncWorkflowBuilder) buildBaseParams(p *RealtimeSyncParams) map[string]interface{} {
	base := map[string]interface{}{
		"data_source_name": p.DataSourceName,
		"token":            p.Token,
		"target_db_path":   p.TargetDBPath,
	}
	interval := p.PullIntervalSecs
	if interval <= 0 {
		interval = 60
	}
	base["pull_interval_seconds"] = interval
	return base
}

// buildAllCollectorTasks 为所有 APINames 按策略生成 DataCollector 任务，返回 tasks 与 collector 任务名列表
func (b *RealtimeSyncWorkflowBuilder) buildAllCollectorTasks(p *RealtimeSyncParams, baseParams map[string]interface{}) ([]*task.Task, []string, error) {
	var tasks []*task.Task
	var names []string
	for _, apiName := range p.APINames {
		apiTasks, apiNames, err := b.buildCollectorTasksForAPI(apiName, baseParams, p)
		if err != nil {
			return nil, nil, err
		}
		tasks = append(tasks, apiTasks...)
		names = append(names, apiNames...)
	}
	return tasks, names, nil
}

// buildCollectorTasksForAPI 为单个 API 按策略生成若干 DataCollector 任务（ts_code 分片 / IterateParams / 单任务）
func (b *RealtimeSyncWorkflowBuilder) buildCollectorTasksForAPI(apiName string, baseParams map[string]interface{}, p *RealtimeSyncParams) ([]*task.Task, []string, error) {
	strategy := b.getStrategy(apiName)

	if strategy.PreferredParam == "ts_code" && strategy.RealtimeTsCodeChunkSize > 0 {
		return b.buildCollectorTasksTsCodeChunks(apiName, strategy, baseParams, p)
	}
	if len(strategy.IterateParams) > 0 {
		return b.buildCollectorTasksIterateParams(apiName, strategy, baseParams)
	}
	return b.buildCollectorTaskSingle(apiName, baseParams)
}

func (b *RealtimeSyncWorkflowBuilder) buildCollectorTasksTsCodeChunks(apiName string, strategy *APISyncStrategy, baseParams map[string]interface{}, p *RealtimeSyncParams) ([]*task.Task, []string, error) {
	codes := b.resolveCodesForStrategy(strategy, p)
	if len(codes) == 0 {
		return nil, nil, nil
	}
	chunkSize := strategy.RealtimeTsCodeChunkSize
	if chunkSize <= 0 {
		chunkSize = 50
	}
	chunks := chunkStrings(codes, chunkSize)
	freq := ""
	if strategy.FixedParams != nil {
		if v, ok := strategy.FixedParams["freq"].(string); ok {
			freq = v
		}
	}

	var tasks []*task.Task
	var names []string
	for i, chunk := range chunks {
		taskName := fmt.Sprintf("RealtimeCollector_%s_%d", sanitizeTaskName(apiName), i)
		params := copyMap(baseParams)
		params["api_name"] = apiName
		params["ts_code"] = strings.Join(chunk, ",")
		if freq != "" {
			params["freq"] = freq
		}
		desc := fmt.Sprintf("实时采集 %s 分片 %d/%d", apiName, i+1, len(chunks))
		t, err := b.buildDataCollectorTask(taskName, desc, params)
		if err != nil {
			return nil, nil, fmt.Errorf("build task %s: %w", taskName, err)
		}
		tasks = append(tasks, t)
		names = append(names, taskName)
	}
	return tasks, names, nil
}

func (b *RealtimeSyncWorkflowBuilder) resolveCodesForStrategy(strategy *APISyncStrategy, p *RealtimeSyncParams) []string {
	for _, dep := range strategy.Dependencies {
		if dep == "FetchIndexBasic" {
			return p.IndexCodes
		}
		if dep == "FetchStockBasic" {
			return p.TsCodes
		}
	}
	return nil
}

func (b *RealtimeSyncWorkflowBuilder) buildCollectorTasksIterateParams(apiName string, strategy *APISyncStrategy, baseParams map[string]interface{}) ([]*task.Task, []string, error) {
	var tasks []*task.Task
	var names []string
	for paramKey, values := range strategy.IterateParams {
		for _, v := range values {
			taskName := fmt.Sprintf("RealtimeCollector_%s_%s_%s", sanitizeTaskName(apiName), paramKey, sanitizeTaskName(v))
			params := copyMap(baseParams)
			params["api_name"] = apiName
			params[paramKey] = v
			t, err := b.buildDataCollectorTask(taskName, "实时采集 "+apiName+" "+paramKey+"="+v, params)
			if err != nil {
				return nil, nil, fmt.Errorf("build task %s: %w", taskName, err)
			}
			tasks = append(tasks, t)
			names = append(names, taskName)
		}
	}
	return tasks, names, nil
}

func (b *RealtimeSyncWorkflowBuilder) buildCollectorTaskSingle(apiName string, baseParams map[string]interface{}) ([]*task.Task, []string, error) {
	taskName := fmt.Sprintf("RealtimeCollector_%s", sanitizeTaskName(apiName))
	params := copyMap(baseParams)
	params["api_name"] = apiName
	t, err := b.buildDataCollectorTask(taskName, "实时采集 "+apiName, params)
	if err != nil {
		return nil, nil, fmt.Errorf("build task %s: %w", taskName, err)
	}
	return []*task.Task{t}, []string{taskName}, nil
}

func (b *RealtimeSyncWorkflowBuilder) buildDataCollectorTask(taskName, description string, params map[string]interface{}) (*task.Task, error) {
	return builder.NewTaskBuilder(taskName, description, b.registry).
		WithJobFunction("RealtimeDataCollector", params).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		Build()
}

func (b *RealtimeSyncWorkflowBuilder) buildHandlerTask(p *RealtimeSyncParams) (*task.Task, error) {
	params := map[string]interface{}{
		"data_source_name": p.DataSourceName,
		"target_db_path":   p.TargetDBPath,
	}
	return builder.NewTaskBuilder("RealtimeSyncDataHandler", "实时数据落库", b.registry).
		WithJobFunction("RealtimeSyncDataHandler", params).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		Build()
}

func (b *RealtimeSyncWorkflowBuilder) buildCloseBufferTask(collectorTaskNames []string) (*task.Task, error) {
	taskBuilder := builder.NewTaskBuilder("RealtimeCloseBuffer", "关闭实时 buffer", b.registry).
		WithJobFunction("RealtimeCloseBuffer", map[string]interface{}{}).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure")
	if len(collectorTaskNames) > 0 {
		taskBuilder = taskBuilder.WithDependencies(collectorTaskNames)
	}
	return taskBuilder.Build()
}

func (b *RealtimeSyncWorkflowBuilder) assembleWorkflow(tasks []*task.Task) (*workflow.Workflow, error) {
	wfBuilder := builder.NewWorkflowBuilder("RealtimeSync", "实时数据同步流式工作流 - 按策略生成 DataCollector + StreamProcessor")
	for _, t := range tasks {
		wfBuilder.WithTask(t)
	}
	return wfBuilder.Build()
}

func (b *RealtimeSyncWorkflowBuilder) loadStrategies(ctx context.Context, apiNames []string) error {
	if b.strategyProvider == nil || b.params.DataSourceID.IsEmpty() {
		return nil
	}
	cache, err := b.strategyProvider.GetStrategies(ctx, b.params.DataSourceID, apiNames)
	if err != nil {
		return err
	}
	b.strategyCache = cache
	return nil
}

func (b *RealtimeSyncWorkflowBuilder) getStrategy(apiName string) *APISyncStrategy {
	s := GetAPISyncStrategyWithFallback(apiName, b.strategyCache)
	return &s
}

// chunkStrings 将 slice 按 size 分片
func chunkStrings(slice []string, size int) [][]string {
	if size <= 0 {
		return [][]string{slice}
	}
	var result [][]string
	for i := 0; i < len(slice); i += size {
		end := i + size
		if end > len(slice) {
			end = len(slice)
		}
		result = append(result, slice[i:end])
	}
	return result
}

func copyMap(m map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// sanitizeTaskName 将 API 名或参数值转为合法任务名（去掉点号等）
func sanitizeTaskName(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, ".", "_"), " ", "_")
}
