// Package taskengine provides Task Engine integration for QDHub.
package taskengine

import (
	"context"
	"fmt"

	"github.com/LENAX/task-engine/pkg/core/task"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/taskengine/workflows"
)

// WorkflowExecutorImpl implements workflow.WorkflowExecutor.
// This implementation executes built-in workflows by directly interacting with
// Task Engine and workflow repository, avoiding dependency on WorkflowApplicationService.
type WorkflowExecutorImpl struct {
	workflowRepo      workflow.WorkflowDefinitionRepository
	taskEngineAdapter workflow.TaskEngineAdapter
	metadataRepo      metadata.Repository
}

// NewWorkflowExecutor creates a new WorkflowExecutor implementation.
func NewWorkflowExecutor(
	workflowRepo workflow.WorkflowDefinitionRepository,
	taskEngineAdapter workflow.TaskEngineAdapter,
	metadataRepo metadata.Repository,
) workflow.WorkflowExecutor {
	return &WorkflowExecutorImpl{
		workflowRepo:      workflowRepo,
		taskEngineAdapter: taskEngineAdapter,
		metadataRepo:      metadataRepo,
	}
}

// ExecuteBuiltInWorkflow executes a built-in workflow by its API name.
func (e *WorkflowExecutorImpl) ExecuteBuiltInWorkflow(ctx context.Context, name string, params map[string]interface{}) (shared.ID, error) {
	// 1. Validate API name exists
	_, err := workflows.GetBuiltInWorkflowMetaByName(name)
	if err != nil {
		return "", fmt.Errorf("built-in workflow not found: %s", err)
	}

	// 2. Map API name to workflow builder name (English name used in builder)
	// The workflow name in DB is the English name from builder (e.g., "MetadataCrawl")
	builderNameMap := map[string]string{
		workflows.BuiltInWorkflowNameMetadataCrawl:    "MetadataCrawl",
		workflows.BuiltInWorkflowNameCreateTables:     "CreateTables",
		workflows.BuiltInWorkflowNameBatchDataSync:    "BatchDataSync",
		workflows.BuiltInWorkflowNameRealtimeDataSync: "RealtimeDataSync",
	}

	workflowName, ok := builderNameMap[name]
	if !ok {
		return "", fmt.Errorf("built-in workflow name mapping not found: %s", name)
	}

	// 3. Find workflow definition by English name (as stored by builder)
	defs, err := e.workflowRepo.FindBy(shared.Eq("name", workflowName))
	if err != nil {
		return "", fmt.Errorf("failed to find workflow by name: %w", err)
	}
	if len(defs) == 0 {
		return "", fmt.Errorf("built-in workflow '%s' not found in database. Please ensure built-in workflows are initialized", name)
	}
	if len(defs) > 1 {
		return "", fmt.Errorf("multiple workflows found with name '%s'", workflowName)
	}

	// 4. Submit workflow to Task Engine
	instanceID, err := e.taskEngineAdapter.SubmitWorkflow(ctx, defs[0], params)
	if err != nil {
		return "", fmt.Errorf("failed to submit workflow: %w", err)
	}

	return shared.ID(instanceID), nil
}

// ExecuteMetadataCrawl executes the metadata_crawl built-in workflow.
// Converts the typed request to params map and delegates to ExecuteBuiltInWorkflow.
func (e *WorkflowExecutorImpl) ExecuteMetadataCrawl(ctx context.Context, req workflow.MetadataCrawlRequest) (shared.ID, error) {
	params := map[string]interface{}{
		"data_source_id":   req.DataSourceID.String(),
		"data_source_name": req.DataSourceName,
		"max_api_crawl":    req.MaxAPICrawl, // 始终传递，0 表示不限制
	}

	return e.ExecuteBuiltInWorkflow(ctx, workflows.BuiltInWorkflowNameMetadataCrawl, params)
}

// ExecuteCreateTables executes the create_tables built-in workflow.
// Converts the typed request to params map and delegates to ExecuteBuiltInWorkflow.
func (e *WorkflowExecutorImpl) ExecuteCreateTables(ctx context.Context, req workflow.CreateTablesRequest) (shared.ID, error) {
	params := map[string]interface{}{
		"data_source_id":   req.DataSourceID.String(),
		"data_source_name": req.DataSourceName,
		"target_db_path":   req.TargetDBPath,
	}

	// Only add max_tables if it's set (non-zero means limit)
	if req.MaxTables > 0 {
		params["max_tables"] = req.MaxTables
	}

	return e.ExecuteBuiltInWorkflow(ctx, workflows.BuiltInWorkflowNameCreateTables, params)
}

// ExecuteBatchDataSync executes the batch_data_sync built-in workflow.
// IMPORTANT: Unlike other built-in workflows that use parameter replacement,
// BatchDataSync dynamically builds the workflow at execution time based on
// the provided API list or APIConfigs. When APIConfigs is set (e.g. from SyncPlan),
// it takes precedence over APINames and uses the plan's dependency and param config.
func (e *WorkflowExecutorImpl) ExecuteBatchDataSync(ctx context.Context, req workflow.BatchDataSyncRequest) (shared.ID, error) {
	// Validate required parameters
	if req.DataSourceName == "" {
		return "", workflows.ErrEmptyDataSourceName
	}
	if req.Token == "" {
		return "", workflows.ErrEmptyToken
	}
	if req.TargetDBPath == "" {
		return "", workflows.ErrEmptyTargetDBPath
	}
	// StartDate/EndDate 在仅同步无需日期参数的 API（如 stock_basic）时可为空，由调用方（如 ExecuteSyncPlan）根据计划 API 参数决定是否必填
	if len(req.APIConfigs) == 0 && len(req.APINames) == 0 {
		return "", workflows.ErrEmptyAPINames
	}

	// Get function registry from Task Engine adapter
	registryInterface := e.taskEngineAdapter.GetFunctionRegistry()
	registry, ok := registryInterface.(task.FunctionRegistry)
	if !ok {
		return "", fmt.Errorf("failed to get function registry: invalid type")
	}

	wfBuilder := workflows.NewBatchDataSyncWorkflowBuilder(registry).
		WithDataSource(req.DataSourceName, req.Token).
		WithTargetDB(req.TargetDBPath).
		WithDateRange(req.StartDate, req.EndDate).
		WithMaxStocks(req.MaxStocks)

	if len(req.APIConfigs) > 0 {
		// SyncPlan 路径：使用解析后的 API 配置（依赖与参数来源由计划决定）
		configs := make([]workflows.APISyncConfig, 0, len(req.APIConfigs))
		for _, c := range req.APIConfigs {
			configs = append(configs, workflows.APISyncConfig{
				APIName:        c.APIName,
				SyncMode:       c.SyncMode,
				ParamKey:       c.ParamKey,
				UpstreamTask:   c.UpstreamTask,
				UpstreamParams: c.UpstreamParams,
				ExtraParams:    c.ExtraParams,
				Dependencies:   c.Dependencies,
			})
		}
		wfBuilder.WithAPIConfigs(configs...)
	} else {
		wfBuilder.WithAPIs(req.APINames...)
	}

	if req.StartTime != "" || req.EndTime != "" {
		wfBuilder.WithTimeRange(req.StartTime, req.EndTime)
	}

	if len(req.CommonDataAPIs) > 0 {
		wfBuilder.WithCommonDataAPIs(req.CommonDataAPIs)
	}

	if !req.DataSourceID.IsEmpty() {
		provider := workflows.NewRepositoryStrategyProvider(e.metadataRepo)
		wfBuilder.WithStrategyProvider(provider, req.DataSourceID)
	}

	wf, err := wfBuilder.Build()
	if err != nil {
		return "", fmt.Errorf("failed to build batch data sync workflow: %w", err)
	}

	controller, err := e.taskEngineAdapter.SubmitDynamicWorkflow(ctx, wf)
	if err != nil {
		return "", fmt.Errorf("failed to submit workflow: %w", err)
	}

	return shared.ID(controller), nil
}

// ExecuteRealtimeDataSync executes the realtime_data_sync built-in workflow.
// Converts the typed request to params map and delegates to ExecuteBuiltInWorkflow.
func (e *WorkflowExecutorImpl) ExecuteRealtimeDataSync(ctx context.Context, req workflow.RealtimeDataSyncRequest) (shared.ID, error) {
	params := map[string]interface{}{
		"data_source_name": req.DataSourceName,
		"token":            req.Token,
		"target_db_path":   req.TargetDBPath,
		"api_names":        req.APINames,
	}

	// Set checkpoint table with default value
	checkpointTable := req.CheckpointTable
	if checkpointTable == "" {
		checkpointTable = "sync_checkpoint"
	}
	params["checkpoint_table"] = checkpointTable

	// Add optional cron expression if set
	if req.CronExpr != "" {
		params["cron_expr"] = req.CronExpr
	}

	// Only add max_stocks if it's set (non-zero means limit)
	if req.MaxStocks > 0 {
		params["max_stocks"] = req.MaxStocks
	}

	return e.ExecuteBuiltInWorkflow(ctx, workflows.BuiltInWorkflowNameRealtimeDataSync, params)
}

// ExecuteFromExecutionGraph executes a data sync workflow based on ExecutionGraph.
// This is the primary method for SyncPlan execution.
func (e *WorkflowExecutorImpl) ExecuteFromExecutionGraph(ctx context.Context, req workflow.ExecutionGraphSyncRequest) (shared.ID, error) {
	// 从 interface{} 转换为具体类型（避免循环依赖）
	// 实际上 ExecutionGraph 应该在 sync 包中定义
	// 这里使用 map[string]interface{} 作为中间格式

	// 构建工作流参数
	params := map[string]interface{}{
		"data_source_name": req.DataSourceName,
		"token":            req.Token,
		"target_db_path":   req.TargetDBPath,
		"start_date":       req.StartDate,
		"end_date":         req.EndDate,
		"api_names":        req.SyncedAPIs,
		"skipped_apis":     req.SkippedAPIs,
	}

	// Add optional time parameters if set
	if req.StartTime != "" {
		params["start_time"] = req.StartTime
	}
	if req.EndTime != "" {
		params["end_time"] = req.EndTime
	}

	// Only add max_stocks if it's set (non-zero means limit)
	if req.MaxStocks > 0 {
		params["max_stocks"] = req.MaxStocks
	}

	// 使用标准的 BatchDataSync 工作流（因为 ExecutionGraph 已经在应用层处理）
	// 工作流需要的是最终的 API 列表，ExecutionGraph 的作用是在应用层确定这些 API
	return e.ExecuteBuiltInWorkflow(ctx, workflows.BuiltInWorkflowNameBatchDataSync, params)
}
