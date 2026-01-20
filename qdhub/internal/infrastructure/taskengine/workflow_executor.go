// Package taskengine provides Task Engine integration for QDHub.
package taskengine

import (
	"context"
	"fmt"

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
}

// NewWorkflowExecutor creates a new WorkflowExecutor implementation.
func NewWorkflowExecutor(
	workflowRepo workflow.WorkflowDefinitionRepository,
	taskEngineAdapter workflow.TaskEngineAdapter,
) workflow.WorkflowExecutor {
	return &WorkflowExecutorImpl{
		workflowRepo:      workflowRepo,
		taskEngineAdapter: taskEngineAdapter,
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
// Converts the typed request to params map and delegates to ExecuteBuiltInWorkflow.
func (e *WorkflowExecutorImpl) ExecuteBatchDataSync(ctx context.Context, req workflow.BatchDataSyncRequest) (shared.ID, error) {
	params := map[string]interface{}{
		"data_source_name": req.DataSourceName,
		"token":            req.Token,
		"target_db_path":   req.TargetDBPath,
		"start_date":       req.StartDate,
		"end_date":         req.EndDate,
		"api_names":        req.APINames,
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

	return e.ExecuteBuiltInWorkflow(ctx, workflows.BuiltInWorkflowNameBatchDataSync, params)
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
