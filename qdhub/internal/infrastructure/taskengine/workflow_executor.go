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
