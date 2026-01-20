// Package impl contains application service implementations.
package impl

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/taskengine/workflows"
)

// BuiltInWorkflowInitializer 负责初始化内建workflow
type BuiltInWorkflowInitializer struct {
	workflowDefRepo   workflow.WorkflowDefinitionRepository
	workflowFactory   *workflows.WorkflowFactory
	taskEngineAdapter workflow.TaskEngineAdapter
}

// NewBuiltInWorkflowInitializer 创建初始化器
func NewBuiltInWorkflowInitializer(
	workflowDefRepo workflow.WorkflowDefinitionRepository,
	workflowFactory *workflows.WorkflowFactory,
	taskEngineAdapter workflow.TaskEngineAdapter,
) *BuiltInWorkflowInitializer {
	return &BuiltInWorkflowInitializer{
		workflowDefRepo:   workflowDefRepo,
		workflowFactory:   workflowFactory,
		taskEngineAdapter: taskEngineAdapter,
	}
}

// Initialize 检查并初始化所有内建workflow
func (i *BuiltInWorkflowInitializer) Initialize(ctx context.Context) error {
	builtInWorkflows := workflows.GetBuiltInWorkflows()
	var errors []error

	for _, meta := range builtInWorkflows {
		if err := i.initializeWorkflow(ctx, meta); err != nil {
			log.Printf("Failed to initialize built-in workflow %s: %v", meta.ID, err)
			errors = append(errors, fmt.Errorf("workflow %s: %w", meta.ID, err))
			// 继续初始化其他workflow，不因单个失败而停止
			continue
		}
		log.Printf("Successfully initialized built-in workflow: %s (%s)", meta.Name, meta.ID)
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to initialize %d built-in workflows: %v", len(errors), errors)
	}

	return nil
}

// initializeWorkflow 初始化单个内建workflow
func (i *BuiltInWorkflowInitializer) initializeWorkflow(ctx context.Context, meta workflows.BuiltInWorkflowMeta) error {
	// 1. 检查是否已存在
	existing, err := i.workflowDefRepo.Get(meta.ID)
	if err != nil && !shared.IsNotFoundError(err) {
		return fmt.Errorf("failed to check existing workflow: %w", err)
	}

	// 2. 如果已存在，先删除旧的工作流和任务定义，确保重新创建时数据一致
	if existing != nil {
		log.Printf("Built-in workflow %s already exists, deleting and recreating", meta.ID)
		if err := i.workflowDefRepo.Delete(meta.ID); err != nil {
			log.Printf("Warning: failed to delete existing workflow %s: %v", meta.ID, err)
			// 继续执行，尝试覆盖
		}
	}

	// 3. 使用占位符参数创建workflow
	wf, err := i.createWorkflowWithPlaceholders(meta)
	if err != nil {
		return fmt.Errorf("failed to create workflow: %w", err)
	}

	// 3.1. 设置固定的 workflow ID（内建workflow使用固定ID）
	wf.ID = meta.ID

	// 4. 创建 WorkflowDefinition
	// 将workflow序列化为YAML（可选，用于存储）
	definitionYAML := ""
	// 注意：由于内建workflow是代码定义的，DefinitionYAML可以留空或存储元数据
	definitionYAMLBytes, _ := json.Marshal(map[string]interface{}{
		"type":        "built-in",
		"workflow_id": meta.ID,
		"name":        meta.Name,
		"api_name":    meta.APIName,
		"category":    meta.Category,
	})
	definitionYAML = string(definitionYAMLBytes)

	// 映射category字符串到WfCategory
	var category workflow.WfCategory
	switch meta.Category {
	case "metadata":
		category = workflow.WfCategoryMetadata
	case "sync":
		category = workflow.WfCategorySync
	default:
		category = workflow.WfCategoryCustom
	}

	// 直接使用创建的 workflow，不创建新的 WorkflowDefinition
	// 这样可以确保 workflow 的 ID 和任务定义保持一致
	def := &workflow.WorkflowDefinition{
		Workflow:       wf,
		Category:       category,
		DefinitionYAML: definitionYAML,
		Version:        1,
		IsSystem:       true,
		UpdatedAt:      shared.Now(),
	}

	// 注意：此时 workflow 的 ID 可能还没有设置为 meta.ID
	// Task Engine 的 SaveWorkflow 会在保存时处理 ID
	// 但我们需要确保保存后，workflow 的 ID 是 meta.ID
	// 如果 workflow 已经有 ID 且不是 meta.ID，我们需要先设置它

	// 5. 先持久化到数据库（SaveWorkflow 会设置 workflow ID）
	// 这样可以确保 workflow 和任务定义一起保存，保持一致性
	if err := i.workflowDefRepo.Create(def); err != nil {
		return fmt.Errorf("failed to persist workflow: %w", err)
	}

	// 6. 注册到Task Engine（在保存后注册，确保 ID 一致）
	if err := i.taskEngineAdapter.RegisterWorkflow(ctx, def); err != nil {
		// 如果注册失败，尝试删除已保存的工作流
		_ = i.workflowDefRepo.Delete(meta.ID)
		return fmt.Errorf("failed to register workflow with task engine: %w", err)
	}
	log.Printf("Registered workflow definition (not executed): %s", meta.ID)

	// 7. 启用workflow
	def.Enable()
	if err := i.workflowDefRepo.Update(def); err != nil {
		return fmt.Errorf("failed to enable workflow: %w", err)
	}

	return nil
}

// createWorkflowWithPlaceholders 使用占位符参数创建workflow
func (i *BuiltInWorkflowInitializer) createWorkflowWithPlaceholders(meta workflows.BuiltInWorkflowMeta) (*workflow.Workflow, error) {
	switch meta.ID {
	case workflows.BuiltInWorkflowIDMetadataCrawl:
		// 使用占位符参数创建
		return i.workflowFactory.MetadataCrawl().
			WithDataSource("${data_source_id}", "${data_source_name}").
			WithMaxAPICrawl(0). // 使用默认值
			Build()

	case workflows.BuiltInWorkflowIDCreateTables:
		// 使用占位符参数创建
		return i.workflowFactory.CreateTables().
			WithDataSource("${data_source_id}", "${data_source_name}").
			WithTargetDB("${target_db_path}").
			WithMaxTables(0). // 使用默认值
			Build()

	case workflows.BuiltInWorkflowIDBatchDataSync:
		// 批量同步：使用空参数触发占位符模式
		// Build方法会检测到所有参数为空，自动使用占位符
		return i.workflowFactory.BatchDataSync().
			WithMaxStocks(0).
			Build()

	case workflows.BuiltInWorkflowIDRealtimeDataSync:
		// 实时同步：使用空参数触发占位符模式
		// 注意：需要显式清空CheckpointTable（因为构造函数有默认值）
		return i.workflowFactory.RealtimeDataSync().
			WithDataSource("", "").  // 清空默认值
			WithTargetDB("").        // 清空默认值
			WithCheckpointTable(""). // 清空默认值（重要！）
			WithAPIs().              // 空数组
			WithMaxStocks(0).
			Build()

	default:
		return nil, fmt.Errorf("unknown built-in workflow ID: %s", meta.ID)
	}
}
