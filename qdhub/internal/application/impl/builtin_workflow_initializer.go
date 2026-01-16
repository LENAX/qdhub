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

	// 2. 如果已存在，跳过创建（可以后续添加版本检查逻辑）
	if existing != nil {
		log.Printf("Built-in workflow %s already exists, skipping creation", meta.ID)
		return nil
	}

	// 3. 使用占位符参数创建workflow
	wf, err := i.createWorkflowWithPlaceholders(meta)
	if err != nil {
		return fmt.Errorf("failed to create workflow: %w", err)
	}

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

	def := workflow.NewWorkflowDefinition(
		meta.Name,
		meta.Description,
		category,
		definitionYAML,
		true, // IsSystem = true
	)

	// 设置workflow ID为固定ID
	// Note: Task Engine Workflow's ID is typically set when the workflow is saved.
	// We need to ensure the workflow has the correct ID before saving.
	// The repository's SaveWorkflow method should use the workflow's GetID() method.
	// If the workflow doesn't have an ID yet, we may need to set it through reflection
	// or modify the repository to accept an ID parameter.
	// For now, we'll rely on the repository to handle ID assignment.
	def.Workflow = wf

	// 5. 注册到Task Engine
	if err := i.taskEngineAdapter.RegisterWorkflow(ctx, def); err != nil {
		return fmt.Errorf("failed to register workflow with task engine: %w", err)
	}

	// 6. 持久化到数据库
	if err := i.workflowDefRepo.Create(def); err != nil {
		// 尝试从Task Engine取消注册
		_ = i.taskEngineAdapter.UnregisterWorkflow(ctx, meta.ID)
		return fmt.Errorf("failed to persist workflow: %w", err)
	}

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
		// 批量同步需要更多参数，使用占位符
		// 注意：APINames是数组，这里使用空数组，执行时通过参数替换处理
		return i.workflowFactory.BatchDataSync().
			WithDataSource("${data_source_name}", "${token}").
			WithTargetDB("${target_db_path}").
			WithDateRange("${start_date}", "${end_date}"). // WithDateRange takes 2 parameters: startDate, endDate
			WithAPIs().                                    // 空数组，执行时通过参数替换
			WithMaxStocks(0).
			Build()

	case workflows.BuiltInWorkflowIDRealtimeDataSync:
		// 实时同步使用占位符参数
		return i.workflowFactory.RealtimeDataSync().
			WithDataSource("${data_source_name}", "${token}").
			WithTargetDB("${target_db_path}").
			WithCheckpointTable("${checkpoint_table}").
			WithAPIs(). // 空数组，执行时通过参数替换
			WithMaxStocks(0).
			Build()

	default:
		return nil, fmt.Errorf("unknown built-in workflow ID: %s", meta.ID)
	}
}
