// Package workflows provides built-in workflow definitions for QDHub.
package workflows

import (
	"github.com/LENAX/task-engine/pkg/core/builder"
	"github.com/LENAX/task-engine/pkg/core/task"
	"github.com/LENAX/task-engine/pkg/core/workflow"
)

// CreateTablesParams 建表工作流参数
type CreateTablesParams struct {
	DataSourceID   string // 数据源 ID
	DataSourceName string // 数据源名称
	TargetDBPath   string // 目标数据库路径
	MaxTables      int    // 最大建表数量（0=不限制）
}

// CreateTablesWorkflowBuilder 建表工作流构建器
type CreateTablesWorkflowBuilder struct {
	registry task.FunctionRegistry
	params   CreateTablesParams
}

// NewCreateTablesWorkflowBuilder 创建建表工作流构建器
func NewCreateTablesWorkflowBuilder(registry task.FunctionRegistry) *CreateTablesWorkflowBuilder {
	return &CreateTablesWorkflowBuilder{
		registry: registry,
	}
}

// WithParams 设置工作流参数
func (b *CreateTablesWorkflowBuilder) WithParams(params CreateTablesParams) *CreateTablesWorkflowBuilder {
	b.params = params
	return b
}

// WithDataSource 设置数据源
func (b *CreateTablesWorkflowBuilder) WithDataSource(id, name string) *CreateTablesWorkflowBuilder {
	b.params.DataSourceID = id
	b.params.DataSourceName = name
	return b
}

// WithTargetDB 设置目标数据库路径
func (b *CreateTablesWorkflowBuilder) WithTargetDB(path string) *CreateTablesWorkflowBuilder {
	b.params.TargetDBPath = path
	return b
}

// WithMaxTables 设置最大建表数量
func (b *CreateTablesWorkflowBuilder) WithMaxTables(max int) *CreateTablesWorkflowBuilder {
	b.params.MaxTables = max
	return b
}

// Build 构建建表工作流
//
// 工作流结构：
// 1. CreateTables [模板任务] - 为每个 API 生成建表子任务
//
// 事务支持：启用 SAGA 事务，建表失败时自动删除已创建的表
func (b *CreateTablesWorkflowBuilder) Build() (*workflow.Workflow, error) {
	params := b.params

	// Task 1: 模板任务 - 批量创建数据表
	createTablesTask, err := builder.NewTaskBuilder("CreateTables", "批量创建数据表（模板任务）", b.registry).
		WithJobFunction("CreateTablesFromCatalog", map[string]interface{}{
			"data_source_id":   params.DataSourceID,
			"data_source_name": params.DataSourceName,
			"target_db_path":   params.TargetDBPath,
			"max_tables":       params.MaxTables,
		}).
		WithTaskHandler(task.TaskStatusSuccess, "TableCreationSuccess").
		WithTaskHandler(task.TaskStatusFailed, "TableCreationFailure").
		WithTemplate(true). // 标记为模板任务
		Build()
	if err != nil {
		return nil, err
	}

	// 构建工作流
	wf, err := builder.NewWorkflowBuilder("CreateTables", "建表工作流 - 根据 API 元数据创建数据表").
		WithTask(createTablesTask).
		Build()
	if err != nil {
		return nil, err
	}

	// 启用 SAGA 事务
	wf.SetTransactional(true)

	return wf, nil
}
