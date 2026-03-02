// Package workflows provides built-in workflow definitions for QDHub.
// These workflows are defined in code to support complex logic like
// template tasks and dynamic sub-task generation.
//
// Available workflows:
//   - MetadataCrawlWorkflow: 元数据爬取工作流
//   - CreateTablesWorkflow: 建表工作流
//   - BatchDataSyncWorkflow: 批量数据同步工作流
//   - RealtimeDataSyncWorkflow: 增量实时同步工作流
package workflows

import (
	"github.com/LENAX/task-engine/pkg/core/task"
	"github.com/LENAX/task-engine/pkg/core/workflow"
)

// WorkflowFactory 工作流工厂
// 提供创建内建工作流的便捷方法
type WorkflowFactory struct {
	registry task.FunctionRegistry
}

// NewWorkflowFactory 创建工作流工厂
func NewWorkflowFactory(registry task.FunctionRegistry) *WorkflowFactory {
	return &WorkflowFactory{
		registry: registry,
	}
}

// ==================== 工作流创建方法 ====================

// CreateMetadataCrawlWorkflow 创建元数据爬取工作流
// 参见 MetadataCrawlWorkflowBuilder 了解详细的工作流结构
func (f *WorkflowFactory) CreateMetadataCrawlWorkflow(params MetadataCrawlParams) (*workflow.Workflow, error) {
	return NewMetadataCrawlWorkflowBuilder(f.registry).
		WithParams(params).
		Build()
}

// CreateTablesWorkflow 创建建表工作流
// 参见 CreateTablesWorkflowBuilder 了解详细的工作流结构
func (f *WorkflowFactory) CreateTablesWorkflow(params CreateTablesParams) (*workflow.Workflow, error) {
	return NewCreateTablesWorkflowBuilder(f.registry).
		WithParams(params).
		Build()
}

// CreateBatchDataSyncWorkflow 创建批量数据同步工作流
// 参见 BatchDataSyncWorkflowBuilder 了解详细的工作流结构
func (f *WorkflowFactory) CreateBatchDataSyncWorkflow(params BatchDataSyncParams) (*workflow.Workflow, error) {
	return NewBatchDataSyncWorkflowBuilder(f.registry).
		WithParams(params).
		Build()
}

// CreateRealtimeDataSyncWorkflow 创建增量实时同步工作流
// 参见 RealtimeDataSyncWorkflowBuilder 了解详细的工作流结构
func (f *WorkflowFactory) CreateRealtimeDataSyncWorkflow(params RealtimeDataSyncParams) (*workflow.Workflow, error) {
	return NewRealtimeDataSyncWorkflowBuilder(f.registry).
		WithParams(params).
		Build()
}

// ==================== 直接使用 Builder 的便捷方法 ====================

// MetadataCrawl 返回元数据爬取工作流构建器
func (f *WorkflowFactory) MetadataCrawl() *MetadataCrawlWorkflowBuilder {
	return NewMetadataCrawlWorkflowBuilder(f.registry)
}

// CreateTables 返回建表工作流构建器
func (f *WorkflowFactory) CreateTables() *CreateTablesWorkflowBuilder {
	return NewCreateTablesWorkflowBuilder(f.registry)
}

// BatchDataSync 返回批量数据同步工作流构建器
func (f *WorkflowFactory) BatchDataSync() *BatchDataSyncWorkflowBuilder {
	return NewBatchDataSyncWorkflowBuilder(f.registry)
}

// RealtimeDataSync 返回增量实时同步工作流构建器
func (f *WorkflowFactory) RealtimeDataSync() *RealtimeDataSyncWorkflowBuilder {
	return NewRealtimeDataSyncWorkflowBuilder(f.registry)
}
