// Package workflows provides built-in workflow definitions for QDHub.
package workflows

import (
	"fmt"

	"github.com/LENAX/task-engine/pkg/core/builder"
	"github.com/LENAX/task-engine/pkg/core/task"
	"github.com/LENAX/task-engine/pkg/core/workflow"
)

// MetadataCrawlParams 元数据爬取工作流参数
type MetadataCrawlParams struct {
	DataSourceID   string // 数据源 ID
	DataSourceName string // 数据源名称 (e.g., "tushare")
	MaxAPICrawl    int    // 最大爬取 API 数量（0=不限制）
}

// MetadataCrawlWorkflowBuilder 元数据爬取工作流构建器
type MetadataCrawlWorkflowBuilder struct {
	registry task.FunctionRegistry
	params   MetadataCrawlParams
}

// NewMetadataCrawlWorkflowBuilder 创建元数据爬取工作流构建器
func NewMetadataCrawlWorkflowBuilder(registry task.FunctionRegistry) *MetadataCrawlWorkflowBuilder {
	return &MetadataCrawlWorkflowBuilder{
		registry: registry,
	}
}

// WithParams 设置工作流参数
func (b *MetadataCrawlWorkflowBuilder) WithParams(params MetadataCrawlParams) *MetadataCrawlWorkflowBuilder {
	b.params = params
	return b
}

// WithDataSource 设置数据源
func (b *MetadataCrawlWorkflowBuilder) WithDataSource(id, name string) *MetadataCrawlWorkflowBuilder {
	b.params.DataSourceID = id
	b.params.DataSourceName = name
	return b
}

// WithMaxAPICrawl 设置最大爬取数量
func (b *MetadataCrawlWorkflowBuilder) WithMaxAPICrawl(max int) *MetadataCrawlWorkflowBuilder {
	b.params.MaxAPICrawl = max
	return b
}

// Build 构建元数据爬取工作流
//
// 工作流结构：
// 1. FetchCatalog - 获取 API 目录页面
// 2. ParseCatalog - 解析目录结构，提取 API URLs
// 3. SaveCategories - 保存分类信息（带事务补偿）
// 4. FetchAPIDetails [模板任务] - 为每个 API URL 生成爬取子任务
// 5. SaveMetadata - 保存 API 元数据（带事务补偿）
//
// 事务支持：启用 SAGA 事务，任务失败时自动回滚已保存的数据
//
// 参数占位符支持：如果参数为空，将使用占位符（如 ${data_source_id}），
// 执行时通过 workflow.ReplaceParams() 替换为实际值
func (b *MetadataCrawlWorkflowBuilder) Build() (*workflow.Workflow, error) {
	params := b.params

	// 如果参数为空，使用占位符
	dataSourceID := params.DataSourceID
	if dataSourceID == "" {
		dataSourceID = "${data_source_id}"
	}
	dataSourceName := params.DataSourceName
	if dataSourceName == "" {
		dataSourceName = "${data_source_name}"
	}

	// Task 1: 获取目录页面
	fetchCatalogTask, err := builder.NewTaskBuilder("FetchCatalog", "获取数据源 API 目录页面", b.registry).
		WithJobFunction("FetchCatalog", map[string]interface{}{
			"data_source_id":   dataSourceID,
			"data_source_name": dataSourceName,
		}).
		WithTaskHandler(task.TaskStatusSuccess, "MetadataRefreshSuccess").
		WithTaskHandler(task.TaskStatusFailed, "MetadataRefreshFailure").
		Build()
	if err != nil {
		return nil, err
	}

	// Task 2: 解析目录结构
	parseCatalogTask, err := builder.NewTaskBuilder("ParseCatalog", "解析 API 目录结构", b.registry).
		WithJobFunction("ParseCatalog", map[string]interface{}{
			"data_source_id":   dataSourceID,
			"data_source_name": dataSourceName,
		}).
		WithDependency("FetchCatalog").
		WithTaskHandler(task.TaskStatusSuccess, "MetadataRefreshSuccess").
		WithTaskHandler(task.TaskStatusFailed, "MetadataRefreshFailure").
		Build()
	if err != nil {
		return nil, err
	}

	// Task 3: 保存分类信息（带补偿）
	saveCategoriesTask, err := builder.NewTaskBuilder("SaveCategories", "保存 API 分类信息", b.registry).
		WithJobFunction("SaveCategories", map[string]interface{}{
			"data_source_id": dataSourceID,
		}).
		WithDependency("ParseCatalog").
		WithTaskHandler(task.TaskStatusSuccess, "MetadataRefreshSuccess").
		WithTaskHandler(task.TaskStatusFailed, "MetadataRefreshFailure").
		WithCompensationFunction("CompensateSaveCategories"). // SAGA 补偿
		Build()
	if err != nil {
		return nil, err
	}

	// Task 4: 模板任务 - 动态生成 API 详情爬取子任务
	// max_api_crawl 使用占位符，执行时通过 ReplaceParams 替换
	// 默认值为 0（不限制），可在执行时通过 params 覆盖
	maxAPICrawl := "${max_api_crawl}"
	if params.MaxAPICrawl > 0 {
		// 如果构建时指定了值，使用具体值而非占位符
		maxAPICrawl = fmt.Sprintf("%d", params.MaxAPICrawl)
	}
	fetchAPIDetailsTask, err := builder.NewTaskBuilder("FetchAPIDetails", "爬取 API 详情（模板任务）", b.registry).
		WithJobFunction("GenerateAPIDetailFetchSubTasks", map[string]interface{}{
			"data_source_id":   dataSourceID,
			"data_source_name": dataSourceName,
			"max_api_crawl":    maxAPICrawl,
		}).
		WithDependency("ParseCatalog").
		WithTaskHandler(task.TaskStatusSuccess, "MetadataRefreshSuccess").
		WithTaskHandler(task.TaskStatusFailed, "MetadataRefreshFailure").
		WithTemplate(true). // 标记为模板任务
		Build()
	if err != nil {
		return nil, err
	}

	// Task 5: 保存 API 元数据（等待所有子任务完成后执行）
	// Task Engine 在触发 SaveAllMetadata 时，须向下游注入模板任务 FetchAPIDetails 的子任务执行结果，
	// 格式为 _cached_<FetchAPIDetails> 含 subtask_results（或 sub_tasks）: []{ result: { api_metadata: map } }，
	// 否则 SaveAPIMetadataBatch 无法拿到 api_metadata 会报 no api_metadata from upstream。
	saveMetadataTask, err := builder.NewTaskBuilder("SaveAllMetadata", "保存所有 API 元数据", b.registry).
		WithJobFunction("SaveAPIMetadataBatch", map[string]interface{}{
			"data_source_id": dataSourceID,
		}).
		WithDependency("FetchAPIDetails"). // 依赖模板任务（须等所有子任务完成，并注入 subtask_results）
		WithDependency("SaveCategories").
		WithTaskHandler(task.TaskStatusSuccess, "MetadataRefreshComplete").
		WithTaskHandler(task.TaskStatusFailed, "MetadataRefreshFailure").
		WithCompensationFunction("CompensateSaveAPIMetadataBatch"). // SAGA 补偿
		Build()
	if err != nil {
		return nil, err
	}

	// 构建工作流
	wf, err := builder.NewWorkflowBuilder("MetadataCrawl", "元数据爬取工作流 - 从数据源爬取 API 文档并保存元数据").
		WithTask(fetchCatalogTask).
		WithTask(parseCatalogTask).
		WithTask(saveCategoriesTask).
		WithTask(fetchAPIDetailsTask).
		WithTask(saveMetadataTask).
		Build()
	if err != nil {
		return nil, err
	}

	// 启用 SAGA 事务
	wf.SetTransactional(true)

	return wf, nil
}
