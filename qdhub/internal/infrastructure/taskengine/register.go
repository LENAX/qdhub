// Package taskengine provides Task Engine integration for QDHub.
package taskengine

import (
	"context"
	"fmt"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/core/task"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/sync"
	"qdhub/internal/infrastructure/datasource"
	"qdhub/internal/infrastructure/datasource/tushare/realtime"
	"qdhub/internal/infrastructure/realtimebuffer"
	"qdhub/internal/infrastructure/taskengine/handlers"
	"qdhub/internal/infrastructure/taskengine/jobs"
	"qdhub/internal/infrastructure/taskengine/workflows"
)

// Dependencies holds the dependencies for task engine jobs.
type Dependencies struct {
	// DataSourceRegistry is the data source adapter registry.
	DataSourceRegistry *datasource.Registry
	// MetadataRepo is the metadata repository (required for compensation handlers).
	MetadataRepo metadata.Repository
	// DataStoreRepo is the data store repository (required for table creation jobs).
	DataStoreRepo datastore.QuantDataStoreRepository
	// QuantDB is the quant database adapter (optional, deprecated: use QuantDBFactory + target_db_path).
	QuantDB datastore.QuantDB
	// QuantDBFactory creates QuantDB by path; sync/table jobs use this with target_db_path from data store.
	QuantDBFactory datastore.QuantDBFactory
	// SyncCallbackInvoker 可选；DataSyncCompleteHandler 用于触发 execution 回调（Plan.MarkCompleted）。
	// 需实现 HandleExecutionCallbackByWorkflowInstance(ctx, workflowInstID, success, recordCount, errMsg).
	SyncCallbackInvoker interface{}
	// CommonDataCache 可选；SyncAPIDataJob 用于公共数据 Cache→DuckDB→API 复用，未设置则不走缓存。
	CommonDataCache sync.CommonDataCache

	// RealtimeAdapterRegistry 实时行情 Adapter 注册表（sina/eastmoney 等），RealtimeDataCollectorJob 使用。
	RealtimeAdapterRegistry realtime.RealtimeAdapterRegistry
	// RealtimeBufferRegistry 实时同步 buffer 按实例 ID 管理，Collector Push、Handler 消费。
	RealtimeBufferRegistry realtimebuffer.Registry
}

// RegisterJobFunctions registers all job functions with the engine.
func RegisterJobFunctions(ctx context.Context, eng *engine.Engine) error {
	registry := eng.GetRegistry()

	// Job function registrations organized by domain
	jobFunctions := []struct {
		name        string
		fn          func(*task.TaskContext) (interface{}, error)
		description string
	}{
		// ==================== 元数据刷新工作流 Jobs ====================
		{"FetchCatalog", jobs.FetchCatalogJob, "从数据源获取 API 目录"},
		{"ParseCatalog", jobs.ParseCatalogJob, "解析 API 目录结构"},
		{"SaveCategories", jobs.SaveCategoriesJob, "保存 API 分类"},
		{"FetchAPIDetail", jobs.FetchAPIDetailJob, "获取 API 详情页"},
		{"ParseAPIDetail", jobs.ParseAPIDetailJob, "解析 API 详情"},
		{"FetchAndParseAPIDetail", jobs.FetchAndParseAPIDetailJob, "获取并解析 API 详情（组合函数）"},
		{"SaveAPIMetadata", jobs.SaveAPIMetadataJob, "保存 API 元数据"},
		{"SaveAPIMetadataBatch", jobs.SaveAPIMetadataBatchJob, "批量保存 API 元数据"},
		// 模板任务 Job Functions（元数据）
		{"GenerateAPIDetailFetchSubTasks", jobs.GenerateAPIDetailFetchSubTasksJob, "生成 API 详情爬取子任务（模板任务）"},
		{"GenerateAPIParseSubTasks", jobs.GenerateAPIParseSubTasksJob, "生成 API 详情解析子任务（模板任务）"},

		// ==================== 数据源 Jobs ====================
		{"QueryData", jobs.QueryDataJob, "查询数据源 API"},
		{"ValidateToken", jobs.ValidateTokenJob, "验证数据源 Token"},
		{"TestConnection", jobs.TestConnectionJob, "测试数据源连接"},

		// ==================== 数据同步 Jobs ====================
		{"SyncAPIData", jobs.SyncAPIDataJob, "同步单个 API 数据"},
		{"SyncMultiParamAPIData", jobs.SyncMultiParamAPIDataJob, "多参数迭代同步（如 index_basic 多市场）"},
		{"GenerateDataSyncSubTasks", jobs.GenerateDataSyncSubTasksJob, "生成数据同步子任务（模板任务）"},
		{"GenerateDatetimeRange", jobs.GenerateDatetimeRangeJob, "生成时间序列（类似 pandas.date_range）"},
		{"GenerateTimeWindowSubTasks", jobs.GenerateTimeWindowSubTasksJob, "根据时间窗口和来源生成 SyncAPIData 子任务（模板任务）"},
		{"DeleteSyncedData", jobs.DeleteSyncedDataJob, "删除同步的数据（用于回滚）"},
		{"NotifySyncComplete", jobs.NotifySyncCompleteJob, "批量同步完成占位（回调在 Handler 中执行）"},

		// ==================== 增量实时同步 Jobs ====================
		{"GetSyncCheckpoint", jobs.GetSyncCheckpointJob, "获取同步检查点"},
		{"GetSyncRangeFromTarget", jobs.GetSyncRangeFromTargetJob, "从目标库表+日期列计算同步起始日（不依赖 checkpoint）"},
		{"FetchLatestTradingDate", jobs.FetchLatestTradingDateJob, "获取最新交易日"},
		{"GenerateIncrementalSyncSubTasks", jobs.GenerateIncrementalSyncSubTasksJob, "生成增量同步子任务（模板任务）"},
		{"UpdateSyncCheckpoint", jobs.UpdateSyncCheckpointJob, "更新同步检查点"},

		// ==================== 实时流式同步 Jobs ====================
		{"RealtimeDataCollector", jobs.RealtimeDataCollectorJob, "实时数据采集（Pull 单次 Fetch，Push 到 buffer）"},
		{"RealtimeSyncDataHandler", jobs.RealtimeSyncDataHandlerJob, "实时数据落库（从 buffer 消费）"},
		{"RealtimeCloseBuffer", jobs.RealtimeCloseBufferJob, "关闭实时 buffer（所有 Collector 完成后调用）"},
		{"RealtimeQuoteStreamHandler", jobs.RealtimeQuoteStreamHandlerJob, "Streaming 模式实时行情流处理（从 DataBuffer Pop 的 data 落库）"},

		// ==================== 建表 Jobs ====================
		{"CreateTableFromMetadata", jobs.CreateTableFromMetadataJob, "根据 Metadata 创建数据表"},
		{"CreateTablesFromCatalog", jobs.CreateTablesFromCatalogJob, "批量创建数据表（模板任务）"},
		{"DropTable", jobs.DropTableJob, "删除数据表（用于回滚）"},
	}

	for _, jf := range jobFunctions {
		_, err := registry.Register(ctx, jf.name, jf.fn, jf.description)
		if err != nil {
			return fmt.Errorf("failed to register job function %s: %w", jf.name, err)
		}
	}

	return nil
}

// RegisterTaskHandlers registers all task handlers with the engine.
func RegisterTaskHandlers(ctx context.Context, eng *engine.Engine) error {
	registry := eng.GetRegistry()

	// Handler registrations organized by domain
	taskHandlers := []struct {
		name        string
		handler     func(*task.TaskContext)
		description string
	}{
		// ==================== 元数据刷新 Handlers ====================
		{"MetadataRefreshStart", handlers.MetadataRefreshStartHandler, "元数据刷新工作流开始"},
		{"MetadataRefreshSuccess", handlers.MetadataRefreshSuccessHandler, "元数据刷新任务成功"},
		{"MetadataRefreshFailure", handlers.MetadataRefreshFailureHandler, "元数据刷新任务失败"},
		{"MetadataRefreshComplete", handlers.MetadataRefreshCompleteHandler, "元数据刷新工作流完成"},

		// ==================== 数据源 Handlers ====================
		{"TokenValidationSuccess", handlers.TokenValidationSuccessHandler, "Token 验证成功"},
		{"TokenValidationFailure", handlers.TokenValidationFailureHandler, "Token 验证失败"},
		{"DataSourceConnectSuccess", handlers.DataSourceConnectSuccessHandler, "数据源连接成功"},
		{"DataSourceConnectFailure", handlers.DataSourceConnectFailureHandler, "数据源连接失败"},

		// ==================== 数据同步 Handlers ====================
		{"DataSyncStart", handlers.DataSyncStartHandler, "数据同步工作流开始"},
		{"DataSyncSuccess", handlers.DataSyncSuccessHandler, "数据同步任务成功"},
		{"DataSyncFailure", handlers.DataSyncFailureHandler, "数据同步任务失败"},
		{"DataSyncComplete", handlers.DataSyncCompleteHandler, "数据同步工作流完成"},

		// ==================== 建表 Handlers ====================
		{"TableCreationStart", handlers.TableCreationStartHandler, "建表工作流开始"},
		{"TableCreationSuccess", handlers.TableCreationSuccessHandler, "建表任务成功"},
		{"TableCreationFailure", handlers.TableCreationFailureHandler, "建表任务失败"},
		{"TableCreationComplete", handlers.TableCreationCompleteHandler, "建表工作流完成"},

		// ==================== SAGA 补偿 Handlers ====================
		{"CompensateSaveCategories", handlers.CompensateSaveCategoriesHandler, "回滚分类保存"},
		{"CompensateSaveAPIMetadata", handlers.CompensateSaveAPIMetadataHandler, "回滚 API 元数据保存"},
		{"CompensateSaveAPIMetadataBatch", handlers.CompensateSaveAPIMetadataBatchHandler, "回滚批量 API 元数据保存"},
		{"CompensateCreateTable", handlers.CompensateCreateTableHandler, "回滚建表操作"},
		{"CompensateSyncData", handlers.CompensateSyncDataHandler, "回滚数据同步"},
		{"CompensateUpdateCheckpoint", handlers.CompensateUpdateCheckpointHandler, "回滚检查点更新"},
		{"CompensateGeneric", handlers.CompensateGenericHandler, "通用补偿处理"},

		// ==================== 通用 Handlers ====================
		{"LogProgress", handlers.LogProgressHandler, "记录任务进度"},
		{"LogError", handlers.LogErrorHandler, "记录错误日志"},
		{"LogSuccess", handlers.LogSuccessHandler, "记录任务成功"},
	}

	for _, h := range taskHandlers {
		// RegisterTaskHandler returns (handler, error) but we only check error
		_, err := registry.RegisterTaskHandler(ctx, h.name, h.handler, h.description)
		if err != nil {
			return fmt.Errorf("failed to register task handler %s: %w", h.name, err)
		}
	}

	return nil
}

// SetupDependencies sets up dependencies for job functions.
func SetupDependencies(eng *engine.Engine, deps *Dependencies) {
	registry := eng.GetRegistry()

	// Register data source registry as dependency
	if deps.DataSourceRegistry != nil {
		registry.RegisterDependencyWithKey("DataSourceRegistry", deps.DataSourceRegistry)
	}

	// Register metadata repository as dependency (required for compensation handlers)
	if deps.MetadataRepo != nil {
		registry.RegisterDependencyWithKey("MetadataRepo", deps.MetadataRepo)
	}

	// Register data store repository as dependency (required for table creation jobs)
	if deps.DataStoreRepo != nil {
		registry.RegisterDependencyWithKey("DataStoreRepo", deps.DataStoreRepo)
	}

	// Register QuantDB adapter as dependency (optional, backward compat)
	if deps.QuantDB != nil {
		registry.RegisterDependencyWithKey("QuantDB", deps.QuantDB)
	}
	// Register QuantDBFactory (sync/table jobs use this with target_db_path from data store)
	if deps.QuantDBFactory != nil {
		registry.RegisterDependencyWithKey("QuantDBFactory", deps.QuantDBFactory)
	}

	// Register SyncCallbackInvoker (optional; for DataSyncCompleteHandler → execution callback)
	if deps.SyncCallbackInvoker != nil {
		registry.RegisterDependencyWithKey("SyncCallbackInvoker", deps.SyncCallbackInvoker)
	}

	// Register CommonDataCache (optional; SyncAPIDataJob uses for cache-first reuse)
	if deps.CommonDataCache != nil {
		registry.RegisterDependencyWithKey("CommonDataCache", deps.CommonDataCache)
	}

	// RealtimeAdapterRegistry & RealtimeBufferRegistry (for realtime sync workflow)
	if deps.RealtimeAdapterRegistry != nil {
		registry.RegisterDependencyWithKey("RealtimeAdapterRegistry", deps.RealtimeAdapterRegistry)
	}
	if deps.RealtimeBufferRegistry != nil {
		registry.RegisterDependencyWithKey("RealtimeBufferRegistry", deps.RealtimeBufferRegistry)
	}

	// Register engine itself as dependency (for template tasks)
	registry.RegisterDependencyWithKey("Engine", eng)
}

// RegisterSyncCallbackInvoker 在 SyncSvc 创建后注册 execution 回调注入（DataSyncCompleteHandler 使用）。
// 应在 initApplicationServices 之后调用。
func RegisterSyncCallbackInvoker(eng *engine.Engine, invoker interface{}) {
	if eng == nil || invoker == nil {
		return
	}
	eng.GetRegistry().RegisterDependencyWithKey("SyncCallbackInvoker", invoker)
}

// Initialize initializes the task engine with all QDHub job functions and handlers.
func Initialize(ctx context.Context, eng *engine.Engine, deps *Dependencies) error {
	// Register job functions
	if err := RegisterJobFunctions(ctx, eng); err != nil {
		return fmt.Errorf("failed to register job functions: %w", err)
	}

	// Register task handlers
	if err := RegisterTaskHandlers(ctx, eng); err != nil {
		return fmt.Errorf("failed to register task handlers: %w", err)
	}

	// Setup dependencies
	SetupDependencies(eng, deps)

	return nil
}

// GetWorkflowFactory creates a WorkflowFactory for creating built-in workflows.
// Must be called after Initialize() to ensure all job functions are registered.
func GetWorkflowFactory(eng *engine.Engine) *workflows.WorkflowFactory {
	return workflows.NewWorkflowFactory(eng.GetRegistry())
}

// ==================== 内建工作流创建便捷方法 ====================

// CreateMetadataCrawlWorkflow creates a metadata crawl workflow.
// This workflow crawls API documentation and saves metadata with SAGA transaction support.
func CreateMetadataCrawlWorkflow(eng *engine.Engine, params workflows.MetadataCrawlParams) (*workflows.WorkflowFactory, error) {
	factory := GetWorkflowFactory(eng)
	_, err := factory.CreateMetadataCrawlWorkflow(params)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata crawl workflow: %w", err)
	}
	return factory, nil
}

// CreateTablesWorkflow creates a table creation workflow.
// This workflow creates data tables based on API metadata with SAGA transaction support.
func CreateTablesWorkflow(eng *engine.Engine, params workflows.CreateTablesParams) (*workflows.WorkflowFactory, error) {
	factory := GetWorkflowFactory(eng)
	_, err := factory.CreateTablesWorkflow(params)
	if err != nil {
		return nil, fmt.Errorf("failed to create tables workflow: %w", err)
	}
	return factory, nil
}

// CreateBatchDataSyncWorkflow creates a batch data sync workflow.
// This workflow syncs data with user-specified date range and API list, with SAGA transaction support.
func CreateBatchDataSyncWorkflow(eng *engine.Engine, params workflows.BatchDataSyncParams) (*workflows.WorkflowFactory, error) {
	factory := GetWorkflowFactory(eng)
	_, err := factory.CreateBatchDataSyncWorkflow(params)
	if err != nil {
		return nil, fmt.Errorf("failed to create batch data sync workflow: %w", err)
	}
	return factory, nil
}

// CreateRealtimeDataSyncWorkflow creates a realtime data sync workflow.
// This workflow syncs data incrementally with checkpoint support, with SAGA transaction support.
func CreateRealtimeDataSyncWorkflow(eng *engine.Engine, params workflows.RealtimeDataSyncParams) (*workflows.WorkflowFactory, error) {
	factory := GetWorkflowFactory(eng)
	_, err := factory.CreateRealtimeDataSyncWorkflow(params)
	if err != nil {
		return nil, fmt.Errorf("failed to create realtime data sync workflow: %w", err)
	}
	return factory, nil
}
