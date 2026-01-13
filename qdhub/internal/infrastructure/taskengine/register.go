// Package taskengine provides Task Engine integration for QDHub.
package taskengine

import (
	"context"
	"fmt"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/core/task"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/infrastructure/datasource"
	"qdhub/internal/infrastructure/taskengine/handlers"
	"qdhub/internal/infrastructure/taskengine/jobs"
)

// Dependencies holds the dependencies for task engine jobs.
type Dependencies struct {
	// DataSourceRegistry is the data source adapter registry.
	DataSourceRegistry *datasource.Registry
	// MetadataRepo is the metadata repository (required for compensation handlers).
	MetadataRepo metadata.Repository
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
		{"SaveAPIMetadata", jobs.SaveAPIMetadataJob, "保存 API 元数据"},
		// 模板任务 Job Functions（元数据）
		{"GenerateAPIDetailFetchSubTasks", jobs.GenerateAPIDetailFetchSubTasksJob, "生成 API 详情爬取子任务（模板任务）"},
		{"GenerateAPIParseSubTasks", jobs.GenerateAPIParseSubTasksJob, "生成 API 详情解析子任务（模板任务）"},

		// ==================== 数据源 Jobs ====================
		{"QueryData", jobs.QueryDataJob, "查询数据源 API"},
		{"ValidateToken", jobs.ValidateTokenJob, "验证数据源 Token"},
		{"TestConnection", jobs.TestConnectionJob, "测试数据源连接"},

		// ==================== 数据同步 Jobs ====================
		{"SyncAPIData", jobs.SyncAPIDataJob, "同步单个 API 数据"},
		{"GenerateDataSyncSubTasks", jobs.GenerateDataSyncSubTasksJob, "生成数据同步子任务（模板任务）"},
		{"DeleteSyncedData", jobs.DeleteSyncedDataJob, "删除同步的数据（用于回滚）"},

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

	// Register engine itself as dependency (for template tasks)
	registry.RegisterDependencyWithKey("Engine", eng)
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
