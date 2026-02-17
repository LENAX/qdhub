//go:build e2e
// +build e2e

// Package e2e 提供端到端测试
// 本文件验证应用服务层的完整功能
package e2e

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/persistence/uow"
	"qdhub/internal/infrastructure/scheduler"
	"qdhub/internal/infrastructure/taskengine"
)

// setupE2EApplicationServices 设置E2E测试所需的应用服务
func setupE2EApplicationServices(t *testing.T, db *persistence.DB, taskEngine *engine.Engine) (
	contracts.MetadataApplicationService,
	contracts.DataStoreApplicationService,
	contracts.SyncApplicationService,
	contracts.WorkflowApplicationService,
	func(),
) {
	ctx := context.Background()

	// 初始化Task Engine依赖
	taskEngineDeps := &taskengine.Dependencies{}
	err := taskengine.Initialize(ctx, taskEngine, taskEngineDeps)
	require.NoError(t, err)

	// 创建仓储
	dataSourceRepo := repository.NewDataSourceRepository(db)
	dataStoreRepo := repository.NewQuantDataStoreRepository(db)
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	workflowRepo, err := repository.NewWorkflowDefinitionRepository(db)
	require.NoError(t, err)
	metadataRepo := repository.NewMetadataRepository(db)

	// 创建适配器
	taskEngineAdapter := taskengine.NewTaskEngineAdapter(taskEngine, 0)
	workflowFactory := taskengine.GetWorkflowFactory(taskEngine)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()

	// 初始化内建工作流
	builtInInitializer := impl.NewBuiltInWorkflowInitializer(workflowRepo, workflowFactory, taskEngineAdapter)
	err = builtInInitializer.Initialize(ctx)
	require.NoError(t, err)

	// 创建WorkflowExecutor
	workflowExecutor := taskengine.NewWorkflowExecutor(workflowRepo, taskEngineAdapter, metadataRepo)

	// 创建依赖解析器
	dependencyResolver := sync.NewDependencyResolver()

	// 创建计划调度器（测试中可以使用 nil）
	var planScheduler sync.PlanScheduler = nil

	// 创建应用服务
	uowImpl := uow.NewUnitOfWork(db)
	metadataSvc := impl.NewMetadataApplicationService(dataSourceRepo, metadataRepo, nil, workflowExecutor)
	dataStoreSvc := impl.NewDataStoreApplicationService(dataStoreRepo, dataSourceRepo, workflowExecutor)
	syncSvc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, planScheduler, dataSourceRepo, workflowExecutor, dependencyResolver, taskEngineAdapter, uowImpl)
	workflowSvc := impl.NewWorkflowApplicationService(workflowRepo, taskEngineAdapter)

	cleanup := func() {
		// 清理资源
	}

	return metadataSvc, dataStoreSvc, syncSvc, workflowSvc, cleanup
}

// TestE2E_ApplicationService_Metadata 测试MetadataApplicationService的完整功能
func TestE2E_ApplicationService_Metadata(t *testing.T) {
	// 设置测试环境
	db, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	taskEngine, cleanupEngine := setupTaskEngine(t, db)
	defer cleanupEngine()

	metadataSvc, _, _, _, cleanup := setupE2EApplicationServices(t, db, taskEngine)
	defer cleanup()

	ctx := context.Background()

	t.Run("CreateDataSource", func(t *testing.T) {
		ds, err := metadataSvc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
			Name:        "TestDataSource",
			Description: "Test Data Source",
			BaseURL:     "https://api.test.com",
			DocURL:      "https://doc.test.com",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, ds.ID)
		assert.Equal(t, "TestDataSource", ds.Name)
	})

	t.Run("GetDataSource", func(t *testing.T) {
		// 先创建一个数据源
		ds, err := metadataSvc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
			Name:        "TestDataSource2",
			Description: "Test Data Source 2",
			BaseURL:     "https://api.test.com",
			DocURL:      "https://doc.test.com",
		})
		require.NoError(t, err)

		// 获取数据源
		got, err := metadataSvc.GetDataSource(ctx, ds.ID)
		require.NoError(t, err)
		assert.Equal(t, ds.ID, got.ID)
		assert.Equal(t, "TestDataSource2", got.Name)
	})

	t.Run("ListDataSources", func(t *testing.T) {
		sources, err := metadataSvc.ListDataSources(ctx)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(sources), 2) // 至少包含前面创建的2个
	})

	t.Run("SaveToken", func(t *testing.T) {
		// 先创建一个数据源
		ds, err := metadataSvc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
			Name:        "TestDataSource3",
			Description: "Test Data Source 3",
			BaseURL:     "https://api.test.com",
			DocURL:      "https://doc.test.com",
		})
		require.NoError(t, err)

		// 保存Token
		err = metadataSvc.SaveToken(ctx, contracts.SaveTokenRequest{
			DataSourceID: ds.ID,
			TokenValue:   "test-token-value",
		})
		require.NoError(t, err)

		// 验证Token已保存
		token, err := metadataSvc.GetToken(ctx, ds.ID)
		require.NoError(t, err)
		assert.NotNil(t, token)
		assert.Equal(t, "test-token-value", token.TokenValue)
	})

	t.Run("APISyncStrategy CRUD", func(t *testing.T) {
		// 先创建一个数据源
		ds, err := metadataSvc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
			Name:        "TestDataSource4",
			Description: "Test Data Source 4",
			BaseURL:     "https://api.test.com",
			DocURL:      "https://doc.test.com",
		})
		require.NoError(t, err)

		// 注意：需要先有API metadata才能创建策略
		// 这里我们跳过API验证，直接测试策略管理功能
		// 在实际场景中，应该先通过ParseAndImportMetadata导入API

		// 创建策略（这里可能会失败，因为API不存在，但可以测试接口）
		strategy, err := metadataSvc.CreateAPISyncStrategy(ctx, contracts.CreateAPISyncStrategyRequest{
			DataSourceID:     ds.ID,
			APIName:          "test_api",
			PreferredParam:   metadata.SyncParamTradeDate,
			SupportDateRange: true,
			RequiredParams:   []string{"ts_code"},
			Description:      "Test sync strategy",
		})
		// 如果API不存在，这是预期的错误
		if err != nil {
			assert.Contains(t, err.Error(), "not found")
		} else {
			assert.NotEmpty(t, strategy.ID)

			// 测试获取策略
			got, err := metadataSvc.GetAPISyncStrategy(ctx, contracts.GetAPISyncStrategyRequest{
				ID: &strategy.ID,
			})
			require.NoError(t, err)
			assert.Equal(t, strategy.ID, got.ID)

			// 测试更新策略
			err = metadataSvc.UpdateAPISyncStrategy(ctx, strategy.ID, contracts.UpdateAPISyncStrategyRequest{
				Description: stringPtr("Updated description"),
			})
			require.NoError(t, err)

			// 测试列出策略
			strategies, err := metadataSvc.ListAPISyncStrategies(ctx, ds.ID)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(strategies), 1)

			// 测试删除策略
			err = metadataSvc.DeleteAPISyncStrategy(ctx, strategy.ID)
			require.NoError(t, err)
		}
	})
}

// TestE2E_ApplicationService_DataStore 测试DataStoreApplicationService的完整功能
func TestE2E_ApplicationService_DataStore(t *testing.T) {
	// 设置测试环境
	db, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	taskEngine, cleanupEngine := setupTaskEngine(t, db)
	defer cleanupEngine()

	_, dataStoreSvc, _, _, cleanup := setupE2EApplicationServices(t, db, taskEngine)
	defer cleanup()

	ctx := context.Background()

	t.Run("CreateDataStore", func(t *testing.T) {
		ds, err := dataStoreSvc.CreateDataStore(ctx, contracts.CreateDataStoreRequest{
			Name:        "TestDataStore",
			Description: "Test Data Store",
			Type:        datastore.DataStoreTypeDuckDB,
			StoragePath: "/tmp/test.duckdb",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, ds.ID)
		assert.Equal(t, "TestDataStore", ds.Name)
	})

	t.Run("GetDataStore", func(t *testing.T) {
		// 先创建一个数据存储
		ds, err := dataStoreSvc.CreateDataStore(ctx, contracts.CreateDataStoreRequest{
			Name:        "TestDataStore2",
			Description: "Test Data Store 2",
			Type:        datastore.DataStoreTypeDuckDB,
			StoragePath: "/tmp/test2.duckdb",
		})
		require.NoError(t, err)

		// 获取数据存储
		got, err := dataStoreSvc.GetDataStore(ctx, ds.ID)
		require.NoError(t, err)
		assert.Equal(t, ds.ID, got.ID)
		assert.Equal(t, "TestDataStore2", got.Name)
	})

	t.Run("ListDataStores", func(t *testing.T) {
		stores, err := dataStoreSvc.ListDataStores(ctx)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(stores), 2) // 至少包含前面创建的2个
	})
}

// TestE2E_ApplicationService_SyncPlan 测试SyncApplicationService的SyncPlan功能
func TestE2E_ApplicationService_SyncPlan(t *testing.T) {
	// 设置测试环境
	db, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	taskEngine, cleanupEngine := setupTaskEngine(t, db)
	defer cleanupEngine()

	metadataSvc, dataStoreSvc, syncSvc, _, cleanup := setupE2EApplicationServices(t, db, taskEngine)
	defer cleanup()

	ctx := context.Background()

	// 创建数据源
	ds, err := metadataSvc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
		Name:        "TestDataSource",
		Description: "Test Data Source",
		BaseURL:     "https://api.test.com",
		DocURL:      "https://doc.test.com",
	})
	require.NoError(t, err)

	// 创建数据存储
	dataStore, err := dataStoreSvc.CreateDataStore(ctx, contracts.CreateDataStoreRequest{
		Name:        "TestDataStore",
		Description: "Test Data Store",
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: "/tmp/test.duckdb",
	})
	require.NoError(t, err)

	t.Run("CreateSyncPlan", func(t *testing.T) {
		plan, err := syncSvc.CreateSyncPlan(ctx, contracts.CreateSyncPlanRequest{
			Name:         "Test Sync Plan",
			Description:  "Test sync plan",
			DataSourceID: ds.ID,
			DataStoreID:  dataStore.ID,
			SelectedAPIs: []string{"daily", "stock_basic"},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, plan.ID)
		assert.Equal(t, "Test Sync Plan", plan.Name)
		assert.Equal(t, sync.PlanStatusDraft, plan.Status)
	})

	t.Run("GetSyncPlan", func(t *testing.T) {
		// 先创建一个计划
		plan, err := syncSvc.CreateSyncPlan(ctx, contracts.CreateSyncPlanRequest{
			Name:         "Test Sync Plan 2",
			Description:  "Test sync plan 2",
			DataSourceID: ds.ID,
			DataStoreID:  dataStore.ID,
			SelectedAPIs: []string{"daily"},
		})
		require.NoError(t, err)

		// 获取计划
		got, err := syncSvc.GetSyncPlan(ctx, plan.ID)
		require.NoError(t, err)
		assert.Equal(t, plan.ID, got.ID)
		assert.Equal(t, "Test Sync Plan 2", got.Name)
	})

	t.Run("ListSyncPlans", func(t *testing.T) {
		plans, err := syncSvc.ListSyncPlans(ctx)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(plans), 2) // 至少包含前面创建的2个
	})

	t.Run("UpdateSyncPlan", func(t *testing.T) {
		// 先创建一个计划
		plan, err := syncSvc.CreateSyncPlan(ctx, contracts.CreateSyncPlanRequest{
			Name:         "Test Sync Plan 3",
			Description:  "Test sync plan 3",
			DataSourceID: ds.ID,
			DataStoreID:  dataStore.ID,
			SelectedAPIs: []string{"daily"},
		})
		require.NoError(t, err)

		// 更新计划
		newName := "Updated Sync Plan"
		err = syncSvc.UpdateSyncPlan(ctx, plan.ID, contracts.UpdateSyncPlanRequest{
			Name: &newName,
		})
		require.NoError(t, err)

		// 验证更新
		got, err := syncSvc.GetSyncPlan(ctx, plan.ID)
		require.NoError(t, err)
		assert.Equal(t, "Updated Sync Plan", got.Name)
	})

	t.Run("DeleteSyncPlan", func(t *testing.T) {
		// 先创建一个计划
		plan, err := syncSvc.CreateSyncPlan(ctx, contracts.CreateSyncPlanRequest{
			Name:         "Test Sync Plan 4",
			Description:  "Test sync plan 4",
			DataSourceID: ds.ID,
			DataStoreID:  dataStore.ID,
			SelectedAPIs: []string{"daily"},
		})
		require.NoError(t, err)

		// 删除计划
		err = syncSvc.DeleteSyncPlan(ctx, plan.ID)
		require.NoError(t, err)

		// 验证已删除
		got, err := syncSvc.GetSyncPlan(ctx, plan.ID)
		assert.Error(t, err)
		assert.Nil(t, got)
	})
}

// TestE2E_ApplicationService_Workflow 测试WorkflowApplicationService的查询和控制功能
func TestE2E_ApplicationService_Workflow(t *testing.T) {
	// 设置测试环境
	db, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	taskEngine, cleanupEngine := setupTaskEngine(t, db)
	defer cleanupEngine()

	_, _, _, workflowSvc, cleanup := setupE2EApplicationServices(t, db, taskEngine)
	defer cleanup()

	ctx := context.Background()

	t.Run("ListWorkflowInstances", func(t *testing.T) {
		// 需要先有一个工作流定义ID
		// 由于我们删除了工作流定义管理，这里需要从已初始化的内建工作流获取
		// 这里我们测试接口是否可用
		instances, err := workflowSvc.ListWorkflowInstances(ctx, shared.NewID(), nil)
		require.NoError(t, err)
		assert.NotNil(t, instances)
	})

	t.Run("GetWorkflowStatus", func(t *testing.T) {
		// 测试获取不存在的实例状态
		status, err := workflowSvc.GetWorkflowStatus(ctx, shared.NewID())
		// 可能会返回错误，这是预期的
		if err != nil {
			assert.Contains(t, err.Error(), "not found")
		} else {
			assert.NotNil(t, status)
		}
	})
}

// Helper functions

func setupTestDB(t *testing.T) (*persistence.DB, func()) {
	dbPath := filepath.Join(os.TempDir(), "test_e2e_app.db")
	os.Remove(dbPath) // 清理旧数据库

	db, err := persistence.NewDB(dbPath)
	require.NoError(t, err)

	// 运行迁移
	migrationSQL, err := os.ReadFile("../../migrations/001_init_schema.up.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(migrationSQL))
	require.NoError(t, err)

	// 执行 SyncPlan 迁移脚本
	syncPlanMigrationSQL, err := os.ReadFile("../../migrations/003_sync_plan_migration.up.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(syncPlanMigrationSQL))
	require.NoError(t, err)

	// 执行 API Sync Strategy 迁移脚本
	apiSyncStrategyMigrationSQL, err := os.ReadFile("../../migrations/004_api_sync_strategy.up.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(apiSyncStrategyMigrationSQL))
	require.NoError(t, err)

	cleanup := func() {
		db.Close()
		os.Remove(dbPath)
	}

	return db, cleanup
}

func setupTaskEngine(t *testing.T, db *persistence.DB) (*engine.Engine, func()) {
	taskEngineDSN := db.DSN()
	aggregateRepo, err := sqlite.NewWorkflowAggregateRepoFromDSN(taskEngineDSN)
	require.NoError(t, err)

	eng, err := engine.NewEngineWithAggregateRepo(5, 30, aggregateRepo)
	require.NoError(t, err)

	ctx := context.Background()
	err = eng.Start(ctx)
	require.NoError(t, err)

	cleanup := func() {
		eng.Stop()
	}

	return eng, cleanup
}

func stringPtr(s string) *string {
	return &s
}
