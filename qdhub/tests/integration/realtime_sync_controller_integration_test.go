//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/impl"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/realtime"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/persistence/uow"
	"qdhub/internal/infrastructure/realtimestore"
	"qdhub/internal/infrastructure/scheduler"
)

// TestRealtimeSyncController_StartAndStop 验证 StartRealtimeSync 启动对应计划执行，StopRealtimeSync 取消执行；UoW 内状态一致。
// 依赖完整迁移（需在模块根 qdhub 下执行 go test -tags=integration ./tests/integration/）。
func TestRealtimeSyncController_StartAndStop(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()
	ctx := context.Background()

	// 完整迁移后应有 data_sources、sync_plan 等表
	var hasDataSources int
	if err := db.Get(&hasDataSources, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='data_sources'"); err != nil || hasDataSources == 0 {
		t.Skip("data_sources table missing (run integration tests from module root so full migrations apply)")
	}
	var hasSyncPlan int
	if err := db.Get(&hasSyncPlan, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='sync_plan'"); err != nil || hasSyncPlan == 0 {
		t.Skip("sync_plan table missing (run integration tests from module root)")
	}

	// 确保有 Tushare 数据源、realtime_duckdb、realtime-sina-quote 计划（033 依赖 Tushare）
	dataSourceRepo := repository.NewDataSourceRepository(db)
	dsList, _ := dataSourceRepo.List()
	var tushareID shared.ID
	for _, d := range dsList {
		if d.Name == "Tushare" {
			tushareID = d.ID
			break
		}
	}
	if tushareID.IsEmpty() {
		ds := metadata.NewDataSource("Tushare", "Tushare", "https://api.tushare.pro", "https://doc.tushare.pro")
		require.NoError(t, dataSourceRepo.Create(ds))
		tushareID = ds.ID
	}

	dataStoreRepo := repository.NewQuantDataStoreRepository(db)
	storeList, _ := dataStoreRepo.List()
	var realtimeStoreID string
	for _, s := range storeList {
		if s.Name == "realtime_duckdb" {
			realtimeStoreID = s.ID.String()
			break
		}
	}
	if realtimeStoreID == "" {
		// 032 未插入时手动插入
		_, err := db.Exec(`
			INSERT OR IGNORE INTO quant_data_stores (id, name, description, type, dsn, storage_path, status, created_at, updated_at)
			VALUES ('realtime-duckdb-0000-4000-8000-000000000001', 'realtime_duckdb', 'realtime', 'duckdb', '', './data/realtime_ticks.duckdb', 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		`)
		require.NoError(t, err)
		realtimeStoreID = "realtime-duckdb-0000-4000-8000-000000000001"
	}

	syncPlanRepo := repository.NewSyncPlanRepository(db)
	plan, err := syncPlanRepo.Get(shared.ID("realtime-sina-quote"))
	if err != nil || plan == nil {
		// 创建 realtime-sina-quote 计划与一条 task
		plan = sync.NewSyncPlan("Realtime_sina_quote", "sina quote", tushareID, []string{"realtime_quote"})
		plan.ID = shared.ID("realtime-sina-quote")
		plan.Mode = sync.PlanModeRealtime
		plan.SetDataStore(shared.ID(realtimeStoreID))
		plan.Status = sync.PlanStatusEnabled
		plan.ResolvedAPIs = []string{"realtime_quote"}
		require.NoError(t, syncPlanRepo.Create(plan))
		task := sync.NewSyncTask("realtime_quote", sync.TaskSyncModeDirect, 0)
		plan.AddTask(task)
		require.NoError(t, syncPlanRepo.AddTask(task))
	}

	realtimeRepo := repository.NewRealtimeSourceRepository(db)
	ensureRealtimeSourcesTable(t, db)
	sources, err := realtimeRepo.List()
	require.NoError(t, err)
	var sinaSourceID string
	for _, s := range sources {
		if s.Type == realtime.TypeSina {
			sinaSourceID = s.ID.String()
			break
		}
	}
	if sinaSourceID == "" {
		src := realtime.NewRealtimeSource("sina", realtime.TypeSina, "{}", 2, false, false, true)
		require.NoError(t, realtimeRepo.Create(src))
		sinaSourceID = src.ID.String()
	}

	// SyncSvc（mock executor，nil task engine 以便 CancelExecution 只更新 DB）
	workflowExecutor := &MockSyncWorkflowExecutor{}
	cronCalc := scheduler.NewCronSchedulerCalculatorAdapter()
	uowImpl := uow.NewUnitOfWork(db)
	metadataRepo := repository.NewMetadataRepository(db)
	syncSvc := impl.NewSyncApplicationService(
		syncPlanRepo,
		cronCalc,
		nil,
		dataSourceRepo,
		dataStoreRepo,
		workflowExecutor,
		&MockSyncDependencyResolver{},
		nil,
		uowImpl,
		metadataRepo,
		nil,
		"",
		nil,
	)

	selector := realtimestore.NewRealtimeSourceSelector()
	controller := impl.NewRealtimeSyncController(realtimeRepo, syncSvc, syncPlanRepo, selector)

	// 无运行中执行
	running0, err := syncPlanRepo.GetRunningExecutionByPlanID(shared.ID("realtime-sina-quote"))
	require.NoError(t, err)
	assert.Nil(t, running0)

	// StartRealtimeSync：全局禁用新浪时不应启动 sina 计划
	err = controller.StartRealtimeSync(ctx, sinaSourceID)
	if realtimestore.SinaRealtimeDisabled {
		require.Error(t, err)
		runningAfterFail, err2 := syncPlanRepo.GetRunningExecutionByPlanID(shared.ID("realtime-sina-quote"))
		require.NoError(t, err2)
		assert.Nil(t, runningAfterFail)
		return
	}
	require.NoError(t, err)
	assert.Equal(t, realtimestore.SourceSina, selector.CurrentSource())

	running1, err := syncPlanRepo.GetRunningExecutionByPlanID(shared.ID("realtime-sina-quote"))
	require.NoError(t, err)
	require.NotNil(t, running1)
	assert.Equal(t, sync.ExecStatusRunning, running1.Status)

	err = controller.StopRealtimeSync(ctx, sinaSourceID)
	require.NoError(t, err)

	running2, err := syncPlanRepo.GetRunningExecutionByPlanID(shared.ID("realtime-sina-quote"))
	require.NoError(t, err)
	assert.Nil(t, running2)
}
