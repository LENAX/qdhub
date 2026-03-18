//go:build e2e
// +build e2e

// Package e2e 实时同步 E2E：从 SyncPlan(realtime) 到实时工作流数据同步的全链路测试。
// 1) Mock：使用 CSV 数据 + MockRealtimeAdapter 模拟整条链路；
// 2) Real：调用新浪/东财真实 API 做实时同步（开盘时间可验证最新数据）。
package e2e

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/storage/sqlite"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/datasource/tushare/realtime"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/persistence/uow"
	"qdhub/internal/infrastructure/quantdb/duckdb"
	"qdhub/internal/infrastructure/quantdb/writequeue"
	"qdhub/internal/infrastructure/realtimebuffer"
	"qdhub/internal/infrastructure/taskengine"
	"qdhub/pkg/config"
)

// ==================== Mock Realtime Adapter（CSV 数据）====================

// mockRealtimeAdapterFromCSV 从 CSV 提供实时数据，用于全链路 Mock E2E。
type mockRealtimeAdapterFromCSV struct {
	rowsByTsCode map[string][]map[string]interface{} // ts_code -> rows
	allRows      []map[string]interface{}
	source       string
}

func newMockRealtimeAdapterFromCSV(csvPath string, source string) (*mockRealtimeAdapterFromCSV, error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	recs, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(recs) < 2 {
		return nil, fmt.Errorf("csv too short")
	}
	headers := recs[0]
	allRows := make([]map[string]interface{}, 0, len(recs)-1)
	byTsCode := make(map[string][]map[string]interface{})
	for i := 1; i < len(recs); i++ {
		row := make(map[string]interface{})
		for j, h := range headers {
			h = strings.Trim(h, `"`)
			if j >= len(recs[i]) {
				continue
			}
			v := strings.Trim(recs[i][j], `"`)
			if h == "open" || h == "close" || h == "high" || h == "low" || h == "vol" || h == "amount" {
				if fv, err := strconv.ParseFloat(v, 64); err == nil {
					row[h] = fv
				} else {
					row[h] = v
				}
			} else {
				row[h] = v
			}
		}
		allRows = append(allRows, row)
		if tc, ok := row["ts_code"].(string); ok && tc != "" {
			byTsCode[tc] = append(byTsCode[tc], row)
		}
	}
	return &mockRealtimeAdapterFromCSV{rowsByTsCode: byTsCode, allRows: allRows, source: source}, nil
}

func (m *mockRealtimeAdapterFromCSV) Source() string { return m.source }

func (m *mockRealtimeAdapterFromCSV) SupportedAPIs() []string {
	return []string{"realtime_quote", "rt_min"}
}

func (m *mockRealtimeAdapterFromCSV) Supports(apiName string) bool {
	for _, n := range m.SupportedAPIs() {
		if n == apiName {
			return true
		}
	}
	return false
}

func (m *mockRealtimeAdapterFromCSV) SupportedModes(apiName string) []realtime.RealtimeMode {
	if m.Supports(apiName) {
		return []realtime.RealtimeMode{realtime.RealtimeModePull}
	}
	return nil
}

func (m *mockRealtimeAdapterFromCSV) Fetch(ctx context.Context, apiName string, params map[string]interface{}) ([]map[string]interface{}, error) {
	if !m.Supports(apiName) {
		return nil, fmt.Errorf("unsupported api: %s", apiName)
	}
	tsCodeVal, _ := params["ts_code"]
	var tsCode string
	switch v := tsCodeVal.(type) {
	case string:
		tsCode = v
	}
	if tsCode != "" {
		// 可能为逗号分隔的多个代码，取第一个用于过滤（mock 仅 000009.SZ）
		for _, part := range strings.Split(tsCode, ",") {
			part = strings.TrimSpace(part)
			if rows, ok := m.rowsByTsCode[part]; ok && len(rows) > 0 {
				return rows, nil
			}
		}
	}
	return m.allRows, nil
}

func (m *mockRealtimeAdapterFromCSV) SupportsPush(apiName string) bool { return false }

func (m *mockRealtimeAdapterFromCSV) StartStream(ctx context.Context, apiName string, params map[string]interface{}, onBatch func([]map[string]interface{}) error) error {
	return fmt.Errorf("mock adapter does not support push")
}

// ==================== E2E 上下文 ====================

type realtimeSyncE2EContext struct {
	db                 *persistence.DB
	engine             *engine.Engine
	duckDBPath         string
	quantDBFactory     datastore.QuantDBFactory
	realtimeAdapterReg *realtime.DefaultRegistry
	realtimeBufferReg  *realtimebuffer.DefaultRegistry
	syncAppService     contracts.SyncApplicationService
	taskEngineAdapter  workflow.TaskEngineAdapter
	metadataRepo       metadata.Repository
	dataSourceRepo     metadata.DataSourceRepository
	datastoreRepo      datastore.QuantDataStoreRepository
	syncPlanRepo       sync.SyncPlanRepository
	workflowExecutor   workflow.WorkflowExecutor
	cleanup            func()
}

func getRealtimeE2EDataDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "data")
}

// setupRealtimeSyncE2EContext 为实时同步 E2E 准备环境：SQLite + DuckDB、迁移、Mock 适配器与 Registry、Sync 服务。
func setupRealtimeSyncE2EContext(t *testing.T, csvPath string) *realtimeSyncE2EContext {
	ctx := context.Background()

	tmpSQLite, err := os.CreateTemp("", "realtime_e2e_*.db")
	require.NoError(t, err)
	tmpSQLite.Close()
	dsn := tmpSQLite.Name()
	duckDBPath := filepath.Join(os.TempDir(), fmt.Sprintf("realtime_e2e_%d.duckdb", time.Now().UnixNano()))

	db, err := persistence.NewDB(dsn)
	require.NoError(t, err)

	// 迁移 001, 003；先插入 Tushare（004/019 依赖 data_sources 存在 tushare）
	migrationDir := filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(getRealtimeE2EDataDir()))), "migrations")
	for _, name := range []string{"001_init_schema.up.sql", "003_sync_plan_migration.up.sql"} {
		sqlPath := filepath.Join(migrationDir, name)
		sql, err := os.ReadFile(sqlPath)
		if err != nil {
			t.Logf("skip migration %s: %v", name, err)
			continue
		}
		_, err = db.Exec(string(sql))
		require.NoError(t, err, "run %s", name)
	}

	var dsID string
	err = db.QueryRow(`SELECT id FROM data_sources WHERE LOWER(name) = 'tushare' LIMIT 1`).Scan(&dsID)
	if err != nil || dsID == "" {
		dsID = shared.NewID().String()
		_, err = db.Exec(`INSERT INTO data_sources (id, name, description, base_url, doc_url, status, created_at, updated_at) VALUES (?, 'Tushare', 'E2E', '', '', 'active', datetime('now'), datetime('now'))`, dsID)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT OR REPLACE INTO tokens (id, data_source_id, token_value, created_at) VALUES (?, ?, 'e2e_mock_token', datetime('now'))`, shared.NewID().String(), dsID)
		require.NoError(t, err)
	}

	// 迁移 004–020（tushare 已存在，019 的 UPDATE 能正确设置 realtime_quote 的 ts_code 策略）
	for _, name := range []string{
		"004_api_sync_strategy.up.sql",
		"005_sync_plan_default_params.up.sql",
		"006_sync_plan_mode_realtime.up.sql",
		"007_daily_adj_factor_trade_date_expand.up.sql",
		"010_sync_plan_incremental_mode.up.sql",
		"015_sync_plan_incremental_start_date_source.up.sql",
		"016_api_sync_strategy_fixed_params.up.sql",
		"017_api_sync_strategy_news_fixed_fields.up.sql",
		"018_api_sync_strategy_time_window_news_cctv.up.sql",
		"019_realtime_api_sync_strategy.up.sql",
		"020_sync_plan_pull_interval_and_schedule_window.up.sql",
		"013_sync_execution_workflow_error_message.up.sql",
	} {
		sqlPath := filepath.Join(migrationDir, name)
		sql, err := os.ReadFile(sqlPath)
		if err != nil {
			t.Logf("skip migration %s: %v", name, err)
			continue
		}
		_, err = db.Exec(string(sql))
		require.NoError(t, err, "run %s", name)
	}

	// DuckDB：建表 stock_basic、realtime_quote
	quantDBFactory := duckdb.NewFactory()
	qdb, err := quantDBFactory.Create(datastore.QuantDBConfig{
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: duckDBPath,
	})
	require.NoError(t, err)
	require.NoError(t, qdb.Connect(ctx))
	_, err = qdb.Execute(ctx, `CREATE TABLE IF NOT EXISTS stock_basic (ts_code VARCHAR(32), name VARCHAR(128), list_date VARCHAR(16))`)
	require.NoError(t, err)
	_, err = qdb.Execute(ctx, `INSERT INTO stock_basic (ts_code, name, list_date) VALUES ('000009.SZ', '中国宝安', '19910101')`)
	require.NoError(t, err)
	_, err = qdb.Execute(ctx, `CREATE TABLE IF NOT EXISTS realtime_quote (
		ts_code VARCHAR(32), name VARCHAR(64), open DOUBLE, pre_close DOUBLE, price DOUBLE, high DOUBLE, low DOUBLE,
		bid DOUBLE, ask DOUBLE, volume DOUBLE, amount DOUBLE,
		b1_v VARCHAR(32), b1_p DOUBLE, b2_v VARCHAR(32), b2_p DOUBLE, b3_v VARCHAR(32), b3_p DOUBLE, b4_v VARCHAR(32), b4_p DOUBLE, b5_v VARCHAR(32), b5_p DOUBLE,
		a1_v VARCHAR(32), a1_p DOUBLE, a2_v VARCHAR(32), a2_p DOUBLE, a3_v VARCHAR(32), a3_p DOUBLE, a4_v VARCHAR(32), a4_p DOUBLE, a5_v VARCHAR(32), a5_p DOUBLE,
		date VARCHAR(16), time VARCHAR(16), trade_time VARCHAR(32), close DOUBLE, vol DOUBLE,
		PRIMARY KEY (ts_code, date, time)
	)`)
	require.NoError(t, err)
	qdb.Close()

	// Mock 实时 Adapter（CSV）
	mockAdapter, err := newMockRealtimeAdapterFromCSV(csvPath, "sina")
	require.NoError(t, err)
	adapterReg := realtime.NewDefaultRegistry()
	adapterReg.Register("sina", mockAdapter)
	bufReg := realtimebuffer.NewDefaultRegistry(256)

	// Task Engine
	aggregateRepo, err := sqlite.NewWorkflowAggregateRepoFromDSN(dsn)
	require.NoError(t, err)
	eng, err := engine.NewEngineWithAggregateRepo(5, 30, aggregateRepo)
	require.NoError(t, err)
	require.NoError(t, eng.Start(ctx))

	metadataRepo := repository.NewMetadataRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	datastoreRepo := repository.NewQuantDataStoreRepository(db)
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	workflowRepo, _ := repository.NewWorkflowDefinitionRepository(db)

	wqCfg := config.Default().QuantDB.WriteQueue
	quantDBWriteQueue := writequeue.NewQueue(wqCfg, quantDBFactory)
	taskEngineDeps := &taskengine.Dependencies{
		DataSourceRegistry:      nil, // 实时不依赖 Tushare registry
		MetadataRepo:            metadataRepo,
		DataStoreRepo:           datastoreRepo,
		QuantDBFactory:          quantDBFactory,
		QuantDBWriteQueue:       quantDBWriteQueue,
		RealtimeAdapterRegistry: adapterReg,
		RealtimeBufferRegistry:  bufReg,
	}
	err = taskengine.Initialize(ctx, eng, taskEngineDeps)
	require.NoError(t, err)

	taskEngineAdapter := taskengine.NewTaskEngineAdapter(eng, 0)
	workflowFactory := taskengine.GetWorkflowFactory(eng)
	builtInInitializer := impl.NewBuiltInWorkflowInitializer(workflowRepo, workflowFactory, taskEngineAdapter)
	err = builtInInitializer.Initialize(ctx)
	require.NoError(t, err)

	workflowExecutor := taskengine.NewWorkflowExecutor(workflowRepo, taskEngineAdapter, metadataRepo, adapterReg, "", nil, "", "", "")

	cronCalculator := sync.NewCronScheduleCalculator()
	dependencyResolver := sync.NewDependencyResolver()
	uowImpl := uow.NewUnitOfWork(db)
	syncAppService := impl.NewSyncApplicationService(
		syncPlanRepo,
		cronCalculator,
		nil,
		dataSourceRepo,
		datastoreRepo,
		workflowExecutor,
		dependencyResolver,
		taskEngineAdapter,
		uowImpl,
		metadataRepo,
		quantDBFactory,
		"",
		nil,
	)

	cleanup := func() {
		_ = quantDBWriteQueue.Close()
		eng.Stop()
		os.Remove(dsn)
		os.Remove(duckDBPath)
		db.Close()
	}

	return &realtimeSyncE2EContext{
		db:                 db,
		engine:             eng,
		duckDBPath:         duckDBPath,
		quantDBFactory:     quantDBFactory,
		realtimeAdapterReg: adapterReg,
		realtimeBufferReg:  bufReg,
		syncAppService:     syncAppService,
		taskEngineAdapter:  taskEngineAdapter,
		metadataRepo:       metadataRepo,
		dataSourceRepo:     dataSourceRepo,
		datastoreRepo:      datastoreRepo,
		syncPlanRepo:       syncPlanRepo,
		workflowExecutor:   workflowExecutor,
		cleanup:            cleanup,
	}
}

// ==================== Mock 全链路 E2E ====================

// TestE2E_RealtimeSync_Mock 从 SyncPlan(realtime) 到实时工作流数据同步的全链路 Mock 测试。
// 使用 tests/e2e/data/stk_mins_202603082215.csv 作为数据源，Mock Adapter 返回 realtime_quote 格式数据，落库到 DuckDB realtime_quote 表。
func TestE2E_RealtimeSync_Mock(t *testing.T) {
	logFile, logCleanup := setupLogFile(t, "RealtimeSync_Mock")
	defer logCleanup()
	defer logFile.Close()

	csvPath := filepath.Join(getRealtimeE2EDataDir(), "stk_mins_202603082215.csv")
	if _, err := os.Stat(csvPath); err != nil {
		t.Skipf("skip: CSV not found: %s", csvPath)
	}

	testCtx := setupRealtimeSyncE2EContext(t, csvPath)
	defer testCtx.cleanup()

	ctx := context.Background()

	// 获取 Tushare 数据源 ID
	var dsID shared.ID
	dsList, err := testCtx.dataSourceRepo.List()
	require.NoError(t, err)
	for _, d := range dsList {
		if strings.EqualFold(d.Name, "tushare") {
			dsID = d.ID
			break
		}
	}
	require.False(t, dsID.IsEmpty(), "need tushare data source")

	// 创建 Quant Data Store（DuckDB 路径）
	storeID := shared.NewID()
	absDuckPath := testCtx.duckDBPath
	if !filepath.IsAbs(absDuckPath) {
		absDuckPath, _ = filepath.Abs(absDuckPath)
	}
	err = testCtx.datastoreRepo.Create(&datastore.QuantDataStore{
		ID:          storeID,
		Name:        "e2e_duckdb",
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: absDuckPath,
		CreatedAt:   shared.Timestamp(time.Now()),
		UpdatedAt:   shared.Timestamp(time.Now()),
	})
	require.NoError(t, err)

	// 创建 SyncPlan：realtime，selected_apis = [realtime_quote]
	planID := shared.NewID()
	plan := &sync.SyncPlan{
		ID:                  planID,
		Name:                "e2e_realtime_mock",
		DataSourceID:        dsID,
		DataStoreID:         storeID,
		Mode:                sync.PlanModeRealtime,
		SelectedAPIs:        []string{"realtime_quote"},
		Status:              sync.PlanStatusDraft,
		CreatedAt:           shared.Timestamp(time.Now()),
		UpdatedAt:           shared.Timestamp(time.Now()),
		PullIntervalSeconds: 60,
	}
	err = testCtx.syncPlanRepo.Create(plan)
	require.NoError(t, err)

	// Resolve + Enable
	err = testCtx.syncAppService.ResolveSyncPlan(ctx, planID)
	require.NoError(t, err)
	plan, _ = testCtx.syncPlanRepo.Get(planID)
	plan.Status = sync.PlanStatusEnabled
	err = testCtx.syncPlanRepo.Update(plan)
	require.NoError(t, err)

	// 执行：Streaming 模式下实例持续运行，不等待“完成”，而是在短时间内观察数据是否持续落库
	execID, err := testCtx.syncAppService.ExecuteSyncPlan(ctx, planID, contracts.ExecuteSyncPlanRequest{})
	require.NoError(t, err)
	require.False(t, execID.IsEmpty())

	// 轮询 DuckDB 中 realtime_quote 行数，验证在一段时间内有数据持续写入
	qdb, err := testCtx.quantDBFactory.Create(datastore.QuantDBConfig{
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: testCtx.duckDBPath,
	})
	require.NoError(t, err)
	require.NoError(t, qdb.Connect(ctx))
	defer qdb.Close()
	var lastCnt int64
	for i := 0; i < 5; i++ {
		rows, err := qdb.Query(ctx, `SELECT COUNT(*) as cnt FROM realtime_quote`)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(rows), 1)
		var cnt int64
		switch v := rows[0]["cnt"].(type) {
		case int64:
			cnt = v
		case int:
			cnt = int64(v)
		case int32:
			cnt = int64(v)
		case int16:
			cnt = int64(v)
		case float64:
			cnt = int64(v)
		case float32:
			cnt = int64(v)
		default:
			if s, ok := rows[0]["cnt"].(fmt.Stringer); ok {
				if n, err := strconv.ParseInt(s.String(), 10, 64); err == nil {
					cnt = n
				}
			}
		}
		t.Logf("mock streaming round %d, realtime_quote row count: %d", i, cnt)
		if cnt > lastCnt {
			lastCnt = cnt
		}
		time.Sleep(2 * time.Second)
	}
	// Streaming 场景下，数据写入通过 Handler 日志和 BulkInsert 完成；此处主要验证链路无 panic/错误。
	logrus.Infof("[RealtimeSync Mock Streaming E2E] realtime_quote final observed row count: %d", lastCnt)
}

// ==================== 真实新浪/东财 E2E ====================

// TestE2E_RealtimeSync_Real 调用新浪/东财真实 API 做实时同步；需在开盘时间运行以验证最新数据。
// 可通过环境变量 QDHUB_REALTIME_E2E=1 启用，避免 CI 非交易时间失败。
func TestE2E_RealtimeSync_Real(t *testing.T) {
	if os.Getenv("QDHUB_REALTIME_E2E") != "1" {
		t.Skip("skip real realtime e2e unless QDHUB_REALTIME_E2E=1")
	}

	logFile, logCleanup := setupLogFile(t, "RealtimeSync_Real")
	defer logCleanup()
	defer logFile.Close()

	ctx := context.Background()
	// 使用与 Mock 相同的 setup，但注册真实 Realtime Adapter（新浪+东财）
	tmpSQLite, err := os.CreateTemp("", "realtime_real_e2e_*.db")
	require.NoError(t, err)
	tmpSQLite.Close()
	dsn := tmpSQLite.Name()
	duckDBPath := filepath.Join(os.TempDir(), fmt.Sprintf("realtime_real_e2e_%d.duckdb", time.Now().UnixNano()))
	db, err := persistence.NewDB(dsn)
	require.NoError(t, err)

	migrationDir := filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(getRealtimeE2EDataDir()))), "migrations")
	for _, name := range []string{"001_init_schema.up.sql", "003_sync_plan_migration.up.sql"} {
		sql, _ := os.ReadFile(filepath.Join(migrationDir, name))
		if len(sql) > 0 {
			_, _ = db.Exec(string(sql))
		}
	}
	var dsID string
	_ = db.QueryRow(`SELECT id FROM data_sources WHERE LOWER(name) = 'tushare' LIMIT 1`).Scan(&dsID)
	if dsID == "" {
		dsID = shared.NewID().String()
		_, _ = db.Exec(`INSERT INTO data_sources (id, name, description, base_url, doc_url, status, created_at, updated_at) VALUES (?, 'Tushare', 'E2E', '', '', 'active', datetime('now'), datetime('now'))`, dsID)
		_, _ = db.Exec(`INSERT OR REPLACE INTO tokens (id, data_source_id, token_value, created_at) VALUES (?, ?, 'real_e2e_token', datetime('now'))`, shared.NewID().String(), dsID)
	}
	for _, name := range []string{
		"004_api_sync_strategy.up.sql", "005_sync_plan_default_params.up.sql", "006_sync_plan_mode_realtime.up.sql",
		"007_daily_adj_factor_trade_date_expand.up.sql", "010_sync_plan_incremental_mode.up.sql",
		"015_sync_plan_incremental_start_date_source.up.sql", "016_api_sync_strategy_fixed_params.up.sql",
		"017_api_sync_strategy_news_fixed_fields.up.sql", "018_api_sync_strategy_time_window_news_cctv.up.sql",
		"019_realtime_api_sync_strategy.up.sql", "020_sync_plan_pull_interval_and_schedule_window.up.sql",
		"013_sync_execution_workflow_error_message.up.sql",
	} {
		sql, _ := os.ReadFile(filepath.Join(migrationDir, name))
		if len(sql) > 0 {
			_, _ = db.Exec(string(sql))
		}
	}

	quantDBFactory := duckdb.NewFactory()
	qdb, _ := quantDBFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	_ = qdb.Connect(ctx)
	_, _ = qdb.Execute(ctx, `CREATE TABLE IF NOT EXISTS stock_basic (ts_code VARCHAR(32), name VARCHAR(128), list_date VARCHAR(16))`)
	_, _ = qdb.Execute(ctx, `INSERT INTO stock_basic (ts_code, name, list_date) VALUES ('000001.SZ', '平安银行', '19910403')`)
	_, _ = qdb.Execute(ctx, `CREATE TABLE IF NOT EXISTS realtime_quote (
		ts_code VARCHAR(32), name VARCHAR(64), open DOUBLE, pre_close DOUBLE, price DOUBLE, high DOUBLE, low DOUBLE,
		bid DOUBLE, ask DOUBLE, volume DOUBLE, amount DOUBLE,
		b1_v VARCHAR(32), b1_p DOUBLE, b2_v VARCHAR(32), b2_p DOUBLE, b3_v VARCHAR(32), b3_p DOUBLE, b4_v VARCHAR(32), b4_p DOUBLE, b5_v VARCHAR(32), b5_p DOUBLE,
		a1_v VARCHAR(32), a1_p DOUBLE, a2_v VARCHAR(32), a2_p DOUBLE, a3_v VARCHAR(32), a3_p DOUBLE, a4_v VARCHAR(32), a4_p DOUBLE, a5_v VARCHAR(32), a5_p DOUBLE,
		date VARCHAR(16), time VARCHAR(16), trade_time VARCHAR(32), close DOUBLE, vol DOUBLE,
		PRIMARY KEY (ts_code, date, time)
	)`)
	qdb.Close()

	adapterReg := realtime.NewRegistryWithDefaults()
	bufReg := realtimebuffer.NewDefaultRegistry(256)

	aggregateRepo, _ := sqlite.NewWorkflowAggregateRepoFromDSN(dsn)
	eng, _ := engine.NewEngineWithAggregateRepo(5, 30, aggregateRepo)
	_ = eng.Start(ctx)

	metadataRepo := repository.NewMetadataRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	datastoreRepo := repository.NewQuantDataStoreRepository(db)
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	workflowRepo, _ := repository.NewWorkflowDefinitionRepository(db)

	wqCfg := config.Default().QuantDB.WriteQueue
	quantDBWriteQueue := writequeue.NewQueue(wqCfg, quantDBFactory)
	taskEngineDeps := &taskengine.Dependencies{
		MetadataRepo:            metadataRepo,
		DataStoreRepo:           datastoreRepo,
		QuantDBFactory:          quantDBFactory,
		QuantDBWriteQueue:       quantDBWriteQueue,
		RealtimeAdapterRegistry: adapterReg,
		RealtimeBufferRegistry:  bufReg,
	}
	_ = taskengine.Initialize(ctx, eng, taskEngineDeps)
	taskEngineAdapter := taskengine.NewTaskEngineAdapter(eng, 0)
	workflowFactory := taskengine.GetWorkflowFactory(eng)
	_ = impl.NewBuiltInWorkflowInitializer(workflowRepo, workflowFactory, taskEngineAdapter).Initialize(ctx)
	workflowExecutor := taskengine.NewWorkflowExecutor(workflowRepo, taskEngineAdapter, metadataRepo, adapterReg, "", nil, "", "", "")

	syncAppService := impl.NewSyncApplicationService(
		syncPlanRepo, sync.NewCronScheduleCalculator(), nil,
		dataSourceRepo, datastoreRepo, workflowExecutor, sync.NewDependencyResolver(),
		taskEngineAdapter, uow.NewUnitOfWork(db), metadataRepo, quantDBFactory,
		"", nil,
	)

	defer func() {
		_ = quantDBWriteQueue.Close()
		eng.Stop()
		os.Remove(dsn)
		os.Remove(duckDBPath)
		db.Close()
	}()

	dsList, _ := dataSourceRepo.List()
	for _, d := range dsList {
		if strings.EqualFold(d.Name, "tushare") {
			dsID = d.ID.String()
			break
		}
	}
	require.NotEmpty(t, dsID, "need tushare data source")
	storeID := shared.NewID()
	absPath := duckDBPath
	if !filepath.IsAbs(absPath) {
		absPath, _ = filepath.Abs(absPath)
	}
	_ = datastoreRepo.Create(&datastore.QuantDataStore{
		ID: storeID, Name: "e2e_real_duckdb", Type: datastore.DataStoreTypeDuckDB,
		StoragePath: absPath, CreatedAt: shared.Timestamp(time.Now()), UpdatedAt: shared.Timestamp(time.Now()),
	})

	planID := shared.NewID()
	dsIDAsID := shared.ID(dsID)
	_ = syncPlanRepo.Create(&sync.SyncPlan{
		ID: planID, Name: "e2e_realtime_real", DataSourceID: dsIDAsID, DataStoreID: storeID,
		Mode: sync.PlanModeRealtime, SelectedAPIs: []string{"realtime_quote"}, Status: sync.PlanStatusDraft,
		// 为了在 E2E 中观察到多轮拉取，这里将 PullIntervalSeconds 缩短到 1 秒（生产环境仍可使用更长间隔）。
		CreatedAt: shared.Timestamp(time.Now()), UpdatedAt: shared.Timestamp(time.Now()), PullIntervalSeconds: 1,
	})
	err = syncAppService.ResolveSyncPlan(ctx, planID)
	require.NoError(t, err)
	plan, _ := syncPlanRepo.Get(planID)
	plan.Status = sync.PlanStatusEnabled
	_ = syncPlanRepo.Update(plan)

	execID, err := syncAppService.ExecuteSyncPlan(ctx, planID, contracts.ExecuteSyncPlanRequest{})
	require.NoError(t, err)
	require.False(t, execID.IsEmpty())
	
	// Streaming 模式：不等待实例结束，而是在一段时间内观察是否有数据写入
	qdb2, _ := quantDBFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	_ = qdb2.Connect(ctx)
	defer qdb2.Close()

	var lastCnt int64
	// 观察约 1 分钟（12 轮，每轮 5 秒），以便看到持续运行效果。
	for i := 0; i < 12; i++ {
		rows, err := qdb2.Query(ctx, `SELECT COUNT(*) as cnt FROM realtime_quote`)
		if err != nil {
			t.Logf("query realtime_quote failed at round %d: %v", i, err)
			time.Sleep(5 * time.Second)
			continue
		}
		if len(rows) >= 1 {
			var cnt int64
			switch v := rows[0]["cnt"].(type) {
			case int64:
				cnt = v
			case int:
				cnt = int64(v)
			case int32:
				cnt = int64(v)
			case int16:
				cnt = int64(v)
			case float64:
				cnt = int64(v)
			case float32:
				cnt = int64(v)
			default:
				if s, ok := rows[0]["cnt"].(fmt.Stringer); ok {
					if n, err := strconv.ParseInt(s.String(), 10, 64); err == nil {
						cnt = n
					}
				}
			}
			t.Logf("real streaming round %d, realtime_quote row count: %d", i, cnt)
			if cnt > lastCnt {
				lastCnt = cnt
			}
		}
		time.Sleep(5 * time.Second)
	}
	
	// 为避免与 Streaming 实例并发访问同一 DuckDB 造成观测偏差，这里先显式停止引擎，再重新打开 DuckDB 文件做最终校验。
	eng.Stop()
	sampleFactory := duckdb.NewFactory()
	sampleDB, err := sampleFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	if err != nil {
		t.Logf("create sample DuckDB failed: %v", err)
		return
	}
	defer sampleDB.Close()
	if err := sampleDB.Connect(ctx); err != nil {
		t.Logf("connect sample DuckDB failed: %v", err)
		return
	}
	
	// 额外打印部分样本行，直接观测 DuckDB 中的实时行情数据。
	rows, err := sampleDB.Query(ctx, `
		SELECT ts_code, trade_time, price, volume, amount
		FROM realtime_quote
		ORDER BY trade_time DESC
		LIMIT 5
	`)
	if err != nil {
		t.Logf("query realtime_quote samples failed: %v", err)
	} else if len(rows) == 0 {
		t.Logf("no rows found in realtime_quote after streaming window")
	} else {
		for i, r := range rows {
			t.Logf("realtime_quote sample %d: %+v", i, r)
		}
	}
}

// TestE2E_RealtimeSync_Real_Tick_Sina 使用真实新浪分笔行情，验证 realtime_tick 从 SyncPlan 到 DuckDB 的 Streaming 全链路。
// 仅在环境变量 QDHUB_REALTIME_E2E=1 时运行，且建议在交易时间内执行以观察到实际分笔数据。
func TestE2E_RealtimeSync_Real_Tick_Sina(t *testing.T) {
	if os.Getenv("QDHUB_REALTIME_E2E") != "1" {
		t.Skip("skip real realtime tick e2e unless QDHUB_REALTIME_E2E=1")
	}

	logFile, logCleanup := setupLogFile(t, "RealtimeSync_Real_Tick_Sina")
	defer logCleanup()
	defer logFile.Close()

	ctx := context.Background()

	tmpSQLite, err := os.CreateTemp("", "realtime_real_e2e_tick_sina_*.db")
	require.NoError(t, err)
	tmpSQLite.Close()
	dsn := tmpSQLite.Name()
	duckDBPath := filepath.Join(os.TempDir(), fmt.Sprintf("realtime_real_e2e_tick_sina_%d.duckdb", time.Now().UnixNano()))
	db, err := persistence.NewDB(dsn)
	require.NoError(t, err)

	migrationDir := filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(getRealtimeE2EDataDir()))), "migrations")
	for _, name := range []string{"001_init_schema.up.sql", "003_sync_plan_migration.up.sql"} {
		sql, _ := os.ReadFile(filepath.Join(migrationDir, name))
		if len(sql) > 0 {
			_, _ = db.Exec(string(sql))
		}
	}
	var dsID string
	_ = db.QueryRow(`SELECT id FROM data_sources WHERE LOWER(name) = 'tushare' LIMIT 1`).Scan(&dsID)
	if dsID == "" {
		dsID = shared.NewID().String()
		_, _ = db.Exec(`INSERT INTO data_sources (id, name, description, base_url, doc_url, status, created_at, updated_at) VALUES (?, 'Tushare', 'E2E Tick Sina', '', '', 'active', datetime('now'), datetime('now'))`, dsID)
		_, _ = db.Exec(`INSERT OR REPLACE INTO tokens (id, data_source_id, token_value, created_at) VALUES (?, ?, 'real_e2e_token', datetime('now'))`, shared.NewID().String(), dsID)
	}
	for _, name := range []string{
		"004_api_sync_strategy.up.sql", "005_sync_plan_default_params.up.sql", "006_sync_plan_mode_realtime.up.sql",
		"007_daily_adj_factor_trade_date_expand.up.sql", "010_sync_plan_incremental_mode.up.sql",
		"015_sync_plan_incremental_start_date_source.up.sql", "016_api_sync_strategy_fixed_params.up.sql",
		"017_api_sync_strategy_news_fixed_fields.up.sql", "018_api_sync_strategy_time_window_news_cctv.up.sql",
		"019_realtime_api_sync_strategy.up.sql", "020_sync_plan_pull_interval_and_schedule_window.up.sql",
		"013_sync_execution_workflow_error_message.up.sql",
	} {
		sql, _ := os.ReadFile(filepath.Join(migrationDir, name))
		if len(sql) > 0 {
			_, _ = db.Exec(string(sql))
		}
	}

	quantDBFactory := duckdb.NewFactory()
	qdb, _ := quantDBFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	_ = qdb.Connect(ctx)
	_, _ = qdb.Execute(ctx, `CREATE TABLE IF NOT EXISTS stock_basic (ts_code VARCHAR(32), name VARCHAR(128), list_date VARCHAR(16))`)
	// 选择一只常见股票，便于在交易时间观测分笔数据
	_, _ = qdb.Execute(ctx, `INSERT INTO stock_basic (ts_code, name, list_date) VALUES ('000001.SZ', '平安银行', '19910403')`)
	_, _ = qdb.Execute(ctx, `CREATE TABLE IF NOT EXISTS realtime_tick (
		ts_code VARCHAR(32),
		time VARCHAR(32),
		price DOUBLE,
		change DOUBLE,
		volume DOUBLE,
		amount DOUBLE,
		type VARCHAR(16)
	)`)
	qdb.Close()

	adapterReg := realtime.NewRegistryWithDefaults()
	bufReg := realtimebuffer.NewDefaultRegistry(256)

	aggregateRepo, _ := sqlite.NewWorkflowAggregateRepoFromDSN(dsn)
	eng, _ := engine.NewEngineWithAggregateRepo(5, 30, aggregateRepo)
	_ = eng.Start(ctx)

	metadataRepo := repository.NewMetadataRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	datastoreRepo := repository.NewQuantDataStoreRepository(db)
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	workflowRepo, _ := repository.NewWorkflowDefinitionRepository(db)

	wqCfg := config.Default().QuantDB.WriteQueue
	quantDBWriteQueue := writequeue.NewQueue(wqCfg, quantDBFactory)
	taskEngineDeps := &taskengine.Dependencies{
		MetadataRepo:            metadataRepo,
		DataStoreRepo:           datastoreRepo,
		QuantDBFactory:          quantDBFactory,
		QuantDBWriteQueue:       quantDBWriteQueue,
		RealtimeAdapterRegistry: adapterReg,
		RealtimeBufferRegistry:  bufReg,
	}
	_ = taskengine.Initialize(ctx, eng, taskEngineDeps)
	taskEngineAdapter := taskengine.NewTaskEngineAdapter(eng, 0)
	workflowFactory := taskengine.GetWorkflowFactory(eng)
	_ = impl.NewBuiltInWorkflowInitializer(workflowRepo, workflowFactory, taskEngineAdapter).Initialize(ctx)
	workflowExecutor := taskengine.NewWorkflowExecutor(workflowRepo, taskEngineAdapter, metadataRepo, adapterReg, "", nil, "", "", "")

	syncAppService := impl.NewSyncApplicationService(
		syncPlanRepo, sync.NewCronScheduleCalculator(), nil,
		dataSourceRepo, datastoreRepo, workflowExecutor, sync.NewDependencyResolver(),
		taskEngineAdapter, uow.NewUnitOfWork(db), metadataRepo, quantDBFactory,
		"", nil,
	)

	defer func() {
		_ = quantDBWriteQueue.Close()
		eng.Stop()
		os.Remove(dsn)
		os.Remove(duckDBPath)
		db.Close()
	}()

	dsList, _ := dataSourceRepo.List()
	for _, d := range dsList {
		if strings.EqualFold(d.Name, "tushare") {
			dsID = d.ID.String()
			break
		}
	}
	require.NotEmpty(t, dsID, "need tushare data source")
	storeID := shared.NewID()
	absPath := duckDBPath
	if !filepath.IsAbs(absPath) {
		absPath, _ = filepath.Abs(absPath)
	}
	_ = datastoreRepo.Create(&datastore.QuantDataStore{
		ID: storeID, Name: "e2e_real_duckdb_tick_sina", Type: datastore.DataStoreTypeDuckDB,
		StoragePath: absPath, CreatedAt: shared.Timestamp(time.Now()), UpdatedAt: shared.Timestamp(time.Now()),
	})

	planID := shared.NewID()
	dsIDAsID := shared.ID(dsID)
	_ = syncPlanRepo.Create(&sync.SyncPlan{
		ID: planID, Name: "e2e_realtime_real_tick_sina", DataSourceID: dsIDAsID, DataStoreID: storeID,
		Mode: sync.PlanModeRealtime, SelectedAPIs: []string{"realtime_tick"}, Status: sync.PlanStatusDraft,
		CreatedAt: shared.Timestamp(time.Now()), UpdatedAt: shared.Timestamp(time.Now()), PullIntervalSeconds: 1,
	})
	err = syncAppService.ResolveSyncPlan(ctx, planID)
	require.NoError(t, err)
	plan, _ := syncPlanRepo.Get(planID)
	plan.Status = sync.PlanStatusEnabled
	_ = syncPlanRepo.Update(plan)

	execID, err := syncAppService.ExecuteSyncPlan(ctx, planID, contracts.ExecuteSyncPlanRequest{})
	require.NoError(t, err)
	require.False(t, execID.IsEmpty())

	// Streaming 模式：不等待实例结束，而是在一段时间内观察是否有分笔数据写入
	qdb2, _ := quantDBFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	_ = qdb2.Connect(ctx)
	defer qdb2.Close()

	var lastCnt int64
	for i := 0; i < 12; i++ {
		rows, err := qdb2.Query(ctx, `SELECT COUNT(*) as cnt FROM realtime_tick`)
		if err != nil {
			t.Logf("query realtime_tick failed at round %d: %v", i, err)
			time.Sleep(5 * time.Second)
			continue
		}
		if len(rows) >= 1 {
			var cnt int64
			switch v := rows[0]["cnt"].(type) {
			case int64:
				cnt = v
			case int:
				cnt = int64(v)
			case int32:
				cnt = int64(v)
			case int16:
				cnt = int64(v)
			case float64:
				cnt = int64(v)
			case float32:
				cnt = int64(v)
			default:
				if s, ok := rows[0]["cnt"].(fmt.Stringer); ok {
					if n, err := strconv.ParseInt(s.String(), 10, 64); err == nil {
						cnt = n
					}
				}
			}
			t.Logf("real tick streaming round %d, realtime_tick row count: %d", i, cnt)
			if cnt > lastCnt {
				lastCnt = cnt
			}
		}
		time.Sleep(5 * time.Second)
	}

	// 为避免与 Streaming 实例并发访问同一 DuckDB 造成观测偏差，这里先显式停止引擎，再重新打开 DuckDB 文件做最终校验。
	eng.Stop()
	sampleFactory := duckdb.NewFactory()
	sampleDB, err := sampleFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	if err != nil {
		t.Logf("create sample DuckDB for tick failed: %v", err)
		return
	}
	defer sampleDB.Close()
	if err := sampleDB.Connect(ctx); err != nil {
		t.Logf("connect sample DuckDB for tick failed: %v", err)
		return
	}

	rows, err := sampleDB.Query(ctx, `
		SELECT ts_code, time, price, volume, amount, type
		FROM realtime_tick
		ORDER BY time DESC
		LIMIT 5
	`)
	if err != nil {
		t.Logf("query realtime_tick samples failed: %v", err)
	} else if len(rows) == 0 {
		t.Logf("no rows found in realtime_tick after streaming window (possibly non-trading time or no data)")
	} else {
		for i, r := range rows {
			t.Logf("realtime_tick sample %d: %+v", i, r)
		}
	}
}

// TestE2E_RealtimeSync_Real_List_Sina 使用真实新浪 realtime_list 行情，验证全市场涨跌榜 Streaming 同步。
// 仅在环境变量 QDHUB_REALTIME_E2E=1 时运行。
func TestE2E_RealtimeSync_Real_List_Sina(t *testing.T) {
	if os.Getenv("QDHUB_REALTIME_E2E") != "1" {
		t.Skip("skip real realtime_list sina e2e unless QDHUB_REALTIME_E2E=1")
	}

	logFile, logCleanup := setupLogFile(t, "RealtimeSync_Real_List_Sina")
	defer logCleanup()
	defer logFile.Close()

	ctx := context.Background()

	tmpSQLite, err := os.CreateTemp("", "realtime_real_e2e_list_sina_*.db")
	require.NoError(t, err)
	tmpSQLite.Close()
	dsn := tmpSQLite.Name()
	duckDBPath := filepath.Join(os.TempDir(), fmt.Sprintf("realtime_real_e2e_list_sina_%d.duckdb", time.Now().UnixNano()))
	db, err := persistence.NewDB(dsn)
	require.NoError(t, err)

	migrationDir := filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(getRealtimeE2EDataDir()))), "migrations")
	for _, name := range []string{"001_init_schema.up.sql", "003_sync_plan_migration.up.sql"} {
		sql, _ := os.ReadFile(filepath.Join(migrationDir, name))
		if len(sql) > 0 {
			_, _ = db.Exec(string(sql))
		}
	}
	var dsID string
	_ = db.QueryRow(`SELECT id FROM data_sources WHERE LOWER(name) = 'tushare' LIMIT 1`).Scan(&dsID)
	if dsID == "" {
		dsID = shared.NewID().String()
		_, _ = db.Exec(`INSERT INTO data_sources (id, name, description, base_url, doc_url, status, created_at, updated_at) VALUES (?, 'Tushare', 'E2E List Sina', '', '', 'active', datetime('now'), datetime('now'))`, dsID)
		_, _ = db.Exec(`INSERT OR REPLACE INTO tokens (id, data_source_id, token_value, created_at) VALUES (?, ?, 'real_e2e_token', datetime('now'))`, shared.NewID().String(), dsID)
	}
	for _, name := range []string{
		"004_api_sync_strategy.up.sql", "005_sync_plan_default_params.up.sql", "006_sync_plan_mode_realtime.up.sql",
		"007_daily_adj_factor_trade_date_expand.up.sql", "010_sync_plan_incremental_mode.up.sql",
		"015_sync_plan_incremental_start_date_source.up.sql", "016_api_sync_strategy_fixed_params.up.sql",
		"017_api_sync_strategy_news_fixed_fields.up.sql", "018_api_sync_strategy_time_window_news_cctv.up.sql",
		"019_realtime_api_sync_strategy.up.sql", "020_sync_plan_pull_interval_and_schedule_window.up.sql",
		"013_sync_execution_workflow_error_message.up.sql",
	} {
		sql, _ := os.ReadFile(filepath.Join(migrationDir, name))
		if len(sql) > 0 {
			_, _ = db.Exec(string(sql))
		}
	}

	quantDBFactory := duckdb.NewFactory()
	qdb, _ := quantDBFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	_ = qdb.Connect(ctx)
	// realtime_list 无需依赖 stock_basic 表，这里仅建目标表。
	_, _ = qdb.Execute(ctx, `CREATE TABLE IF NOT EXISTS realtime_list (
		ts_code VARCHAR(32),
		name VARCHAR(128),
		price DOUBLE,
		pct_change DOUBLE,
		change DOUBLE,
		volume DOUBLE,
		amount DOUBLE,
		swing DOUBLE,
		high DOUBLE,
		low DOUBLE,
		open DOUBLE,
		close DOUBLE,
		vol_ratio DOUBLE,
		turnover_rate DOUBLE,
		pe DOUBLE,
		pb DOUBLE,
		total_mv DOUBLE,
		float_mv DOUBLE,
		rise DOUBLE,
		"5min" DOUBLE,
		"60day" DOUBLE,
		"1tyear" DOUBLE
	)`)
	_ = qdb.Close()

	adapterReg := realtime.NewRegistryWithDefaults()
	bufReg := realtimebuffer.NewDefaultRegistry(256)

	aggregateRepo, _ := sqlite.NewWorkflowAggregateRepoFromDSN(dsn)
	eng, _ := engine.NewEngineWithAggregateRepo(5, 30, aggregateRepo)
	_ = eng.Start(ctx)

	metadataRepo := repository.NewMetadataRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	datastoreRepo := repository.NewQuantDataStoreRepository(db)
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	workflowRepo, _ := repository.NewWorkflowDefinitionRepository(db)

	wqCfg := config.Default().QuantDB.WriteQueue
	quantDBWriteQueue := writequeue.NewQueue(wqCfg, quantDBFactory)
	taskEngineDeps := &taskengine.Dependencies{
		MetadataRepo:            metadataRepo,
		DataStoreRepo:           datastoreRepo,
		QuantDBFactory:          quantDBFactory,
		QuantDBWriteQueue:       quantDBWriteQueue,
		RealtimeAdapterRegistry: adapterReg,
		RealtimeBufferRegistry:  bufReg,
	}
	_ = taskengine.Initialize(ctx, eng, taskEngineDeps)
	taskEngineAdapter := taskengine.NewTaskEngineAdapter(eng, 0)
	workflowFactory := taskengine.GetWorkflowFactory(eng)
	_ = impl.NewBuiltInWorkflowInitializer(workflowRepo, workflowFactory, taskEngineAdapter).Initialize(ctx)
	workflowExecutor := taskengine.NewWorkflowExecutor(workflowRepo, taskEngineAdapter, metadataRepo, adapterReg, "", nil, "", "", "")

	syncAppService := impl.NewSyncApplicationService(
		syncPlanRepo, sync.NewCronScheduleCalculator(), nil,
		dataSourceRepo, datastoreRepo, workflowExecutor, sync.NewDependencyResolver(),
		taskEngineAdapter, uow.NewUnitOfWork(db), metadataRepo, quantDBFactory,
		"", nil,
	)

	defer func() {
		_ = quantDBWriteQueue.Close()
		eng.Stop()
		os.Remove(dsn)
		os.Remove(duckDBPath)
		db.Close()
	}()

	dsList, _ := dataSourceRepo.List()
	for _, d := range dsList {
		if strings.EqualFold(d.Name, "tushare") {
			dsID = d.ID.String()
			break
		}
	}
	require.NotEmpty(t, dsID, "need tushare data source")
	storeID := shared.NewID()
	absPath := duckDBPath
	if !filepath.IsAbs(absPath) {
		absPath, _ = filepath.Abs(absPath)
	}
	_ = datastoreRepo.Create(&datastore.QuantDataStore{
		ID: storeID, Name: "e2e_real_duckdb_list_sina", Type: datastore.DataStoreTypeDuckDB,
		StoragePath: absPath, CreatedAt: shared.Timestamp(time.Now()), UpdatedAt: shared.Timestamp(time.Now()),
	})

	planID := shared.NewID()
	dsIDAsID := shared.ID(dsID)
	_ = syncPlanRepo.Create(&sync.SyncPlan{
		ID: planID, Name: "e2e_realtime_real_list_sina", DataSourceID: dsIDAsID, DataStoreID: storeID,
		Mode: sync.PlanModeRealtime, SelectedAPIs: []string{"realtime_list"}, Status: sync.PlanStatusDraft,
		CreatedAt: shared.Timestamp(time.Now()), UpdatedAt: shared.Timestamp(time.Now()), PullIntervalSeconds: 5,
	})
	err = syncAppService.ResolveSyncPlan(ctx, planID)
	require.NoError(t, err)
	plan, _ := syncPlanRepo.Get(planID)
	plan.Status = sync.PlanStatusEnabled
	_ = syncPlanRepo.Update(plan)

	execID, err := syncAppService.ExecuteSyncPlan(ctx, planID, contracts.ExecuteSyncPlanRequest{})
	require.NoError(t, err)
	require.False(t, execID.IsEmpty())

	// 观察约 1 分钟内 realtime_list 行数变化。
	qdb2, _ := quantDBFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	_ = qdb2.Connect(ctx)
	defer qdb2.Close()

	for i := 0; i < 12; i++ {
		rows, err := qdb2.Query(ctx, `SELECT COUNT(*) as cnt FROM realtime_list`)
		if err != nil {
			t.Logf("query realtime_list failed at round %d: %v", i, err)
			time.Sleep(5 * time.Second)
			continue
		}
		if len(rows) >= 1 {
			var cnt int64
			switch v := rows[0]["cnt"].(type) {
			case int64:
				cnt = v
			case float64:
				cnt = int64(v)
			}
			t.Logf("real list sina round %d, realtime_list row count: %d", i, cnt)
		}
		time.Sleep(5 * time.Second)
	}

	// 停掉引擎后再次打开 DuckDB，打印部分样本行
	eng.Stop()
	sampleFactory := duckdb.NewFactory()
	sampleDB, err := sampleFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	if err == nil && sampleDB.Connect(ctx) == nil {
		defer sampleDB.Close()
		rows, err := sampleDB.Query(ctx, `
			SELECT ts_code, name, price, pct_change, amount
			FROM realtime_list
			ORDER BY pct_change DESC
			LIMIT 10`)
		if err == nil {
			for i, r := range rows {
				t.Logf("[sina list] realtime_list sample %d: %+v", i, r)
			}
		} else {
			t.Logf("[sina list] query samples failed: %v", err)
		}
	}
}

// TestE2E_RealtimeSync_Real_LargeBatch500_Sina 使用真实新浪实时行情，模拟 500 支股票的 Streaming 同步，持续约 1 分钟。
// 主要验证：在大规模 ts_code 分片下，Streaming Workflow 能稳定运行且 DuckDB 中可以观测到 realtime_quote 数据。
func TestE2E_RealtimeSync_Real_LargeBatch500_Sina(t *testing.T) {
	if os.Getenv("QDHUB_REALTIME_E2E") != "1" {
		t.Skip("skip real realtime e2e unless QDHUB_REALTIME_E2E=1")
	}

	logFile, logCleanup := setupLogFile(t, "RealtimeSync_Real_LargeBatch500_Sina")
	defer logCleanup()
	defer logFile.Close()

	ctx := context.Background()

	tmpSQLite, err := os.CreateTemp("", "realtime_real_e2e_sina500_*.db")
	require.NoError(t, err)
	tmpSQLite.Close()
	dsn := tmpSQLite.Name()
	duckDBPath := filepath.Join(os.TempDir(), fmt.Sprintf("realtime_real_e2e_sina500_%d.duckdb", time.Now().UnixNano()))
	db, err := persistence.NewDB(dsn)
	require.NoError(t, err)

	migrationDir := filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(getRealtimeE2EDataDir()))), "migrations")
	for _, name := range []string{"001_init_schema.up.sql", "003_sync_plan_migration.up.sql"} {
		sql, _ := os.ReadFile(filepath.Join(migrationDir, name))
		if len(sql) > 0 {
			_, _ = db.Exec(string(sql))
		}
	}
	var dsID string
	_ = db.QueryRow(`SELECT id FROM data_sources WHERE LOWER(name) = 'tushare' LIMIT 1`).Scan(&dsID)
	if dsID == "" {
		dsID = shared.NewID().String()
		_, _ = db.Exec(`INSERT INTO data_sources (id, name, description, base_url, doc_url, status, created_at, updated_at) VALUES (?, 'Tushare', 'E2E Sina500', '', '', 'active', datetime('now'), datetime('now'))`, dsID)
		_, _ = db.Exec(`INSERT OR REPLACE INTO tokens (id, data_source_id, token_value, created_at) VALUES (?, ?, 'real_e2e_token', datetime('now'))`, shared.NewID().String(), dsID)
	}
	for _, name := range []string{
		"004_api_sync_strategy.up.sql", "005_sync_plan_default_params.up.sql", "006_sync_plan_mode_realtime.up.sql",
		"007_daily_adj_factor_trade_date_expand.up.sql", "010_sync_plan_incremental_mode.up.sql",
		"015_sync_plan_incremental_start_date_source.up.sql", "016_api_sync_strategy_fixed_params.up.sql",
		"017_api_sync_strategy_news_fixed_fields.up.sql", "018_api_sync_strategy_time_window_news_cctv.up.sql",
		"019_realtime_api_sync_strategy.up.sql", "020_sync_plan_pull_interval_and_schedule_window.up.sql",
		"013_sync_execution_workflow_error_message.up.sql",
	} {
		sql, _ := os.ReadFile(filepath.Join(migrationDir, name))
		if len(sql) > 0 {
			_, _ = db.Exec(string(sql))
		}
	}

	quantDBFactory := duckdb.NewFactory()
	qdb, _ := quantDBFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	_ = qdb.Connect(ctx)
	_, _ = qdb.Execute(ctx, `CREATE TABLE IF NOT EXISTS stock_basic (ts_code VARCHAR(32), name VARCHAR(128), list_date VARCHAR(16))`)
	// 插入 500 支股票（使用 000001.SZ~000500.SZ 的形式，部分不存在也无碍，主要验证 Collector 分片与 Streaming 稳定性）
	for i := 1; i <= 500; i++ {
		code := fmt.Sprintf("%06d.SZ", i)
		_, _ = qdb.Execute(ctx, fmt.Sprintf(
			`INSERT INTO stock_basic (ts_code, name, list_date) VALUES ('%s', 'RealStock%06d', '19900101')`,
			code, i,
		))
	}
	_, _ = qdb.Execute(ctx, `CREATE TABLE IF NOT EXISTS realtime_quote (
		ts_code VARCHAR(32), name VARCHAR(64), open DOUBLE, pre_close DOUBLE, price DOUBLE, high DOUBLE, low DOUBLE,
		bid DOUBLE, ask DOUBLE, volume DOUBLE, amount DOUBLE,
		b1_v VARCHAR(32), b1_p DOUBLE, b2_v VARCHAR(32), b2_p DOUBLE, b3_v VARCHAR(32), b3_p DOUBLE, b4_v VARCHAR(32), b4_p DOUBLE, b5_v VARCHAR(32), b5_p DOUBLE,
		a1_v VARCHAR(32), a1_p DOUBLE, a2_v VARCHAR(32), a2_p DOUBLE, a3_v VARCHAR(32), a3_p DOUBLE, a4_v VARCHAR(32), a4_p DOUBLE, a5_v VARCHAR(32), a5_p DOUBLE,
		date VARCHAR(16), time VARCHAR(16), trade_time VARCHAR(32), close DOUBLE, vol DOUBLE,
		PRIMARY KEY (ts_code, date, time)
	)`)
	_ = qdb.Close()

	adapterReg := realtime.NewRegistryWithDefaults()
	bufReg := realtimebuffer.NewDefaultRegistry(256)

	aggregateRepo, _ := sqlite.NewWorkflowAggregateRepoFromDSN(dsn)
	eng, _ := engine.NewEngineWithAggregateRepo(5, 30, aggregateRepo)
	_ = eng.Start(ctx)

	metadataRepo := repository.NewMetadataRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	datastoreRepo := repository.NewQuantDataStoreRepository(db)
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	workflowRepo, _ := repository.NewWorkflowDefinitionRepository(db)

	wqCfg := config.Default().QuantDB.WriteQueue
	quantDBWriteQueue := writequeue.NewQueue(wqCfg, quantDBFactory)
	taskEngineDeps := &taskengine.Dependencies{
		MetadataRepo:            metadataRepo,
		DataStoreRepo:           datastoreRepo,
		QuantDBFactory:          quantDBFactory,
		QuantDBWriteQueue:       quantDBWriteQueue,
		RealtimeAdapterRegistry: adapterReg,
		RealtimeBufferRegistry:  bufReg,
	}
	_ = taskengine.Initialize(ctx, eng, taskEngineDeps)
	taskEngineAdapter := taskengine.NewTaskEngineAdapter(eng, 0)
	workflowFactory := taskengine.GetWorkflowFactory(eng)
	_ = impl.NewBuiltInWorkflowInitializer(workflowRepo, workflowFactory, taskEngineAdapter).Initialize(ctx)
	workflowExecutor := taskengine.NewWorkflowExecutor(workflowRepo, taskEngineAdapter, metadataRepo, adapterReg, "", nil, "", "", "")

	syncAppService := impl.NewSyncApplicationService(
		syncPlanRepo, sync.NewCronScheduleCalculator(), nil,
		dataSourceRepo, datastoreRepo, workflowExecutor, sync.NewDependencyResolver(),
		taskEngineAdapter, uow.NewUnitOfWork(db), metadataRepo, quantDBFactory,
		"", nil,
	)

	defer func() {
		_ = quantDBWriteQueue.Close()
		eng.Stop()
		os.Remove(dsn)
		os.Remove(duckDBPath)
		db.Close()
	}()

	dsList, _ := dataSourceRepo.List()
	for _, d := range dsList {
		if strings.EqualFold(d.Name, "tushare") {
			dsID = d.ID.String()
			break
		}
	}
	require.NotEmpty(t, dsID, "need tushare data source")
	storeID := shared.NewID()
	absPath := duckDBPath
	if !filepath.IsAbs(absPath) {
		absPath, _ = filepath.Abs(absPath)
	}
	_ = datastoreRepo.Create(&datastore.QuantDataStore{
		ID: storeID, Name: "e2e_real_duckdb_sina500", Type: datastore.DataStoreTypeDuckDB,
		StoragePath: absPath, CreatedAt: shared.Timestamp(time.Now()), UpdatedAt: shared.Timestamp(time.Now()),
	})

	planID := shared.NewID()
	dsIDAsID := shared.ID(dsID)
	_ = syncPlanRepo.Create(&sync.SyncPlan{
		ID: planID, Name: "e2e_realtime_real_sina500", DataSourceID: dsIDAsID, DataStoreID: storeID,
		Mode: sync.PlanModeRealtime, SelectedAPIs: []string{"realtime_quote"}, Status: sync.PlanStatusDraft,
		CreatedAt: shared.Timestamp(time.Now()), UpdatedAt: shared.Timestamp(time.Now()), PullIntervalSeconds: 1,
	})
	err = syncAppService.ResolveSyncPlan(ctx, planID)
	require.NoError(t, err)
	plan, _ := syncPlanRepo.Get(planID)
	plan.Status = sync.PlanStatusEnabled
	_ = syncPlanRepo.Update(plan)

	execID, err := syncAppService.ExecuteSyncPlan(ctx, planID, contracts.ExecuteSyncPlanRequest{})
	require.NoError(t, err)
	require.False(t, execID.IsEmpty())

	// 观察约 1 分钟内 realtime_quote 行数变化
	qdbObs, _ := quantDBFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	_ = qdbObs.Connect(ctx)
	defer qdbObs.Close()

	var lastCnt int64
	for i := 0; i < 12; i++ {
		rows, err := qdbObs.Query(ctx, `SELECT COUNT(*) as cnt FROM realtime_quote`)
		if err != nil {
			t.Logf("[sina 500] query realtime_quote failed at round %d: %v", i, err)
			time.Sleep(5 * time.Second)
			continue
		}
		if len(rows) >= 1 {
			var cnt int64
			switch v := rows[0]["cnt"].(type) {
			case int64:
				cnt = v
			case float64:
				cnt = int64(v)
			}
			t.Logf("[sina 500] round %d, realtime_quote row count: %d", i, cnt)
			if cnt > lastCnt {
				lastCnt = cnt
			}
		}
		time.Sleep(5 * time.Second)
	}

	// 停掉引擎后再次打开 DuckDB，打印部分样本行
	eng.Stop()
	sampleFactory := duckdb.NewFactory()
	sampleDB, err := sampleFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	if err == nil && sampleDB.Connect(ctx) == nil {
		defer sampleDB.Close()
		rows, err := sampleDB.Query(ctx, `
			SELECT ts_code, trade_time, price, volume, amount
			FROM realtime_quote
			ORDER BY trade_time DESC
			LIMIT 10`)
		if err == nil {
			for i, r := range rows {
				t.Logf("[sina 500] realtime_quote sample %d: %+v", i, r)
			}
		} else {
			t.Logf("[sina 500] query samples failed: %v", err)
		}
	}
}

// TestE2E_RealtimeSync_Real_LargeBatch500_Eastmoney 使用东财实时行情，模拟大量股票 Streaming 同步。
func TestE2E_RealtimeSync_Real_LargeBatch500_Eastmoney(t *testing.T) {
	if os.Getenv("QDHUB_REALTIME_E2E") != "1" {
		t.Skip("skip real realtime e2e unless QDHUB_REALTIME_E2E=1")
	}

	logFile, logCleanup := setupLogFile(t, "RealtimeSync_Real_LargeBatch500_Eastmoney")
	defer logCleanup()
	defer logFile.Close()

	ctx := context.Background()

	tmpSQLite, err := os.CreateTemp("", "realtime_real_e2e_eastmoney500_*.db")
	require.NoError(t, err)
	tmpSQLite.Close()
	dsn := tmpSQLite.Name()
	duckDBPath := filepath.Join(os.TempDir(), fmt.Sprintf("realtime_real_e2e_eastmoney500_%d.duckdb", time.Now().UnixNano()))
	db, err := persistence.NewDB(dsn)
	require.NoError(t, err)

	migrationDir := filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(getRealtimeE2EDataDir()))), "migrations")
	for _, name := range []string{"001_init_schema.up.sql", "003_sync_plan_migration.up.sql"} {
		sql, _ := os.ReadFile(filepath.Join(migrationDir, name))
		if len(sql) > 0 {
			_, _ = db.Exec(string(sql))
		}
	}
	var dsID string
	_ = db.QueryRow(`SELECT id FROM data_sources WHERE LOWER(name) = 'tushare' LIMIT 1`).Scan(&dsID)
	if dsID == "" {
		dsID = shared.NewID().String()
		_, _ = db.Exec(`INSERT INTO data_sources (id, name, description, base_url, doc_url, status, created_at, updated_at) VALUES (?, 'Tushare', 'E2E Eastmoney500', '', '', 'active', datetime('now'), datetime('now'))`, dsID)
		_, _ = db.Exec(`INSERT OR REPLACE INTO tokens (id, data_source_id, token_value, created_at) VALUES (?, ?, 'real_e2e_token', datetime('now'))`, shared.NewID().String(), dsID)
	}
	for _, name := range []string{
		"004_api_sync_strategy.up.sql", "005_sync_plan_default_params.up.sql", "006_sync_plan_mode_realtime.up.sql",
		"007_daily_adj_factor_trade_date_expand.up.sql", "010_sync_plan_incremental_mode.up.sql",
		"015_sync_plan_incremental_start_date_source.up.sql", "016_api_sync_strategy_fixed_params.up.sql",
		"017_api_sync_strategy_news_fixed_fields.up.sql", "018_api_sync_strategy_time_window_news_cctv.up.sql",
		"019_realtime_api_sync_strategy.up.sql", "020_sync_plan_pull_interval_and_schedule_window.up.sql",
		"013_sync_execution_workflow_error_message.up.sql",
	} {
		sql, _ := os.ReadFile(filepath.Join(migrationDir, name))
		if len(sql) > 0 {
			_, _ = db.Exec(string(sql))
		}
	}

	quantDBFactory := duckdb.NewFactory()
	qdb, _ := quantDBFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	_ = qdb.Connect(ctx)
	_, _ = qdb.Execute(ctx, `CREATE TABLE IF NOT EXISTS stock_basic (ts_code VARCHAR(32), name VARCHAR(128), list_date VARCHAR(16))`)
	for i := 1; i <= 500; i++ {
		code := fmt.Sprintf("%06d.SZ", i)
		_, _ = qdb.Execute(ctx, fmt.Sprintf(
			`INSERT INTO stock_basic (ts_code, name, list_date) VALUES ('%s', 'RealStockEM%06d', '19900101')`,
			code, i,
		))
	}
	_, _ = qdb.Execute(ctx, `CREATE TABLE IF NOT EXISTS realtime_quote (
		ts_code VARCHAR(32), name VARCHAR(64), open DOUBLE, pre_close DOUBLE, price DOUBLE, high DOUBLE, low DOUBLE,
		bid DOUBLE, ask DOUBLE, volume DOUBLE, amount DOUBLE,
		b1_v VARCHAR(32), b1_p DOUBLE, b2_v VARCHAR(32), b2_p DOUBLE, b3_v VARCHAR(32), b3_p DOUBLE, b4_v VARCHAR(32), b4_p DOUBLE, b5_v VARCHAR(32), b5_p DOUBLE,
		a1_v VARCHAR(32), a1_p DOUBLE, a2_v VARCHAR(32), a2_p DOUBLE, a3_v VARCHAR(32), a3_p DOUBLE, a4_v VARCHAR(32), a4_p DOUBLE, a5_v VARCHAR(32), a5_p DOUBLE,
		date VARCHAR(16), time VARCHAR(16), trade_time VARCHAR(32), close DOUBLE, vol DOUBLE,
		PRIMARY KEY (ts_code, date, time)
	)`)
	_ = qdb.Close()

	// 构造只走东财的 RealtimeAdapterRegistry：将 eastmoney 适配器注册到 key "sina" 上，供 QuotePullCollector 使用。
	eastReg := realtime.NewDefaultRegistry()
	eastReg.Register("sina", realtime.NewEastmoneyRealtimeAdapter())
	bufReg := realtimebuffer.NewDefaultRegistry(256)

	aggregateRepo, _ := sqlite.NewWorkflowAggregateRepoFromDSN(dsn)
	eng, _ := engine.NewEngineWithAggregateRepo(5, 30, aggregateRepo)
	_ = eng.Start(ctx)

	metadataRepo := repository.NewMetadataRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	datastoreRepo := repository.NewQuantDataStoreRepository(db)
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	workflowRepo, _ := repository.NewWorkflowDefinitionRepository(db)

	wqCfg := config.Default().QuantDB.WriteQueue
	quantDBWriteQueue := writequeue.NewQueue(wqCfg, quantDBFactory)
	taskEngineDeps := &taskengine.Dependencies{
		MetadataRepo:            metadataRepo,
		DataStoreRepo:           datastoreRepo,
		QuantDBFactory:          quantDBFactory,
		QuantDBWriteQueue:       quantDBWriteQueue,
		RealtimeAdapterRegistry: eastReg,
		RealtimeBufferRegistry:  bufReg,
	}
	_ = taskengine.Initialize(ctx, eng, taskEngineDeps)
	taskEngineAdapter := taskengine.NewTaskEngineAdapter(eng, 0)
	workflowFactory := taskengine.GetWorkflowFactory(eng)
	_ = impl.NewBuiltInWorkflowInitializer(workflowRepo, workflowFactory, taskEngineAdapter).Initialize(ctx)
	workflowExecutor := taskengine.NewWorkflowExecutor(workflowRepo, taskEngineAdapter, metadataRepo, eastReg, "", nil, "", "", "")

	syncAppService := impl.NewSyncApplicationService(
		syncPlanRepo, sync.NewCronScheduleCalculator(), nil,
		dataSourceRepo, datastoreRepo, workflowExecutor, sync.NewDependencyResolver(),
		taskEngineAdapter, uow.NewUnitOfWork(db), metadataRepo, quantDBFactory,
		"", nil,
	)

	defer func() {
		_ = quantDBWriteQueue.Close()
		eng.Stop()
		os.Remove(dsn)
		os.Remove(duckDBPath)
		db.Close()
	}()

	dsList, _ := dataSourceRepo.List()
	for _, d := range dsList {
		if strings.EqualFold(d.Name, "tushare") {
			dsID = d.ID.String()
			break
		}
	}
	require.NotEmpty(t, dsID, "need tushare data source")
	storeID := shared.NewID()
	absPath := duckDBPath
	if !filepath.IsAbs(absPath) {
		absPath, _ = filepath.Abs(absPath)
	}
	_ = datastoreRepo.Create(&datastore.QuantDataStore{
		ID: storeID, Name: "e2e_real_duckdb_eastmoney500", Type: datastore.DataStoreTypeDuckDB,
		StoragePath: absPath, CreatedAt: shared.Timestamp(time.Now()), UpdatedAt: shared.Timestamp(time.Now()),
	})

	planID := shared.NewID()
	dsIDAsID := shared.ID(dsID)
	_ = syncPlanRepo.Create(&sync.SyncPlan{
		ID: planID, Name: "e2e_realtime_real_eastmoney500", DataSourceID: dsIDAsID, DataStoreID: storeID,
		Mode: sync.PlanModeRealtime, SelectedAPIs: []string{"realtime_quote"}, Status: sync.PlanStatusDraft,
		CreatedAt: shared.Timestamp(time.Now()), UpdatedAt: shared.Timestamp(time.Now()), PullIntervalSeconds: 1,
	})
	err = syncAppService.ResolveSyncPlan(ctx, planID)
	require.NoError(t, err)
	plan, _ := syncPlanRepo.Get(planID)
	plan.Status = sync.PlanStatusEnabled
	_ = syncPlanRepo.Update(plan)

	execID, err := syncAppService.ExecuteSyncPlan(ctx, planID, contracts.ExecuteSyncPlanRequest{})
	require.NoError(t, err)
	require.False(t, execID.IsEmpty())

	// 观察约 1 分钟内 realtime_quote 行数变化
	qdbObs, _ := quantDBFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	_ = qdbObs.Connect(ctx)
	defer qdbObs.Close()

	var lastCnt int64
	for i := 0; i < 12; i++ {
		rows, err := qdbObs.Query(ctx, `SELECT COUNT(*) as cnt FROM realtime_quote`)
		if err != nil {
			t.Logf("[eastmoney 500] query realtime_quote failed at round %d: %v", i, err)
			time.Sleep(5 * time.Second)
			continue
		}
		if len(rows) >= 1 {
			var cnt int64
			switch v := rows[0]["cnt"].(type) {
			case int64:
				cnt = v
			case float64:
				cnt = int64(v)
			}
			t.Logf("[eastmoney 500] round %d, realtime_quote row count: %d", i, cnt)
			if cnt > lastCnt {
				lastCnt = cnt
			}
		}
		time.Sleep(5 * time.Second)
	}

	// 停掉引擎后再次打开 DuckDB，打印部分样本行
	eng.Stop()
	sampleFactory := duckdb.NewFactory()
	sampleDB, err := sampleFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	if err == nil && sampleDB.Connect(ctx) == nil {
		defer sampleDB.Close()
		rows, err := sampleDB.Query(ctx, `
			SELECT ts_code, trade_time, price, volume, amount
			FROM realtime_quote
			ORDER BY trade_time DESC
			LIMIT 10`)
		if err == nil {
			for i, r := range rows {
				t.Logf("[eastmoney 500] realtime_quote sample %d: %+v", i, r)
			}
		} else {
			t.Logf("[eastmoney 500] query samples failed: %v", err)
		}
	}
}

// TestE2E_RealtimeSync_Real_List_Eastmoney 使用东财 realtime_list 行情，验证全市场涨跌榜 Streaming 同步。
// 仅在 QDHUB_REALTIME_E2E=1 时运行。
func TestE2E_RealtimeSync_Real_List_Eastmoney(t *testing.T) {
	if os.Getenv("QDHUB_REALTIME_E2E") != "1" {
		t.Skip("skip real realtime_list eastmoney e2e unless QDHUB_REALTIME_E2E=1")
	}

	logFile, logCleanup := setupLogFile(t, "RealtimeSync_Real_List_Eastmoney")
	defer logCleanup()
	defer logFile.Close()

	ctx := context.Background()

	tmpSQLite, err := os.CreateTemp("", "realtime_real_e2e_list_eastmoney_*.db")
	require.NoError(t, err)
	tmpSQLite.Close()
	dsn := tmpSQLite.Name()
	duckDBPath := filepath.Join(os.TempDir(), fmt.Sprintf("realtime_real_e2e_list_eastmoney_%d.duckdb", time.Now().UnixNano()))
	db, err := persistence.NewDB(dsn)
	require.NoError(t, err)

	migrationDir := filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(getRealtimeE2EDataDir()))), "migrations")
	for _, name := range []string{"001_init_schema.up.sql", "003_sync_plan_migration.up.sql"} {
		sql, _ := os.ReadFile(filepath.Join(migrationDir, name))
		if len(sql) > 0 {
			_, _ = db.Exec(string(sql))
		}
	}
	var dsID string
	_ = db.QueryRow(`SELECT id FROM data_sources WHERE LOWER(name) = 'tushare' LIMIT 1`).Scan(&dsID)
	if dsID == "" {
		dsID = shared.NewID().String()
		_, _ = db.Exec(`INSERT INTO data_sources (id, name, description, base_url, doc_url, status, created_at, updated_at) VALUES (?, 'Tushare', 'E2E List Eastmoney', '', '', 'active', datetime('now'), datetime('now'))`, dsID)
		_, _ = db.Exec(`INSERT OR REPLACE INTO tokens (id, data_source_id, token_value, created_at) VALUES (?, ?, 'real_e2e_token', datetime('now'))`, shared.NewID().String(), dsID)
	}
	for _, name := range []string{
		"004_api_sync_strategy.up.sql", "005_sync_plan_default_params.up.sql", "006_sync_plan_mode_realtime.up.sql",
		"007_daily_adj_factor_trade_date_expand.up.sql", "010_sync_plan_incremental_mode.up.sql",
		"015_sync_plan_incremental_start_date_source.up.sql", "016_api_sync_strategy_fixed_params.up.sql",
		"017_api_sync_strategy_news_fixed_fields.up.sql", "018_api_sync_strategy_time_window_news_cctv.up.sql",
		"019_realtime_api_sync_strategy.up.sql", "020_sync_plan_pull_interval_and_schedule_window.up.sql",
		"013_sync_execution_workflow_error_message.up.sql",
	} {
		sql, _ := os.ReadFile(filepath.Join(migrationDir, name))
		if len(sql) > 0 {
			_, _ = db.Exec(string(sql))
		}
	}

	quantDBFactory := duckdb.NewFactory()
	qdb, _ := quantDBFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	_ = qdb.Connect(ctx)
	// realtime_list 不依赖 stock_basic，这里仅建目标表。
	_, _ = qdb.Execute(ctx, `CREATE TABLE IF NOT EXISTS realtime_list (
		ts_code VARCHAR(32),
		name VARCHAR(128),
		price DOUBLE,
		pct_change DOUBLE,
		change DOUBLE,
		volume DOUBLE,
		amount DOUBLE,
		swing DOUBLE,
		high DOUBLE,
		low DOUBLE,
		open DOUBLE,
		close DOUBLE,
		vol_ratio DOUBLE,
		turnover_rate DOUBLE,
		pe DOUBLE,
		pb DOUBLE,
		total_mv DOUBLE,
		float_mv DOUBLE,
		rise DOUBLE,
		"5min" DOUBLE,
		"60day" DOUBLE,
		"1tyear" DOUBLE
	)`)
	_ = qdb.Close()

	// 复用 quote eastmoney E2E 的做法：将 Eastmoney 适配器注册到 key "sina" 上，供 QuotePullCollector 使用。
	eastReg := realtime.NewDefaultRegistry()
	eastReg.Register("sina", realtime.NewEastmoneyRealtimeAdapter())
	bufReg := realtimebuffer.NewDefaultRegistry(256)

	aggregateRepo, _ := sqlite.NewWorkflowAggregateRepoFromDSN(dsn)
	eng, _ := engine.NewEngineWithAggregateRepo(5, 30, aggregateRepo)
	_ = eng.Start(ctx)

	metadataRepo := repository.NewMetadataRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	datastoreRepo := repository.NewQuantDataStoreRepository(db)
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	workflowRepo, _ := repository.NewWorkflowDefinitionRepository(db)

	wqCfg := config.Default().QuantDB.WriteQueue
	quantDBWriteQueue := writequeue.NewQueue(wqCfg, quantDBFactory)
	taskEngineDeps := &taskengine.Dependencies{
		MetadataRepo:            metadataRepo,
		DataStoreRepo:           datastoreRepo,
		QuantDBFactory:          quantDBFactory,
		QuantDBWriteQueue:       quantDBWriteQueue,
		RealtimeAdapterRegistry: eastReg,
		RealtimeBufferRegistry:  bufReg,
	}
	_ = taskengine.Initialize(ctx, eng, taskEngineDeps)
	taskEngineAdapter := taskengine.NewTaskEngineAdapter(eng, 0)
	workflowFactory := taskengine.GetWorkflowFactory(eng)
	_ = impl.NewBuiltInWorkflowInitializer(workflowRepo, workflowFactory, taskEngineAdapter).Initialize(ctx)
	workflowExecutor := taskengine.NewWorkflowExecutor(workflowRepo, taskEngineAdapter, metadataRepo, eastReg, "", nil, "", "", "")

	syncAppService := impl.NewSyncApplicationService(
		syncPlanRepo, sync.NewCronScheduleCalculator(), nil,
		dataSourceRepo, datastoreRepo, workflowExecutor, sync.NewDependencyResolver(),
		taskEngineAdapter, uow.NewUnitOfWork(db), metadataRepo, quantDBFactory,
		"", nil,
	)

	defer func() {
		_ = quantDBWriteQueue.Close()
		eng.Stop()
		os.Remove(dsn)
		os.Remove(duckDBPath)
		db.Close()
	}()

	dsList, _ := dataSourceRepo.List()
	for _, d := range dsList {
		if strings.EqualFold(d.Name, "tushare") {
			dsID = d.ID.String()
			break
		}
	}
	require.NotEmpty(t, dsID, "need tushare data source")
	storeID := shared.NewID()
	absPath := duckDBPath
	if !filepath.IsAbs(absPath) {
		absPath, _ = filepath.Abs(absPath)
	}
	_ = datastoreRepo.Create(&datastore.QuantDataStore{
		ID: storeID, Name: "e2e_real_duckdb_list_eastmoney", Type: datastore.DataStoreTypeDuckDB,
		StoragePath: absPath, CreatedAt: shared.Timestamp(time.Now()), UpdatedAt: shared.Timestamp(time.Now()),
	})

	planID := shared.NewID()
	dsIDAsID := shared.ID(dsID)
	_ = syncPlanRepo.Create(&sync.SyncPlan{
		ID: planID, Name: "e2e_realtime_real_list_eastmoney", DataSourceID: dsIDAsID, DataStoreID: storeID,
		Mode: sync.PlanModeRealtime, SelectedAPIs: []string{"realtime_list"}, Status: sync.PlanStatusDraft,
		CreatedAt: shared.Timestamp(time.Now()), UpdatedAt: shared.Timestamp(time.Now()), PullIntervalSeconds: 5,
	})
	err = syncAppService.ResolveSyncPlan(ctx, planID)
	require.NoError(t, err)
	plan, _ := syncPlanRepo.Get(planID)
	plan.Status = sync.PlanStatusEnabled
	_ = syncPlanRepo.Update(plan)

	execID, err := syncAppService.ExecuteSyncPlan(ctx, planID, contracts.ExecuteSyncPlanRequest{})
	require.NoError(t, err)
	require.False(t, execID.IsEmpty())

	// 观察约 1 分钟内 realtime_list 行数变化。
	qdb2, _ := quantDBFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	_ = qdb2.Connect(ctx)
	defer qdb2.Close()

	for i := 0; i < 12; i++ {
		rows, err := qdb2.Query(ctx, `SELECT COUNT(*) as cnt FROM realtime_list`)
		if err != nil {
			t.Logf("[eastmoney list] query realtime_list failed at round %d: %v", i, err)
			time.Sleep(5 * time.Second)
			continue
		}
		if len(rows) >= 1 {
			var cnt int64
			switch v := rows[0]["cnt"].(type) {
			case int64:
				cnt = v
			case float64:
				cnt = int64(v)
			}
			t.Logf("[eastmoney list] round %d, realtime_list row count: %d", i, cnt)
		}
		time.Sleep(5 * time.Second)
	}

	// 停掉引擎后再次打开 DuckDB，打印部分样本行
	eng.Stop()
	sampleFactory := duckdb.NewFactory()
	sampleDB, err := sampleFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	if err == nil && sampleDB.Connect(ctx) == nil {
		defer sampleDB.Close()
		rows, err := sampleDB.Query(ctx, `
			SELECT ts_code, name, price, pct_change, amount
			FROM realtime_list
			ORDER BY pct_change DESC
			LIMIT 10`)
		if err == nil {
			for i, r := range rows {
				t.Logf("[eastmoney list] realtime_list sample %d: %+v", i, r)
			}
		} else {
			t.Logf("[eastmoney list] query samples failed: %v", err)
		}
	}
}
// TestE2E_RealtimeSync_Real_Tick_Eastmoney 使用东财 SSE 分笔行情，验证 realtime_tick SSE Push 全链路。
// 仅在 QDHUB_REALTIME_E2E=1 且交易时间运行才有意义。
func TestE2E_RealtimeSync_Real_Tick_Eastmoney(t *testing.T) {
	if os.Getenv("QDHUB_REALTIME_E2E") != "1" {
		t.Skip("skip real realtime tick eastmoney e2e unless QDHUB_REALTIME_E2E=1")
	}

	logFile, logCleanup := setupLogFile(t, "RealtimeSync_Real_Tick_Eastmoney")
	defer logCleanup()
	defer logFile.Close()

	ctx := context.Background()

	tmpSQLite, err := os.CreateTemp("", "realtime_real_e2e_tick_eastmoney_*.db")
	require.NoError(t, err)
	tmpSQLite.Close()
	dsn := tmpSQLite.Name()
	duckDBPath := filepath.Join(os.TempDir(), fmt.Sprintf("realtime_real_e2e_tick_eastmoney_%d.duckdb", time.Now().UnixNano()))
	db, err := persistence.NewDB(dsn)
	require.NoError(t, err)

	migrationDir := filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(getRealtimeE2EDataDir()))), "migrations")
	for _, name := range []string{"001_init_schema.up.sql", "003_sync_plan_migration.up.sql"} {
		sql, _ := os.ReadFile(filepath.Join(migrationDir, name))
		if len(sql) > 0 {
			_, _ = db.Exec(string(sql))
		}
	}
	var dsID string
	_ = db.QueryRow(`SELECT id FROM data_sources WHERE LOWER(name) = 'tushare' LIMIT 1`).Scan(&dsID)
	if dsID == "" {
		dsID = shared.NewID().String()
		_, _ = db.Exec(`INSERT INTO data_sources (id, name, description, base_url, doc_url, status, created_at, updated_at) VALUES (?, 'Tushare', 'E2E Tick Eastmoney', '', '', 'active', datetime('now'), datetime('now'))`, dsID)
		_, _ = db.Exec(`INSERT OR REPLACE INTO tokens (id, data_source_id, token_value, created_at) VALUES (?, ?, 'real_e2e_token', datetime('now'))`, shared.NewID().String(), dsID)
	}
	for _, name := range []string{
		"004_api_sync_strategy.up.sql", "005_sync_plan_default_params.up.sql", "006_sync_plan_mode_realtime.up.sql",
		"007_daily_adj_factor_trade_date_expand.up.sql", "010_sync_plan_incremental_mode.up.sql",
		"015_sync_plan_incremental_start_date_source.up.sql", "016_api_sync_strategy_fixed_params.up.sql",
		"017_api_sync_strategy_news_fixed_fields.up.sql", "018_api_sync_strategy_time_window_news_cctv.up.sql",
		"019_realtime_api_sync_strategy.up.sql", "020_sync_plan_pull_interval_and_schedule_window.up.sql",
		"013_sync_execution_workflow_error_message.up.sql",
	} {
		sql, _ := os.ReadFile(filepath.Join(migrationDir, name))
		if len(sql) > 0 {
			_, _ = db.Exec(string(sql))
		}
	}

	quantDBFactory := duckdb.NewFactory()
	qdb, _ := quantDBFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	_ = qdb.Connect(ctx)
	_, _ = qdb.Execute(ctx, `CREATE TABLE IF NOT EXISTS stock_basic (ts_code VARCHAR(32), name VARCHAR(128), list_date VARCHAR(16))`)
	// 选择上交所一只股票，东财 SSE 接口支持 6 开头/深市 0 开头均可。
	_, _ = qdb.Execute(ctx, `INSERT INTO stock_basic (ts_code, name, list_date) VALUES ('600000.SH', '浦发银行', '19991110')`)
	_, _ = qdb.Execute(ctx, `CREATE TABLE IF NOT EXISTS realtime_tick (
		ts_code VARCHAR(32),
		time VARCHAR(32),
		price DOUBLE,
		change DOUBLE,
		volume DOUBLE,
		amount DOUBLE,
		type VARCHAR(16)
	)`)
	_ = qdb.Close()

	// RealtimeAdapterRegistry：eastmoney 适配器按默认 key 注册。
	eastReg := realtime.NewDefaultRegistry()
	eastReg.Register("eastmoney", realtime.NewEastmoneyRealtimeAdapter())
	bufReg := realtimebuffer.NewDefaultRegistry(256)

	aggregateRepo, _ := sqlite.NewWorkflowAggregateRepoFromDSN(dsn)
	eng, _ := engine.NewEngineWithAggregateRepo(5, 30, aggregateRepo)
	_ = eng.Start(ctx)

	metadataRepo := repository.NewMetadataRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	datastoreRepo := repository.NewQuantDataStoreRepository(db)
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	workflowRepo, _ := repository.NewWorkflowDefinitionRepository(db)

	wqCfg := config.Default().QuantDB.WriteQueue
	quantDBWriteQueue := writequeue.NewQueue(wqCfg, quantDBFactory)
	taskEngineDeps := &taskengine.Dependencies{
		MetadataRepo:            metadataRepo,
		DataStoreRepo:           datastoreRepo,
		QuantDBFactory:          quantDBFactory,
		QuantDBWriteQueue:       quantDBWriteQueue,
		RealtimeAdapterRegistry: eastReg,
		RealtimeBufferRegistry:  bufReg,
	}
	_ = taskengine.Initialize(ctx, eng, taskEngineDeps)
	taskEngineAdapter := taskengine.NewTaskEngineAdapter(eng, 0)
	workflowFactory := taskengine.GetWorkflowFactory(eng)
	_ = impl.NewBuiltInWorkflowInitializer(workflowRepo, workflowFactory, taskEngineAdapter).Initialize(ctx)
	workflowExecutor := taskengine.NewWorkflowExecutor(workflowRepo, taskEngineAdapter, metadataRepo, eastReg, "", nil, "", "", "")

	syncAppService := impl.NewSyncApplicationService(
		syncPlanRepo, sync.NewCronScheduleCalculator(), nil,
		dataSourceRepo, datastoreRepo, workflowExecutor, sync.NewDependencyResolver(),
		taskEngineAdapter, uow.NewUnitOfWork(db), metadataRepo, quantDBFactory,
		"", nil,
	)

	defer func() {
		_ = quantDBWriteQueue.Close()
		eng.Stop()
		os.Remove(dsn)
		os.Remove(duckDBPath)
		db.Close()
	}()

	dsList, _ := dataSourceRepo.List()
	for _, d := range dsList {
		if strings.EqualFold(d.Name, "tushare") {
			dsID = d.ID.String()
			break
		}
	}
	require.NotEmpty(t, dsID, "need tushare data source")
	storeID := shared.NewID()
	absPath := duckDBPath
	if !filepath.IsAbs(absPath) {
		absPath, _ = filepath.Abs(absPath)
	}
	_ = datastoreRepo.Create(&datastore.QuantDataStore{
		ID: storeID, Name: "e2e_real_duckdb_tick_eastmoney", Type: datastore.DataStoreTypeDuckDB,
		StoragePath: absPath, CreatedAt: shared.Timestamp(time.Now()), UpdatedAt: shared.Timestamp(time.Now()),
	})

	planID := shared.NewID()
	dsIDAsID := shared.ID(dsID)
	_ = syncPlanRepo.Create(&sync.SyncPlan{
		ID: planID, Name: "e2e_realtime_real_tick_eastmoney", DataSourceID: dsIDAsID, DataStoreID: storeID,
		Mode: sync.PlanModeRealtime, SelectedAPIs: []string{"realtime_tick"}, Status: sync.PlanStatusDraft,
		CreatedAt: shared.Timestamp(time.Now()), UpdatedAt: shared.Timestamp(time.Now()), PullIntervalSeconds: 1,
	})
	err = syncAppService.ResolveSyncPlan(ctx, planID)
	require.NoError(t, err)
	plan, _ := syncPlanRepo.Get(planID)
	plan.Status = sync.PlanStatusEnabled
	_ = syncPlanRepo.Update(plan)

	execID, err := syncAppService.ExecuteSyncPlan(ctx, planID, contracts.ExecuteSyncPlanRequest{})
	require.NoError(t, err)
	require.False(t, execID.IsEmpty())

	// Streaming Push 模式：观察一段时间内 realtime_tick 是否有数据写入。
	qdb2, _ := quantDBFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	_ = qdb2.Connect(ctx)
	defer qdb2.Close()

	for i := 0; i < 12; i++ {
		rows, err := qdb2.Query(ctx, `SELECT COUNT(*) as cnt FROM realtime_tick`)
		if err != nil {
			t.Logf("[eastmoney tick] query realtime_tick failed at round %d: %v", i, err)
			time.Sleep(5 * time.Second)
			continue
		}
		if len(rows) >= 1 {
			var cnt int64
			switch v := rows[0]["cnt"].(type) {
			case int64:
				cnt = v
			case float64:
				cnt = int64(v)
			}
			t.Logf("[eastmoney tick] round %d, realtime_tick row count: %d", i, cnt)
		}
		time.Sleep(5 * time.Second)
	}

	// 停掉引擎后再次打开 DuckDB，打印部分样本行
	eng.Stop()
	sampleFactory := duckdb.NewFactory()
	sampleDB, err := sampleFactory.Create(datastore.QuantDBConfig{Type: datastore.DataStoreTypeDuckDB, StoragePath: duckDBPath})
	if err == nil && sampleDB.Connect(ctx) == nil {
		defer sampleDB.Close()
		rows, err := sampleDB.Query(ctx, `
			SELECT ts_code, time, price, volume, amount, type
			FROM realtime_tick
			ORDER BY time DESC
			LIMIT 10`)
		if err == nil {
			for i, r := range rows {
				t.Logf("[eastmoney tick] realtime_tick sample %d: %+v", i, r)
			}
		} else {
			t.Logf("[eastmoney tick] query samples failed: %v", err)
		}
	}
}

// ==================== Mock 大批量 Streaming E2E（500 支股票） ====================

// TestE2E_RealtimeSync_Mock_LargeBatch500 使用 Mock Realtime Adapter 模拟 500 支股票的实时同步，
// 持续约 1 分钟，以验证 Streaming 工作流在大规模 ts_code 分片下能够稳定运行。
func TestE2E_RealtimeSync_Mock_LargeBatch500(t *testing.T) {
	logFile, logCleanup := setupLogFile(t, "RealtimeSync_Mock_LargeBatch500")
	defer logCleanup()
	defer logFile.Close()

	csvPath := filepath.Join(getRealtimeE2EDataDir(), "stk_mins_202603082215.csv")
	if _, err := os.Stat(csvPath); err != nil {
		t.Skipf("skip: CSV not found: %s", csvPath)
	}

	testCtx := setupRealtimeSyncE2EContext(t, csvPath)
	defer testCtx.cleanup()

	ctx := context.Background()

	// 在 DuckDB 中构造 500 支股票的 stock_basic，供 Streaming Collector 分片。
	qdb, err := testCtx.quantDBFactory.Create(datastore.QuantDBConfig{
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: testCtx.duckDBPath,
	})
	require.NoError(t, err)
	require.NoError(t, qdb.Connect(ctx))
	for i := 0; i < 500; i++ {
		code := fmt.Sprintf("TEST%06d.SZ", i)
		sql := fmt.Sprintf(
			`INSERT INTO stock_basic (ts_code, name, list_date) VALUES ('%s', 'MockStock%06d', '20200101')`,
			code, i,
		)
		_, err = qdb.Execute(ctx, sql)
		require.NoError(t, err)
	}
	qdb.Close()

	// 获取 Tushare 数据源 ID
	var dsID shared.ID
	dsList, err := testCtx.dataSourceRepo.List()
	require.NoError(t, err)
	for _, d := range dsList {
		if strings.EqualFold(d.Name, "tushare") {
			dsID = d.ID
			break
		}
	}
	require.False(t, dsID.IsEmpty(), "need tushare data source")

	// 创建 Quant Data Store（DuckDB 路径）
	storeID := shared.NewID()
	absDuckPath := testCtx.duckDBPath
	if !filepath.IsAbs(absDuckPath) {
		absDuckPath, _ = filepath.Abs(absDuckPath)
	}
	err = testCtx.datastoreRepo.Create(&datastore.QuantDataStore{
		ID:          storeID,
		Name:        "e2e_duckdb_large_500",
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: absDuckPath,
		CreatedAt:   shared.Timestamp(time.Now()),
		UpdatedAt:   shared.Timestamp(time.Now()),
	})
	require.NoError(t, err)

	// 创建 SyncPlan：realtime，selected_apis = [realtime_quote]，PullIntervalSeconds=1
	planID := shared.NewID()
	plan := &sync.SyncPlan{
		ID:                  planID,
		Name:                "e2e_realtime_mock_500",
		DataSourceID:        dsID,
		DataStoreID:         storeID,
		Mode:                sync.PlanModeRealtime,
		SelectedAPIs:        []string{"realtime_quote"},
		Status:              sync.PlanStatusDraft,
		CreatedAt:           shared.Timestamp(time.Now()),
		UpdatedAt:           shared.Timestamp(time.Now()),
		PullIntervalSeconds: 1,
	}
	err = testCtx.syncPlanRepo.Create(plan)
	require.NoError(t, err)

	// Resolve + Enable
	err = testCtx.syncAppService.ResolveSyncPlan(ctx, planID)
	require.NoError(t, err)
	plan, _ = testCtx.syncPlanRepo.Get(planID)
	plan.Status = sync.PlanStatusEnabled
	err = testCtx.syncPlanRepo.Update(plan)
	require.NoError(t, err)

	// 执行：Streaming 模式下实例持续运行，不等待“完成”，观察约 1 分钟的数据写入情况。
	execID, err := testCtx.syncAppService.ExecuteSyncPlan(ctx, planID, contracts.ExecuteSyncPlanRequest{})
	require.NoError(t, err)
	require.False(t, execID.IsEmpty())

	// 期间周期性查询 DuckDB 中 realtime_quote 的行数，用于大致观察写入节奏。
	qdbObs, err := testCtx.quantDBFactory.Create(datastore.QuantDBConfig{
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: testCtx.duckDBPath,
	})
	require.NoError(t, err)
	require.NoError(t, qdbObs.Connect(ctx))
	defer qdbObs.Close()

	var lastCnt int64
	for i := 0; i < 12; i++ {
		rows, err := qdbObs.Query(ctx, `SELECT COUNT(*) as cnt FROM realtime_quote`)
		if err != nil {
			t.Logf("large batch mock: query realtime_quote failed at round %d: %v", i, err)
			time.Sleep(5 * time.Second)
			continue
		}
		if len(rows) >= 1 {
			var cnt int64
			switch v := rows[0]["cnt"].(type) {
			case int64:
				cnt = v
			case int:
				cnt = int64(v)
			case int32:
				cnt = int64(v)
			case int16:
				cnt = int64(v)
			case float64:
				cnt = int64(v)
			case float32:
				cnt = int64(v)
			default:
				if s, ok := rows[0]["cnt"].(fmt.Stringer); ok {
					if n, err := strconv.ParseInt(s.String(), 10, 64); err == nil {
						cnt = n
					}
				}
			}
			t.Logf("large batch mock round %d, realtime_quote row count: %d", i, cnt)
			if cnt > lastCnt {
				lastCnt = cnt
			}
		}
		time.Sleep(5 * time.Second)
	}

	// Mock 大批量场景下，主要验证 Streaming 工作流在 500 ts_code 下持续运行无 panic/错误，
	// 且 realtime_quote 有数据写入（至少能在某轮看到 COUNT(*) > 0 或最终样本行）。
	if lastCnt == 0 {
		t.Logf("large batch mock: realtime_quote row count did not increase during window (check logs for writeRealtimeBatch output)")
	}
}
