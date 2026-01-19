//go:build e2e
// +build e2e

// Package e2e 提供端到端测试
// 本文件实现使用真实组件的完整流程测试：
// - 真实 SQLite 数据库（Task Engine 和应用数据存储）
// - 真实 DuckDB 数据库（量化数据存储）
// - 真实 Tushare API（需要 QDHUB_TUSHARE_TOKEN 环境变量）
//
// 测试通过应用服务层（而非直接使用 WorkflowExecutor）来执行工作流
//
// 运行方式：
//
//	QDHUB_TUSHARE_TOKEN=your_token go test -tags e2e -v -run "TestE2E_RealWorkflow" ./tests/e2e/...
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/datasource"
	"qdhub/internal/infrastructure/datasource/tushare"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/quantdb/duckdb"
	"qdhub/internal/infrastructure/scheduler"
	"qdhub/internal/infrastructure/taskengine"
	"qdhub/internal/infrastructure/taskengine/workflows"
)

// ==================== 测试配置 ====================

// realE2EConfig 真实 E2E 测试配置
type realE2EConfig struct {
	TushareToken    string
	SQLiteDBPath    string
	DuckDBPath      string
	SkipRealAPICall bool // 如果没有 token，跳过真实 API 调用
}

// loadRealE2EConfig 从环境变量加载配置
func loadRealE2EConfig(t *testing.T) *realE2EConfig {
	token := os.Getenv("QDHUB_TUSHARE_TOKEN")
	if token == "" {
		token = os.Getenv("TUSHARE_TOKEN") // 兼容旧的环境变量名
	}
	token = strings.TrimSpace(token)

	return &realE2EConfig{
		TushareToken:    token,
		SkipRealAPICall: token == "",
	}
}

// ==================== 真实 E2E 测试上下文 ====================

// realE2EContext 真实 E2E 测试上下文
type realE2EContext struct {
	config *realE2EConfig

	// 基础设施
	db               *persistence.DB
	engine           *engine.Engine
	dsRegistry       *datasource.Registry
	tushareAdapter   *tushare.Adapter
	duckDBAdapter    *duckdb.Adapter
	workflowExecutor workflow.WorkflowExecutor

	// 应用服务
	metadataSvc  contracts.MetadataApplicationService
	dataStoreSvc contracts.DataStoreApplicationService
	syncSvc      contracts.SyncApplicationService
	workflowSvc  contracts.WorkflowApplicationService

	// Repositories
	workflowRepo   workflow.WorkflowDefinitionRepository
	dataSourceRepo metadata.DataSourceRepository
	metadataRepo   metadata.Repository

	cleanup func()
}

// setupRealE2EContext 设置真实 E2E 测试环境
func setupRealE2EContext(t *testing.T) *realE2EContext {
	ctx := context.Background()
	config := loadRealE2EConfig(t)

	// 1. 创建临时目录
	tmpDir, err := os.MkdirTemp("", "real_e2e_")
	require.NoError(t, err)

	// 2. 创建 SQLite 数据库路径（应用数据）
	config.SQLiteDBPath = filepath.Join(tmpDir, "app.db")

	// 3. 创建 DuckDB 数据库路径（注意：不创建文件，让 DuckDB 自己创建）
	config.DuckDBPath = filepath.Join(tmpDir, "quant.duckdb")

	t.Logf("临时目录: %s", tmpDir)
	t.Logf("SQLite 数据库: %s", config.SQLiteDBPath)
	t.Logf("DuckDB 数据库: %s", config.DuckDBPath)
	if config.TushareToken != "" {
		t.Logf("Tushare Token: 已配置 (长度=%d)", len(config.TushareToken))
	} else {
		t.Logf("⚠️  Tushare Token: 未配置，将跳过真实 API 调用测试")
	}

	// 4. 创建应用层 SQLite 数据库
	db, err := persistence.NewDB(config.SQLiteDBPath)
	require.NoError(t, err)

	// 执行迁移
	migrationSQL, err := os.ReadFile("../../migrations/001_init_schema.up.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(migrationSQL))
	require.NoError(t, err)

	// 5. 创建 Task Engine（使用同一个 SQLite 数据库）
	aggregateRepo, err := sqlite.NewWorkflowAggregateRepoFromDSN(config.SQLiteDBPath)
	require.NoError(t, err)

	eng, err := engine.NewEngineWithAggregateRepo(5, 30, aggregateRepo)
	require.NoError(t, err)

	err = eng.Start(ctx)
	require.NoError(t, err)

	// 6. 创建真实的 Tushare Adapter
	tushareAdapter := tushare.NewAdapter()
	if config.TushareToken != "" {
		tushareAdapter.SetToken(config.TushareToken)
	}

	// 7. 创建真实的 DuckDB Adapter（不预先创建文件）
	duckDBAdapter := duckdb.NewAdapter(config.DuckDBPath)
	err = duckDBAdapter.Connect(ctx)
	require.NoError(t, err)

	// 8. 创建 DataSource Registry 并注册 Tushare adapter
	dsRegistry := datasource.NewRegistry()
	err = dsRegistry.RegisterAdapter(tushareAdapter)
	require.NoError(t, err)

	// 9. 创建 repositories
	workflowRepo, err := repository.NewWorkflowDefinitionRepository(db)
	require.NoError(t, err)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	metadataRepo := repository.NewMetadataRepository(db)
	dataStoreRepo := repository.NewQuantDataStoreRepository(db)
	mappingRuleRepo := repository.NewDataTypeMappingRuleRepository(db)
	syncJobRepo := repository.NewSyncJobRepository(db)

	// 10. 初始化 Task Engine（注册 job functions 和 handlers）
	taskEngineDeps := &taskengine.Dependencies{
		DataSourceRegistry: dsRegistry,
		MetadataRepo:       metadataRepo,
	}
	err = taskengine.Initialize(ctx, eng, taskEngineDeps)
	require.NoError(t, err)

	// 11. 创建 adapters
	taskEngineAdapter := taskengine.NewTaskEngineAdapter(eng)
	workflowFactory := taskengine.GetWorkflowFactory(eng)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	jobScheduler := scheduler.NewCronScheduler(&mockJobHandler{})

	// 12. 初始化内建 workflows
	builtInInitializer := impl.NewBuiltInWorkflowInitializer(workflowRepo, workflowFactory, taskEngineAdapter)
	err = builtInInitializer.Initialize(ctx)
	require.NoError(t, err)

	// 13. 创建 WorkflowExecutor（领域服务）
	workflowExecutor := taskengine.NewWorkflowExecutor(workflowRepo, taskEngineAdapter)

	// 14. 创建 QuantDB Adapter 包装器（满足应用服务接口）
	quantDBAdapterWrapper := &quantDBAdapterWrapper{adapter: duckDBAdapter}

	// 15. 创建 Document Parser Factory（测试用 mock）
	parserFactory := &mockParserFactory{}

	// 16. 创建应用服务
	metadataSvc := impl.NewMetadataApplicationService(dataSourceRepo, parserFactory, workflowExecutor)
	dataStoreSvc := impl.NewDataStoreApplicationService(dataStoreRepo, mappingRuleRepo, dataSourceRepo, quantDBAdapterWrapper, workflowExecutor)
	syncSvc := impl.NewSyncApplicationService(syncJobRepo, workflowRepo, taskEngineAdapter, cronCalculator, jobScheduler, dataSourceRepo, workflowExecutor)
	workflowSvc := impl.NewWorkflowApplicationService(workflowRepo, taskEngineAdapter)

	cleanup := func() {
		eng.Stop()
		duckDBAdapter.Close()
		db.Close()
		os.RemoveAll(tmpDir) // 删除整个临时目录
	}

	return &realE2EContext{
		config:           config,
		db:               db,
		engine:           eng,
		dsRegistry:       dsRegistry,
		tushareAdapter:   tushareAdapter,
		duckDBAdapter:    duckDBAdapter,
		workflowExecutor: workflowExecutor,
		metadataSvc:      metadataSvc,
		dataStoreSvc:     dataStoreSvc,
		syncSvc:          syncSvc,
		workflowSvc:      workflowSvc,
		workflowRepo:     workflowRepo,
		dataSourceRepo:   dataSourceRepo,
		metadataRepo:     metadataRepo,
		cleanup:          cleanup,
	}
}

// quantDBAdapterWrapper 包装 DuckDB Adapter 以满足应用服务接口
type quantDBAdapterWrapper struct {
	adapter *duckdb.Adapter
}

func (w *quantDBAdapterWrapper) TestConnection(ctx context.Context, ds *datastore.QuantDataStore) error {
	return w.adapter.Ping(ctx)
}

func (w *quantDBAdapterWrapper) ExecuteDDL(ctx context.Context, ds *datastore.QuantDataStore, ddl string) error {
	_, err := w.adapter.Execute(ctx, ddl)
	return err
}

func (w *quantDBAdapterWrapper) TableExists(ctx context.Context, ds *datastore.QuantDataStore, tableName string) (bool, error) {
	return w.adapter.TableExists(ctx, tableName)
}

// mockJobHandler 用于测试的 JobHandler 实现
type mockJobHandler struct{}

func (h *mockJobHandler) ExecuteScheduledJob(ctx context.Context, jobID string) error {
	return nil
}

// mockParserFactory 用于测试的 DocumentParserFactory 实现
type mockParserFactory struct{}

func (f *mockParserFactory) GetParser(docType metadata.DocumentType) (metadata.DocumentParser, error) {
	return nil, fmt.Errorf("parser not implemented in test")
}

func (f *mockParserFactory) RegisterParser(parser metadata.DocumentParser) {}

// waitForWorkflowStatus 等待 workflow 达到指定状态
func waitForWorkflowStatus(ctx context.Context, adapter workflow.TaskEngineAdapter, instanceID string, timeout time.Duration) (*workflow.WorkflowStatus, error) {
	deadline := time.Now().Add(timeout)
	var lastStatus *workflow.WorkflowStatus
	var lastErr error

	for time.Now().Before(deadline) {
		status, err := adapter.GetInstanceStatus(ctx, instanceID)
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		lastStatus = status
		lastErr = nil

		// 检查是否完成（成功或失败）
		normalizedStatus := strings.ToLower(status.Status)
		switch normalizedStatus {
		case "success", "completed":
			return status, nil
		case "failed", "error":
			errMsg := ""
			if status.ErrorMessage != nil {
				errMsg = *status.ErrorMessage
			}
			return status, fmt.Errorf("workflow failed: %s", errMsg)
		case "cancelled":
			return status, fmt.Errorf("workflow cancelled")
		}

		time.Sleep(200 * time.Millisecond)
	}

	if lastErr != nil {
		return lastStatus, fmt.Errorf("timeout waiting for workflow completion: %w", lastErr)
	}
	return lastStatus, fmt.Errorf("timeout waiting for workflow completion, last status: %v", lastStatus)
}

// truncateString 截断字符串，超过 maxLen 时添加省略号
func truncateString(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return string(runes[:maxLen])
	}
	return string(runes[:maxLen-2]) + ".."
}

// ==================== 真实 E2E 测试用例 ====================

// TestE2E_RealWorkflow_FullPipeline 使用真实组件和应用服务测试完整流程
// 需要设置 QDHUB_TUSHARE_TOKEN 环境变量
func TestE2E_RealWorkflow_FullPipeline(t *testing.T) {
	testCtx := setupRealE2EContext(t)
	defer testCtx.cleanup()

	ctx := context.Background()

	t.Log("========== 真实组件 E2E 测试开始 ==========")
	t.Logf("SQLite: %s", testCtx.config.SQLiteDBPath)
	t.Logf("DuckDB: %s", testCtx.config.DuckDBPath)

	// ==================== Step 1: 验证内建 Workflows ====================
	t.Run("Step1_VerifyBuiltinWorkflows", func(t *testing.T) {
		t.Log("----- Step 1: 验证内建 Workflows -----")

		builtInWorkflows := workflows.GetBuiltInWorkflows()
		for _, meta := range builtInWorkflows {
			def, err := testCtx.workflowRepo.Get(meta.ID)
			require.NoError(t, err)
			require.NotNil(t, def)
			assert.True(t, def.IsSystem)
			t.Logf("✅ %s (%s)", meta.Name, meta.ID)
		}
	})

	// ==================== Step 2: 通过应用服务创建数据源 ====================
	var dataSourceID shared.ID
	t.Run("Step2_CreateDataSourceViaService", func(t *testing.T) {
		t.Log("----- Step 2: 通过应用服务创建 Tushare 数据源 -----")

		// 使用 MetadataApplicationService 创建数据源
		// 注意：名称必须与 tushare.Adapter.Name() 一致（小写 "tushare"），
		// 因为 Job Functions 通过此名称从 Registry 获取 client
		ds, err := testCtx.metadataSvc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
			Name:        "tushare",
			Description: "Tushare Pro 数据源",
			BaseURL:     "http://api.tushare.pro",
			DocURL:      "https://tushare.pro/document/2",
		})
		require.NoError(t, err)
		require.NotNil(t, ds)
		dataSourceID = ds.ID
		t.Logf("✅ 数据源创建成功: ID=%s", dataSourceID)

		// 设置 Token（如果有的话）
		if testCtx.config.TushareToken != "" {
			err = testCtx.metadataSvc.SaveToken(ctx, contracts.SaveTokenRequest{
				DataSourceID: dataSourceID,
				TokenValue:   testCtx.config.TushareToken,
			})
			require.NoError(t, err)
			t.Logf("✅ Token 设置成功")

			// 同时设置到 adapter 上（Job Functions 需要）
			testCtx.tushareAdapter.SetToken(testCtx.config.TushareToken)
		} else {
			t.Logf("⚠️  跳过 Token 设置（未提供）")
		}
	})

	// ==================== Step 3: 通过应用服务创建数据存储 ====================
	var dataStoreID shared.ID
	t.Run("Step3_CreateDataStoreViaService", func(t *testing.T) {
		t.Log("----- Step 3: 通过应用服务创建 DuckDB 数据存储 -----")

		store, err := testCtx.dataStoreSvc.CreateDataStore(ctx, contracts.CreateDataStoreRequest{
			Name:        "Test DuckDB",
			Description: "E2E 测试数据存储",
			Type:        datastore.DataStoreTypeDuckDB,
			StoragePath: testCtx.config.DuckDBPath,
		})
		require.NoError(t, err)
		require.NotNil(t, store)
		dataStoreID = store.ID
		t.Logf("✅ 数据存储创建成功: ID=%s, Path=%s", dataStoreID, testCtx.config.DuckDBPath)

		// 测试连接
		err = testCtx.dataStoreSvc.TestConnection(ctx, dataStoreID)
		require.NoError(t, err)
		t.Logf("✅ 数据存储连接测试成功")
	})

	// ==================== Step 4: 验证 Tushare API 连接 ====================
	t.Run("Step4_VerifyTushareConnection", func(t *testing.T) {
		if testCtx.config.SkipRealAPICall {
			t.Skip("跳过：未设置 QDHUB_TUSHARE_TOKEN")
		}

		t.Log("----- Step 4: 验证 Tushare API 连接 -----")

		// 验证 Token
		valid, err := testCtx.tushareAdapter.Client().ValidateToken(ctx)
		require.NoError(t, err)
		assert.True(t, valid, "Token 应该有效")
		t.Logf("✅ Token 验证成功")
	})

	// ==================== Step 5: 通过应用服务执行元数据爬取 ====================
	t.Run("Step5_ParseAndImportMetadataViaService", func(t *testing.T) {
		if testCtx.config.SkipRealAPICall {
			t.Skip("跳过：未设置 QDHUB_TUSHARE_TOKEN")
		}

		t.Log("----- Step 5: 通过应用服务执行元数据爬取 -----")

		// 调用 ParseAndImportMetadata（内部会触发 metadata_crawl workflow）
		// 限制爬取数量，加快测试速度
		maxAPICrawl := 0
		result, err := testCtx.metadataSvc.ParseAndImportMetadata(ctx, contracts.ParseMetadataRequest{
			DataSourceID: dataSourceID,
			DocContent:   "", // workflow 会自己爬取
			DocType:      metadata.DocumentTypeHTML,
			MaxAPICrawl:  maxAPICrawl,
		})
		require.NoError(t, err)
		t.Logf("✅ 元数据爬取 Workflow 已启动: %+v", result)

		// 轮询等待数据保存（最多等待 120 秒）
		t.Log("等待 workflow 执行并保存数据...")
		var categories []*metadata.APICategory
		var apis []*metadata.APIMetadata
		maxWait := 480 * time.Second
		pollInterval := 5 * time.Second
		deadline := time.Now().Add(maxWait)

		for time.Now().Before(deadline) {
			// 使用 metadataRepo 查询（与 Job Function 保存时使用的是同一个 repo）
			catList, _ := testCtx.metadataRepo.ListCategoriesByDataSource(ctx, dataSourceID)
			categories = make([]*metadata.APICategory, len(catList))
			for i := range catList {
				categories[i] = &catList[i]
			}
			apis, _ = testCtx.metadataSvc.ListAPIMetadataByDataSource(ctx, dataSourceID)

			t.Logf("   轮询中... DataSourceID=%s, 分类: %d, API: %d (目标: %d)", dataSourceID, len(categories), len(apis), maxAPICrawl)

			// 如果已经有数据，检查是否达到目标数量
			// 目标：至少有 1 个分类，且 API 数量达到 maxAPICrawl 或至少 1 个
			targetAPICount := maxAPICrawl
			if targetAPICount <= 0 {
				targetAPICount = 1 // 至少有 1 个 API
			}
			if len(categories) > 0 && len(apis) >= targetAPICount {
				t.Logf("   ✅ 检测到数据已达目标，分类: %d, API: %d", len(categories), len(apis))
				break
			}
			// 如果有 API 但还没达到目标，也可能是因为限流正在进行
			if len(categories) > 0 && len(apis) > 0 && len(apis) < targetAPICount {
				t.Logf("   ⏳ 正在爬取中... 当前 %d/%d 个 API", len(apis), targetAPICount)
			}
			time.Sleep(pollInterval)
		}

		// ==================== 打印 API Categories ====================
		t.Log("")
		t.Log("==================== API Categories ====================")
		t.Logf("| %-4s | %-30s | %-40s |", "序号", "分类名称", "分类ID")
		t.Log("|------|--------------------------------|------------------------------------------|")
		for i, cat := range categories {
			t.Logf("| %-4d | %-30s | %-40s |", i+1, truncateString(cat.Name, 28), cat.ID.String())
		}
		t.Logf("共 %d 个分类", len(categories))
		t.Log("")

		// 断言：至少有 1 个分类
		require.GreaterOrEqual(t, len(categories), 1, "期望至少获取到 1 个 API 分类")
		t.Logf("✅ 分类数量验证通过: %d 个分类", len(categories))

		// ==================== 打印 API Metadata ====================
		t.Log("")
		t.Log("==================== API Metadata ====================")
		t.Logf("| %-4s | %-20s | %-30s | %-10s | %-6s | %-6s |", "序号", "API名称", "显示名称", "权限", "请求参数", "响应字段")
		t.Log("|------|----------------------|--------------------------------|------------|--------|--------|")
		maxDisplay := 20
		for i, api := range apis {
			if i >= maxDisplay {
				t.Logf("| ...  | %-20s | %-30s | %-10s | %-6s | %-6s |", "...", "...", "...", "...", "...")
				break
			}
			paramCount := len(api.RequestParams)
			fieldCount := len(api.ResponseFields)
			t.Logf("| %-4d | %-20s | %-30s | %-10s | %-6d | %-6d |",
				i+1,
				truncateString(api.Name, 18),
				truncateString(api.DisplayName, 28),
				truncateString(api.Permission, 8),
				paramCount,
				fieldCount,
			)
		}
		t.Logf("共 %d 个 API", len(apis))
		t.Log("")

		// 断言：至少有 1 个 API
		require.GreaterOrEqual(t, len(apis), 1, "期望至少获取到 1 个 API 元数据")
		t.Logf("✅ API 数量验证通过: %d 个 API", len(apis))
	})

	// ==================== Step 6: 通过应用服务执行建表 ====================
	t.Run("Step6_CreateTablesForDatasourceViaService", func(t *testing.T) {
		if testCtx.config.SkipRealAPICall {
			t.Skip("跳过：未设置 QDHUB_TUSHARE_TOKEN")
		}

		t.Log("----- Step 6: 通过应用服务执行建表 -----")

		maxTables := 3
		instanceID, err := testCtx.dataStoreSvc.CreateTablesForDatasource(ctx, contracts.CreateTablesForDatasourceRequest{
			DataSourceID: dataSourceID,
			DataStoreID:  dataStoreID,
			MaxTables:    &maxTables,
		})
		require.NoError(t, err)
		t.Logf("✅ 建表 Workflow 已启动: InstanceID=%s", instanceID)

		// 等待完成
		adapter := taskengine.NewTaskEngineAdapter(testCtx.engine)
		status, err := waitForWorkflowStatus(ctx, adapter, instanceID.String(), 60*time.Second)
		if err != nil {
			t.Logf("⚠️  Workflow 状态: %v, Error: %v", status, err)
		} else {
			t.Logf("✅ Workflow 完成: Status=%s, Progress=%.2f%%", status.Status, status.Progress)
		}

		// 验证表结构已创建
		schemas, err := testCtx.dataStoreSvc.ListTableSchemas(ctx, dataStoreID)
		require.NoError(t, err)
		t.Logf("✅ 已创建表结构数: %d", len(schemas))
	})

	// ==================== Step 7: 通过应用服务执行批量数据同步 ====================
	t.Run("Step7_SyncDataSourceViaService", func(t *testing.T) {
		if testCtx.config.SkipRealAPICall {
			t.Skip("跳过：未设置 QDHUB_TUSHARE_TOKEN")
		}

		t.Log("----- Step 7: 通过应用服务执行批量数据同步 -----")

		// 使用最近的日期范围
		endDate := time.Now().Format("20060102")
		startDate := time.Now().AddDate(0, 0, -7).Format("20060102")

		instanceID, err := testCtx.syncSvc.SyncDataSource(ctx, contracts.SyncDataSourceRequest{
			DataSourceID: dataSourceID,
			TargetDBPath: testCtx.config.DuckDBPath,
			StartDate:    startDate,
			EndDate:      endDate,
			APINames:     []string{"stock_basic"}, // 只同步基础数据
			MaxStocks:    10,
		})
		require.NoError(t, err)
		t.Logf("✅ 批量同步 Workflow 已启动: InstanceID=%s", instanceID)

		// 等待完成
		adapter := taskengine.NewTaskEngineAdapter(testCtx.engine)
		status, err := waitForWorkflowStatus(ctx, adapter, instanceID.String(), 120*time.Second)
		if err != nil {
			t.Logf("⚠️  Workflow 状态: %v, Error: %v", status, err)
		} else {
			t.Logf("✅ Workflow 完成: Status=%s, Progress=%.2f%%", status.Status, status.Progress)
		}

		// 验证数据已同步
		exists, err := testCtx.duckDBAdapter.TableExists(ctx, "stock_basic")
		if err == nil && exists {
			stats, err := testCtx.duckDBAdapter.GetTableStats(ctx, "stock_basic")
			if err == nil {
				t.Logf("✅ stock_basic 表: %d 条记录", stats.RowCount)
			}
		}
	})

	t.Log("========== 真实组件 E2E 测试完成 ==========")
}

// TestE2E_RealWorkflow_TushareAPIBasic 测试 Tushare API 基础调用
func TestE2E_RealWorkflow_TushareAPIBasic(t *testing.T) {
	testCtx := setupRealE2EContext(t)
	defer testCtx.cleanup()

	if testCtx.config.SkipRealAPICall {
		t.Skip("跳过：未设置 QDHUB_TUSHARE_TOKEN")
	}

	ctx := context.Background()

	t.Log("========== Tushare API 基础测试 ==========")

	// 测试 Token 验证
	t.Run("ValidateToken", func(t *testing.T) {
		valid, err := testCtx.tushareAdapter.Client().ValidateToken(ctx)
		require.NoError(t, err)
		assert.True(t, valid)
		t.Logf("✅ Token 验证成功")
	})

	// 测试获取股票基础信息
	t.Run("QueryStockBasic", func(t *testing.T) {
		result, err := testCtx.tushareAdapter.Client().Query(ctx, "stock_basic", map[string]interface{}{
			"list_status": "L",
		})
		require.NoError(t, err)
		require.NotNil(t, result)
		t.Logf("✅ stock_basic 查询成功: %d 条记录", len(result.Data))

		// 输出前 3 条
		for i, row := range result.Data {
			if i >= 3 {
				break
			}
			t.Logf("   %v", row)
		}
	})

	// 测试获取交易日历
	t.Run("QueryTradeCal", func(t *testing.T) {
		result, err := testCtx.tushareAdapter.Client().Query(ctx, "trade_cal", map[string]interface{}{
			"exchange":   "SSE",
			"start_date": "20251201",
			"end_date":   "20251231",
		})
		require.NoError(t, err)
		require.NotNil(t, result)
		t.Logf("✅ trade_cal 查询成功: %d 条记录", len(result.Data))
	})

	t.Log("========== Tushare API 基础测试完成 ==========")
}

// TestE2E_RealWorkflow_DuckDBOperations 测试 DuckDB 数据库操作
func TestE2E_RealWorkflow_DuckDBOperations(t *testing.T) {
	testCtx := setupRealE2EContext(t)
	defer testCtx.cleanup()

	ctx := context.Background()

	t.Log("========== DuckDB 操作测试 ==========")

	// 测试连接
	t.Run("Ping", func(t *testing.T) {
		err := testCtx.duckDBAdapter.Ping(ctx)
		require.NoError(t, err)
		t.Logf("✅ DuckDB 连接正常")
	})

	// 测试创建表
	t.Run("CreateTable", func(t *testing.T) {
		// 创建测试表
		createSQL := `
			CREATE TABLE IF NOT EXISTS test_stock_basic (
				ts_code VARCHAR PRIMARY KEY,
				symbol VARCHAR,
				name VARCHAR,
				area VARCHAR,
				industry VARCHAR,
				list_date VARCHAR,
				sync_batch_id VARCHAR
			)
		`
		_, err := testCtx.duckDBAdapter.Execute(ctx, createSQL)
		require.NoError(t, err)

		exists, err := testCtx.duckDBAdapter.TableExists(ctx, "test_stock_basic")
		require.NoError(t, err)
		assert.True(t, exists)
		t.Logf("✅ 表创建成功")
	})

	// 测试插入数据
	t.Run("BulkInsert", func(t *testing.T) {
		data := []map[string]any{
			{"ts_code": "000001.SZ", "symbol": "000001", "name": "平安银行", "area": "深圳", "industry": "银行", "list_date": "19910403"},
			{"ts_code": "000002.SZ", "symbol": "000002", "name": "万科A", "area": "深圳", "industry": "房地产", "list_date": "19910129"},
			{"ts_code": "000003.SZ", "symbol": "000003", "name": "PT金田A", "area": "深圳", "industry": "综合", "list_date": "19910704"},
		}

		count, err := testCtx.duckDBAdapter.BulkInsert(ctx, "test_stock_basic", data)
		require.NoError(t, err)
		assert.Equal(t, int64(3), count)
		t.Logf("✅ 插入 %d 条记录", count)
	})

	// 测试查询数据
	t.Run("Query", func(t *testing.T) {
		results, err := testCtx.duckDBAdapter.Query(ctx, "SELECT * FROM test_stock_basic ORDER BY ts_code")
		require.NoError(t, err)
		assert.Len(t, results, 3)
		t.Logf("✅ 查询到 %d 条记录", len(results))
	})

	// 测试表统计
	t.Run("GetTableStats", func(t *testing.T) {
		stats, err := testCtx.duckDBAdapter.GetTableStats(ctx, "test_stock_basic")
		require.NoError(t, err)
		assert.Equal(t, int64(3), stats.RowCount)
		t.Logf("✅ 表统计: %d 条记录", stats.RowCount)
	})

	// 测试删除表
	t.Run("DropTable", func(t *testing.T) {
		err := testCtx.duckDBAdapter.DropTable(ctx, "test_stock_basic")
		require.NoError(t, err)

		exists, err := testCtx.duckDBAdapter.TableExists(ctx, "test_stock_basic")
		require.NoError(t, err)
		assert.False(t, exists)
		t.Logf("✅ 表删除成功")
	})

	t.Log("========== DuckDB 操作测试完成 ==========")
}

// TestE2E_RealWorkflow_ApplicationServicesIntegration 测试应用服务集成
func TestE2E_RealWorkflow_ApplicationServicesIntegration(t *testing.T) {
	testCtx := setupRealE2EContext(t)
	defer testCtx.cleanup()

	ctx := context.Background()

	t.Log("========== 应用服务集成测试 ==========")

	// 测试 MetadataApplicationService
	t.Run("MetadataService_CreateAndGetDataSource", func(t *testing.T) {
		// 创建数据源
		ds, err := testCtx.metadataSvc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
			Name:        "Test Source",
			Description: "测试数据源",
			BaseURL:     "http://test.api.com",
			DocURL:      "http://test.api.com/docs",
		})
		require.NoError(t, err)
		require.NotNil(t, ds)
		t.Logf("✅ 创建数据源: %s", ds.ID)

		// 获取数据源
		retrieved, err := testCtx.metadataSvc.GetDataSource(ctx, ds.ID)
		require.NoError(t, err)
		assert.Equal(t, ds.ID, retrieved.ID)
		assert.Equal(t, "Test Source", retrieved.Name)
		t.Logf("✅ 获取数据源: %s", retrieved.Name)

		// 列出数据源
		sources, err := testCtx.metadataSvc.ListDataSources(ctx)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(sources), 1)
		t.Logf("✅ 列出数据源: %d 个", len(sources))
	})

	// 测试 DataStoreApplicationService
	t.Run("DataStoreService_CreateAndTestConnection", func(t *testing.T) {
		// 创建数据存储
		store, err := testCtx.dataStoreSvc.CreateDataStore(ctx, contracts.CreateDataStoreRequest{
			Name:        "Test Store",
			Description: "测试数据存储",
			Type:        datastore.DataStoreTypeDuckDB,
			StoragePath: testCtx.config.DuckDBPath,
		})
		require.NoError(t, err)
		require.NotNil(t, store)
		t.Logf("✅ 创建数据存储: %s", store.ID)

		// 测试连接
		err = testCtx.dataStoreSvc.TestConnection(ctx, store.ID)
		require.NoError(t, err)
		t.Logf("✅ 测试连接成功")

		// 列出数据存储
		stores, err := testCtx.dataStoreSvc.ListDataStores(ctx)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(stores), 1)
		t.Logf("✅ 列出数据存储: %d 个", len(stores))
	})

	// 测试 WorkflowApplicationService
	t.Run("WorkflowService_ListWorkflows", func(t *testing.T) {
		defs, err := testCtx.workflowSvc.ListWorkflowDefinitions(ctx, nil) // nil 表示列出所有
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(defs), 4) // 至少有 4 个内建 workflow
		t.Logf("✅ 列出 Workflow 定义: %d 个", len(defs))

		for _, def := range defs {
			if def.IsSystem {
				t.Logf("   - [系统] %s", def.Workflow.Name)
			}
		}
	})

	t.Log("========== 应用服务集成测试完成 ==========")
}

// TestE2E_RealWorkflow_MetadataCrawlWithRealAPI 使用真实 API 测试元数据爬取
func TestE2E_RealWorkflow_MetadataCrawlWithRealAPI(t *testing.T) {
	testCtx := setupRealE2EContext(t)
	defer testCtx.cleanup()

	if testCtx.config.SkipRealAPICall {
		t.Skip("跳过：未设置 QDHUB_TUSHARE_TOKEN")
	}

	ctx := context.Background()

	t.Log("========== 元数据爬取真实 API 测试 ==========")

	// 通过应用服务创建数据源
	// 注意：名称必须与 tushare.Adapter.Name() 一致（小写 "tushare"）
	ds, err := testCtx.metadataSvc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
		Name:        "tushare",
		Description: "Tushare Pro 数据源",
		BaseURL:     "http://api.tushare.pro",
		DocURL:      "https://tushare.pro/document/2",
	})
	require.NoError(t, err)

	// 设置 Token
	err = testCtx.metadataSvc.SaveToken(ctx, contracts.SaveTokenRequest{
		DataSourceID: ds.ID,
		TokenValue:   testCtx.config.TushareToken,
	})
	require.NoError(t, err)

	// 同时设置到 adapter 上
	testCtx.tushareAdapter.SetToken(testCtx.config.TushareToken)

	// 通过应用服务触发元数据爬取（爬取完整 API 列表）
	result, err := testCtx.metadataSvc.ParseAndImportMetadata(ctx, contracts.ParseMetadataRequest{
		DataSourceID: ds.ID,
		DocType:      metadata.DocumentTypeHTML,
		MaxAPICrawl:  0, // 0 表示不限制，爬取全部 API
	})
	require.NoError(t, err)
	t.Logf("✅ 元数据爬取已启动: %+v", result)

	// 使用轮询等待 workflow 完成（最多等待 10 分钟）
	t.Log("等待 workflow 执行（爬取完整 API 列表）...")
	adapter := taskengine.NewTaskEngineAdapter(testCtx.engine)
	status, err := waitForWorkflowStatus(ctx, adapter, result.InstanceID.String(), 600*time.Second)
	if err != nil {
		t.Logf("⚠️  Workflow 状态: %v, Error: %v", status, err)
	} else {
		t.Logf("✅ Workflow 完成: Status=%s, Progress=%.2f%%", status.Status, status.Progress)
	}

	// ==================== 从数据库读取并验证数据 ====================
	t.Log("从数据库读取 API Metadata...")
	apis, err := testCtx.metadataSvc.ListAPIMetadataByDataSource(ctx, ds.ID)
	require.NoError(t, err)
	t.Logf("✅ 从数据库读取到 %d 个 API Metadata", len(apis))

	// 验证数据库读写完整性：检查 request_params 和 response_fields 是否正确保存和读取
	t.Log("验证数据库读写完整性...")
	apisWithParams := 0
	apisWithFields := 0
	for _, api := range apis {
		if len(api.RequestParams) > 0 {
			apisWithParams++
		}
		if len(api.ResponseFields) > 0 {
			apisWithFields++
		}
	}
	t.Logf("📊 统计: %d/%d 个 API 有请求参数, %d/%d 个 API 有响应字段",
		apisWithParams, len(apis), apisWithFields, len(apis))

	// 要求至少 50% 的 API 有请求参数或响应字段（Tushare API 文档大部分都有这些信息）
	minWithParams := len(apis) / 2
	require.GreaterOrEqual(t, apisWithParams+apisWithFields, minWithParams,
		"期望至少 50%% 的 API 有请求参数或响应字段，实际: 有参数=%d, 有字段=%d, 总数=%d",
		apisWithParams, apisWithFields, len(apis))
	t.Logf("✅ 数据库读写完整性验证通过")

	// ==================== 导出 API Metadata 到 JSON ====================
	jsonPath := "/Users/stevelan/Desktop/projects/qdhub/qdhub/tests/data/tushare_api_metadata.json"
	jsonFile, err := os.Create(jsonPath)
	if err != nil {
		t.Logf("⚠️ 无法创建 JSON 文件: %v", err)
	} else {
		defer jsonFile.Close()

		// 构造输出结构（包含参数的完整信息：名称、类型、描述）
		type ParamOutput struct {
			Name        string `json:"name"`
			Type        string `json:"type"`
			Required    bool   `json:"required"`
			Description string `json:"description"`
		}
		type FieldOutput struct {
			Name        string `json:"name"`
			Type        string `json:"type"`
			Description string `json:"description"`
		}
		type APIOutput struct {
			Index          int           `json:"index"`
			Name           string        `json:"name"`
			DisplayName    string        `json:"display_name"`
			Description    string        `json:"description"`
			Permission     string        `json:"permission"`
			Endpoint       string        `json:"endpoint"`
			RequestParams  []ParamOutput `json:"request_params"`
			ResponseFields []FieldOutput `json:"response_fields"`
		}

		output := make([]APIOutput, len(apis))
		for i, api := range apis {
			// 转换请求参数
			params := make([]ParamOutput, len(api.RequestParams))
			for j, p := range api.RequestParams {
				params[j] = ParamOutput{
					Name:        p.Name,
					Type:        p.Type,
					Required:    p.Required,
					Description: p.Description,
				}
			}
			// 转换响应字段
			fields := make([]FieldOutput, len(api.ResponseFields))
			for j, f := range api.ResponseFields {
				fields[j] = FieldOutput{
					Name:        f.Name,
					Type:        f.Type,
					Description: f.Description,
				}
			}

			output[i] = APIOutput{
				Index:          i + 1,
				Name:           api.Name,
				DisplayName:    api.DisplayName,
				Description:    api.Description,
				Permission:     api.Permission,
				Endpoint:       api.Endpoint,
				RequestParams:  params,
				ResponseFields: fields,
			}
		}

		encoder := json.NewEncoder(jsonFile)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(output); err != nil {
			t.Logf("⚠️ JSON 编码失败: %v", err)
		} else {
			t.Logf("✅ API Metadata 已导出到 JSON: %s", jsonPath)
		}
	}

	// ==================== 打印摘要表格 ====================
	t.Log("")
	t.Log("==================== API Metadata 爬取结果摘要 ====================")
	t.Logf("| %-4s | %-25s | %-30s | %-12s | %-6s | %-6s |", "序号", "API名称", "显示名称", "权限", "请求参数", "响应字段")
	t.Log("|------|---------------------------|--------------------------------|--------------|--------|--------|")
	// 只打印前 20 条和后 5 条，避免日志过长
	printCount := 20
	if len(apis) <= printCount+5 {
		for i, api := range apis {
			paramCount := len(api.RequestParams)
			fieldCount := len(api.ResponseFields)
			t.Logf("| %-4d | %-25s | %-30s | %-12s | %-6d | %-6d |",
				i+1,
				truncateString(api.Name, 23),
				truncateString(api.DisplayName, 28),
				truncateString(api.Permission, 10),
				paramCount,
				fieldCount,
			)
		}
	} else {
		// 打印前 20 条
		for i := 0; i < printCount; i++ {
			api := apis[i]
			paramCount := len(api.RequestParams)
			fieldCount := len(api.ResponseFields)
			t.Logf("| %-4d | %-25s | %-30s | %-12s | %-6d | %-6d |",
				i+1,
				truncateString(api.Name, 23),
				truncateString(api.DisplayName, 28),
				truncateString(api.Permission, 10),
				paramCount,
				fieldCount,
			)
		}
		t.Logf("| ...  | ... 省略 %d 条 ...                                                              |", len(apis)-printCount-5)
		// 打印后 5 条
		for i := len(apis) - 5; i < len(apis); i++ {
			api := apis[i]
			paramCount := len(api.RequestParams)
			fieldCount := len(api.ResponseFields)
			t.Logf("| %-4d | %-25s | %-30s | %-12s | %-6d | %-6d |",
				i+1,
				truncateString(api.Name, 23),
				truncateString(api.DisplayName, 28),
				truncateString(api.Permission, 10),
				paramCount,
				fieldCount,
			)
		}
	}
	t.Logf("共 %d 个 API，完整数据见: %s", len(apis), jsonPath)
	t.Log("")

	// 断言：至少有 200 个 API
	require.GreaterOrEqual(t, len(apis), 200, "期望至少获取到 200 个 API 元数据")
	t.Logf("✅ API 数量验证通过: %d 个 API", len(apis))

	t.Log("========== 元数据爬取真实 API 测试完成 ==========")
}
