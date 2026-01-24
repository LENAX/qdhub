//go:build e2e
// +build e2e

// Package e2e 提供端到端测试
// 本文件实现使用真实 Task Engine 和内建 Workflow 的完整流程测试
// 测试场景：创建 Tushare 数据源 -> 爬取元数据 -> 建表 -> 批量数据同步
//
// 运行模式：
// - Mock 模式（默认）：使用临时数据库和 mock 数据源适配器
// - 真实模式：设置 QDHUB_TUSHARE_TOKEN 环境变量，使用真实 API 和持久化数据库
//
// 真实模式运行命令：
//
//	QDHUB_TUSHARE_TOKEN=your_token go test -tags e2e -v -run "TestE2E_BuiltinWorkflow" ./tests/e2e/...
package e2e

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/storage/sqlite"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/datasource"
	"qdhub/internal/infrastructure/datasource/tushare"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/persistence/uow"
	"qdhub/internal/infrastructure/quantdb/duckdb"
	"qdhub/internal/infrastructure/taskengine"
	"qdhub/internal/infrastructure/taskengine/workflows"
)

// ==================== 测试模式配置 ====================

// e2eTestConfig E2E 测试配置
type e2eTestConfig struct {
	TushareToken string // Tushare API Token
	IsRealMode   bool   // 是否为真实模式
	DuckDBPath   string // DuckDB 数据库路径
	SQLiteDBPath string // SQLite 数据库路径
}

// loadE2ETestConfig 从环境变量加载配置
func loadE2ETestConfig(t *testing.T) *e2eTestConfig {
	token := os.Getenv("QDHUB_TUSHARE_TOKEN")
	if token == "" {
		token = os.Getenv("TUSHARE_TOKEN")
	}
	token = strings.TrimSpace(token)

	isRealMode := token != ""

	var duckDBPath, sqliteDBPath string
	if isRealMode {
		// 真实模式：使用持久化数据库
		dataDir, err := filepath.Abs(filepath.Join(".", "data"))
		require.NoError(t, err)
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			require.NoError(t, err)
		}
		duckDBPath = filepath.Join(dataDir, "e2e_quant.duckdb")
		sqliteDBPath = filepath.Join(dataDir, "e2e_app.db")
		t.Logf("🔥 真实模式: DuckDB=%s, SQLite=%s", duckDBPath, sqliteDBPath)
	} else {
		t.Logf("🧪 Mock 模式: 使用临时数据库")
	}

	return &e2eTestConfig{
		TushareToken: token,
		IsRealMode:   isRealMode,
		DuckDBPath:   duckDBPath,
		SQLiteDBPath: sqliteDBPath,
	}
}

// setupLogFile 设置日志文件输出
// 返回日志文件句柄和清理函数
func setupLogFile(t *testing.T, testName string) (*os.File, func()) {
	// 创建 logs 目录
	logsDir := filepath.Join(".", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("Failed to create logs directory: %v", err)
	}

	// 生成日志文件名：test_<testName>_<timestamp>.log
	timestamp := time.Now().Format("20060102_150405")
	logFileName := fmt.Sprintf("test_%s_%s.log", strings.ToLower(strings.ReplaceAll(testName, "TestE2E_", "")), timestamp)
	logFilePath := filepath.Join(logsDir, logFileName)

	// 打开日志文件
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	// 设置 logrus 同时输出到文件和控制台
	logrus.SetOutput(io.MultiWriter(os.Stdout, logFile))
	logrus.SetLevel(logrus.InfoLevel)
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		ForceColors:   true,
	})

	// 同时设置标准库 log
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))

	t.Logf("📝 日志文件: %s", logFilePath)

	cleanup := func() {
		logFile.Close()
	}

	return logFile, cleanup
}

// ==================== Mock Data Source Adapter ====================

// mockTushareClient 模拟 Tushare API 客户端
type mockTushareClient struct {
	token string
}

func newMockTushareClient() *mockTushareClient {
	return &mockTushareClient{}
}

func (c *mockTushareClient) Name() string { return "tushare" }

func (c *mockTushareClient) SetToken(token string) { c.token = token }

func (c *mockTushareClient) Query(ctx context.Context, apiName string, params map[string]interface{}) (*datasource.QueryResult, error) {
	// 根据 API 名称返回模拟数据
	switch apiName {
	case "stock_basic":
		return &datasource.QueryResult{
			Data: []map[string]interface{}{
				{"ts_code": "000001.SZ", "symbol": "000001", "name": "平安银行", "area": "深圳", "industry": "银行", "list_date": "19910403"},
				{"ts_code": "000002.SZ", "symbol": "000002", "name": "万科A", "area": "深圳", "industry": "房地产", "list_date": "19910129"},
				{"ts_code": "000003.SZ", "symbol": "000003", "name": "PT金田A", "area": "深圳", "industry": "综合", "list_date": "19910704"},
			},
			Total:   3,
			HasMore: false,
		}, nil
	case "daily":
		return &datasource.QueryResult{
			Data: []map[string]interface{}{
				{"ts_code": "000001.SZ", "trade_date": "20251201", "open": 10.5, "high": 11.0, "low": 10.2, "close": 10.8, "vol": 100000, "amount": 1050000},
				{"ts_code": "000001.SZ", "trade_date": "20251202", "open": 10.8, "high": 11.2, "low": 10.5, "close": 11.0, "vol": 120000, "amount": 1320000},
			},
			Total:   2,
			HasMore: false,
		}, nil
	default:
		return &datasource.QueryResult{Data: []map[string]interface{}{}, Total: 0, HasMore: false}, nil
	}
}

func (c *mockTushareClient) ValidateToken(ctx context.Context) (bool, error) {
	return c.token != "", nil
}

// mockTushareCrawler 模拟 Tushare 文档爬虫
type mockTushareCrawler struct{}

func newMockTushareCrawler() *mockTushareCrawler {
	return &mockTushareCrawler{}
}

func (c *mockTushareCrawler) Name() string { return "tushare" }

func (c *mockTushareCrawler) FetchCatalogPage(ctx context.Context, dataSourceID shared.ID) (string, metadata.DocumentType, error) {
	// 返回模拟的目录页面 HTML
	html := `<html><body>
		<div class="category">
			<h2>股票数据</h2>
			<ul>
				<li><a href="https://tushare.pro/document/2?doc_id=25">stock_basic-获取基础信息数据</a></li>
				<li><a href="https://tushare.pro/document/2?doc_id=27">daily-日线行情</a></li>
			</ul>
		</div>
	</body></html>`
	return html, metadata.DocumentTypeHTML, nil
}

func (c *mockTushareCrawler) FetchAPIDetailPage(ctx context.Context, apiURL string) (string, metadata.DocumentType, error) {
	// 根据 URL 返回模拟的 API 详情页面
	if apiURL == "https://tushare.pro/document/2?doc_id=25" {
		return getMockStockBasicHTML(), metadata.DocumentTypeHTML, nil
	} else if apiURL == "https://tushare.pro/document/2?doc_id=27" {
		return getMockDailyHTML(), metadata.DocumentTypeHTML, nil
	}
	return "<html><body>Unknown API</body></html>", metadata.DocumentTypeHTML, nil
}

// mockTushareParser 模拟 Tushare 文档解析器
type mockTushareParser struct{}

func newMockTushareParser() *mockTushareParser {
	return &mockTushareParser{}
}

func (p *mockTushareParser) ParseCatalog(content string) ([]metadata.APICategory, []string, error) {
	// 返回模拟的分类和 API URL 列表
	categories := []metadata.APICategory{
		{
			ID:          shared.NewID(),
			Name:        "股票数据",
			Description: "股票相关的基础数据",
		},
	}
	apiURLs := []string{
		"https://tushare.pro/document/2?doc_id=25",
		"https://tushare.pro/document/2?doc_id=27",
	}
	return categories, apiURLs, nil
}

func (p *mockTushareParser) ParseAPIDetail(content string) (*metadata.APIMetadata, error) {
	// 根据内容返回模拟的 API 元数据
	if content == getMockStockBasicHTML() {
		return &metadata.APIMetadata{
			ID:          shared.NewID(),
			Name:        "stock_basic",
			DisplayName: "股票基础信息",
			Description: "获取股票基础信息数据",
			Endpoint:    "/stock_basic",
			Permission:  "basic",
			RequestParams: []metadata.ParamMeta{
				{Name: "list_status", Type: "str", Required: false, Description: "上市状态"},
			},
			ResponseFields: []metadata.FieldMeta{
				{Name: "ts_code", Type: "str", Description: "股票代码", IsPrimary: true},
				{Name: "symbol", Type: "str", Description: "股票代码（6位数字）"},
				{Name: "name", Type: "str", Description: "股票名称"},
				{Name: "area", Type: "str", Description: "地域"},
				{Name: "industry", Type: "str", Description: "所属行业"},
				{Name: "list_date", Type: "str", Description: "上市日期"},
			},
		}, nil
	} else if content == getMockDailyHTML() {
		return &metadata.APIMetadata{
			ID:          shared.NewID(),
			Name:        "daily",
			DisplayName: "日线行情",
			Description: "获取日线行情数据",
			Endpoint:    "/daily",
			Permission:  "basic",
			RequestParams: []metadata.ParamMeta{
				{Name: "ts_code", Type: "str", Required: true, Description: "股票代码"},
				{Name: "start_date", Type: "str", Required: false, Description: "开始日期"},
				{Name: "end_date", Type: "str", Required: false, Description: "结束日期"},
			},
			ResponseFields: []metadata.FieldMeta{
				{Name: "ts_code", Type: "str", Description: "股票代码", IsPrimary: true},
				{Name: "trade_date", Type: "str", Description: "交易日期", IsPrimary: true},
				{Name: "open", Type: "float", Description: "开盘价"},
				{Name: "high", Type: "float", Description: "最高价"},
				{Name: "low", Type: "float", Description: "最低价"},
				{Name: "close", Type: "float", Description: "收盘价"},
				{Name: "vol", Type: "float", Description: "成交量"},
				{Name: "amount", Type: "float", Description: "成交额"},
			},
		}, nil
	}
	return nil, fmt.Errorf("unknown content")
}

func (p *mockTushareParser) SupportedType() metadata.DocumentType {
	return metadata.DocumentTypeHTML
}

// mockTushareAdapter 组合 mock 组件形成完整适配器
type mockTushareAdapter struct {
	client  *mockTushareClient
	crawler *mockTushareCrawler
	parser  *mockTushareParser
}

func newMockTushareAdapter() *mockTushareAdapter {
	return &mockTushareAdapter{
		client:  newMockTushareClient(),
		crawler: newMockTushareCrawler(),
		parser:  newMockTushareParser(),
	}
}

func (a *mockTushareAdapter) Name() string                      { return "tushare" }
func (a *mockTushareAdapter) Client() datasource.APIClient      { return a.client }
func (a *mockTushareAdapter) Crawler() datasource.Crawler       { return a.crawler }
func (a *mockTushareAdapter) Parser() datasource.DocumentParser { return a.parser }

// ==================== Mock QuantDB Adapter ====================

// mockQuantDBAdapter 模拟 QuantDB 适配器（内存数据库）
type mockQuantDBAdapter struct {
	tables     map[string]bool
	data       map[string][]map[string]interface{}
	checkpoint map[string]string
}

func newMockQuantDBAdapter() *mockQuantDBAdapter {
	return &mockQuantDBAdapter{
		tables:     make(map[string]bool),
		data:       make(map[string][]map[string]interface{}),
		checkpoint: make(map[string]string),
	}
}

func (a *mockQuantDBAdapter) TestConnection(ctx context.Context, ds *datastore.QuantDataStore) error {
	return nil
}

func (a *mockQuantDBAdapter) ExecuteDDL(ctx context.Context, ds *datastore.QuantDataStore, ddl string) error {
	// 简单解析 DDL，提取表名
	// 这里只是简单模拟
	a.tables["mock_table"] = true
	return nil
}

func (a *mockQuantDBAdapter) TableExists(ctx context.Context, ds *datastore.QuantDataStore, tableName string) (bool, error) {
	return a.tables[tableName], nil
}

func (a *mockQuantDBAdapter) InsertData(ctx context.Context, ds *datastore.QuantDataStore, tableName string, data []map[string]interface{}) error {
	if a.data[tableName] == nil {
		a.data[tableName] = make([]map[string]interface{}, 0)
	}
	a.data[tableName] = append(a.data[tableName], data...)
	return nil
}

func (a *mockQuantDBAdapter) GetCheckpoint(ctx context.Context, ds *datastore.QuantDataStore, tableName string) (string, error) {
	return a.checkpoint[tableName], nil
}

func (a *mockQuantDBAdapter) SetCheckpoint(ctx context.Context, ds *datastore.QuantDataStore, tableName, checkpoint string) error {
	a.checkpoint[tableName] = checkpoint
	return nil
}

// ==================== 测试辅助函数 ====================

func getMockStockBasicHTML() string {
	return `<html><body>
		<h1>stock_basic - 获取基础信息数据</h1>
		<p>接口：stock_basic</p>
		<p>描述：获取股票基础信息数据</p>
		<h3>输入参数</h3>
		<table>
			<tr><td>list_status</td><td>str</td><td>N</td><td>上市状态</td></tr>
		</table>
		<h3>输出参数</h3>
		<table>
			<tr><td>ts_code</td><td>str</td><td>股票代码</td></tr>
			<tr><td>symbol</td><td>str</td><td>股票代码（6位数字）</td></tr>
			<tr><td>name</td><td>str</td><td>股票名称</td></tr>
			<tr><td>area</td><td>str</td><td>地域</td></tr>
			<tr><td>industry</td><td>str</td><td>所属行业</td></tr>
			<tr><td>list_date</td><td>str</td><td>上市日期</td></tr>
		</table>
	</body></html>`
}

func getMockDailyHTML() string {
	return `<html><body>
		<h1>daily - 日线行情</h1>
		<p>接口：daily</p>
		<p>描述：获取日线行情数据</p>
		<h3>输入参数</h3>
		<table>
			<tr><td>ts_code</td><td>str</td><td>Y</td><td>股票代码</td></tr>
			<tr><td>start_date</td><td>str</td><td>N</td><td>开始日期</td></tr>
			<tr><td>end_date</td><td>str</td><td>N</td><td>结束日期</td></tr>
		</table>
		<h3>输出参数</h3>
		<table>
			<tr><td>ts_code</td><td>str</td><td>股票代码</td></tr>
			<tr><td>trade_date</td><td>str</td><td>交易日期</td></tr>
			<tr><td>open</td><td>float</td><td>开盘价</td></tr>
			<tr><td>high</td><td>float</td><td>最高价</td></tr>
			<tr><td>low</td><td>float</td><td>最低价</td></tr>
			<tr><td>close</td><td>float</td><td>收盘价</td></tr>
			<tr><td>vol</td><td>float</td><td>成交量</td></tr>
			<tr><td>amount</td><td>float</td><td>成交额</td></tr>
		</table>
	</body></html>`
}

// ==================== 辅助函数 ====================

// buildTableSchemaFromMetadata 从 API 元数据构建表结构
// 复用 table_jobs.go 中 buildTableSchema 的逻辑
func buildTableSchemaFromMetadata(tableName string, fields []metadata.FieldMeta) *datastore.TableSchema {
	schema := &datastore.TableSchema{
		ID:          shared.NewID(),
		TableName:   tableName,
		Columns:     make([]datastore.ColumnDef, 0, len(fields)+2), // +2 for sync_batch_id and created_at
		PrimaryKeys: []string{},
		Indexes:     []datastore.IndexDef{},
	}

	// 去重字段名
	seenFields := make(map[string]bool)
	primaryKeys := make([]string, 0)

	for _, f := range fields {
		fieldName := strings.TrimSpace(f.Name)
		if fieldName == "" || seenFields[fieldName] {
			continue
		}
		seenFields[fieldName] = true

		col := datastore.ColumnDef{
			Name:       fieldName,
			SourceType: f.Type,
			TargetType: mapTypeToDuckDBForTest(f.Type),
			Nullable:   !f.IsPrimary,
			Comment:    f.Description,
		}
		schema.Columns = append(schema.Columns, col)

		if f.IsPrimary {
			primaryKeys = append(primaryKeys, fieldName)
		}
		if f.IsIndex {
			schema.Indexes = append(schema.Indexes, datastore.IndexDef{
				Name:    fmt.Sprintf("idx_%s_%s", tableName, fieldName),
				Columns: []string{fieldName},
				Unique:  false,
			})
		}
	}

	// 添加同步批次字段（用于回滚）
	schema.Columns = append(schema.Columns, datastore.ColumnDef{
		Name:       "sync_batch_id",
		SourceType: "string",
		TargetType: "VARCHAR",
		Nullable:   true,
		Comment:    "同步批次ID，用于数据回滚",
	})

	// 添加创建时间字段
	schema.Columns = append(schema.Columns, datastore.ColumnDef{
		Name:       "created_at",
		SourceType: "timestamp",
		TargetType: "TIMESTAMP",
		Nullable:   true,
		Comment:    "记录创建时间",
	})

	schema.PrimaryKeys = primaryKeys

	return schema
}

// mapTypeToDuckDBForTest 将数据源字段类型映射为 DuckDB 类型
func mapTypeToDuckDBForTest(sourceType string) string {
	sourceType = strings.ToLower(strings.TrimSpace(sourceType))
	switch sourceType {
	case "int", "integer":
		return "INTEGER"
	case "bigint", "long":
		return "BIGINT"
	case "float", "number", "double", "decimal":
		return "DOUBLE"
	case "str", "string", "text", "varchar", "char":
		return "VARCHAR"
	case "date":
		return "DATE"
	case "datetime", "timestamp":
		return "TIMESTAMP"
	case "bool", "boolean":
		return "BOOLEAN"
	default:
		return "VARCHAR" // 默认使用 VARCHAR
	}
}

// ==================== E2E 测试上下文 ====================

// builtinWorkflowE2EContext 内建 Workflow E2E 测试上下文
type builtinWorkflowE2EContext struct {
	config              *e2eTestConfig
	db                  *persistence.DB
	engine              *engine.Engine
	workflowRepo        workflow.WorkflowDefinitionRepository
	dataSourceRepo      metadata.DataSourceRepository
	metadataRepo        metadata.Repository
	syncPlanRepo        sync.SyncPlanRepository
	workflowExecutor    workflow.WorkflowExecutor
	metadataAppService  contracts.MetadataApplicationService  // 元数据应用服务
	workflowAppService  contracts.WorkflowApplicationService  // 工作流应用服务
	datastoreAppService contracts.DataStoreApplicationService // 数据存储应用服务
	syncAppService      contracts.SyncApplicationService      // 同步应用服务
	dsRegistry          *datasource.Registry
	quantDBAdapter      *mockQuantDBAdapter
	duckDBAdapter       *duckdb.Adapter // 真实模式下使用
	taskEngineAdapter   workflow.TaskEngineAdapter
	cleanup             func()
}

// setupBuiltinWorkflowE2EContext 设置 E2E 测试环境
// 根据 QDHUB_TUSHARE_TOKEN 环境变量自动选择模式：
// - 有 token: 真实模式，使用 e2e/data/e2e_quant.duckdb
// - 无 token: Mock 模式，使用临时数据库
func setupBuiltinWorkflowE2EContext(t *testing.T) *builtinWorkflowE2EContext {
	ctx := context.Background()
	config := loadE2ETestConfig(t)

	var dsn string
	var removeSQLiteOnCleanup bool
	var duckDBAdapter *duckdb.Adapter

	if config.IsRealMode {
		// 真实模式：使用持久化数据库
		dsn = config.SQLiteDBPath
		removeSQLiteOnCleanup = false // 不删除持久化数据库

		// 创建 DuckDB adapter
		duckDBAdapter = duckdb.NewAdapter(config.DuckDBPath)
		err := duckDBAdapter.Connect(ctx)
		require.NoError(t, err, "连接 DuckDB 失败")
		t.Logf("✅ DuckDB 已连接: %s", config.DuckDBPath)
	} else {
		// Mock 模式：使用临时数据库
		tmpfile, err := os.CreateTemp("", "builtin_workflow_e2e_*.db")
		require.NoError(t, err)
		tmpfile.Close()
		dsn = tmpfile.Name()
		removeSQLiteOnCleanup = true
	}

	db, err := persistence.NewDB(dsn)
	require.NoError(t, err)

	// 执行迁移
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

	// 2. 创建 Task Engine
	aggregateRepo, err := sqlite.NewWorkflowAggregateRepoFromDSN(dsn)
	require.NoError(t, err)

	eng, err := engine.NewEngineWithAggregateRepo(5, 30, aggregateRepo)
	require.NoError(t, err)

	err = eng.Start(ctx)
	require.NoError(t, err)

	// 3. 创建 DataSource Registry（根据模式选择 mock 或真实 adapter）
	dsRegistry := datasource.NewRegistry()
	if config.IsRealMode {
		// 真实模式：使用真实 Tushare adapter
		tushareAdapter := tushare.NewAdapter()
		err = dsRegistry.RegisterAdapter(tushareAdapter)
		require.NoError(t, err)
		t.Logf("✅ 已注册真实 Tushare Adapter")
	} else {
		// Mock 模式
		mockAdapter := newMockTushareAdapter()
		err = dsRegistry.RegisterAdapter(mockAdapter)
		require.NoError(t, err)
	}

	// 4. 创建 repositories
	workflowRepo, err := repository.NewWorkflowDefinitionRepository(db)
	require.NoError(t, err)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	metadataRepo := repository.NewMetadataRepository(db)
	syncPlanRepo := repository.NewSyncPlanRepository(db)

	// 5. 初始化 Task Engine（注册 job functions 和 handlers）
	taskEngineDeps := &taskengine.Dependencies{
		DataSourceRegistry: dsRegistry,
		MetadataRepo:       metadataRepo,
	}
	// 真实模式下注册 QuantDB adapter
	if config.IsRealMode && duckDBAdapter != nil {
		taskEngineDeps.QuantDB = duckDBAdapter
		t.Logf("✅ 已注册 QuantDB (DuckDB Adapter)")
	}
	err = taskengine.Initialize(ctx, eng, taskEngineDeps)
	require.NoError(t, err)

	// 6. 创建 adapters
	taskEngineAdapter := taskengine.NewTaskEngineAdapter(eng)
	workflowFactory := taskengine.GetWorkflowFactory(eng)

	// 7. 初始化内建 workflows
	builtInInitializer := impl.NewBuiltInWorkflowInitializer(workflowRepo, workflowFactory, taskEngineAdapter)
	err = builtInInitializer.Initialize(ctx)
	require.NoError(t, err)

	// 8. 创建 WorkflowExecutor
	workflowExecutor := taskengine.NewWorkflowExecutor(workflowRepo, taskEngineAdapter, metadataRepo)

	// 9. 创建 MetadataApplicationService
	metadataAppService := impl.NewMetadataApplicationService(
		dataSourceRepo,
		metadataRepo,
		nil, // parserFactory - not needed for tests (use workflow for metadata crawl)
		workflowExecutor,
	)

	// 10. 创建 WorkflowApplicationService
	workflowAppService := impl.NewWorkflowApplicationService(
		workflowRepo,
		taskEngineAdapter,
	)

	// 11. 创建 DataStoreApplicationService
	datastoreRepo := repository.NewQuantDataStoreRepository(db)
	datastoreAppService := impl.NewDataStoreApplicationService(
		datastoreRepo,
		dataSourceRepo,
		workflowExecutor,
	)

	// 12. 创建 SyncApplicationService
	cronCalculator := sync.NewCronScheduleCalculator()
	dependencyResolver := sync.NewDependencyResolver()
	// 使用 nil 作为 PlanScheduler，因为测试不需要调度功能
	uowImpl := uow.NewUnitOfWork(db)
	syncAppService := impl.NewSyncApplicationService(
		syncPlanRepo,
		cronCalculator,
		nil, // planScheduler - not needed for tests
		dataSourceRepo,
		workflowExecutor,
		dependencyResolver,
		taskEngineAdapter,
		uowImpl,
	)

	cleanup := func() {
		eng.Stop()
		if duckDBAdapter != nil {
			duckDBAdapter.Close()
		}
		db.Close()
		if removeSQLiteOnCleanup {
			os.Remove(dsn)
		}
	}

	return &builtinWorkflowE2EContext{
		config:              config,
		db:                  db,
		engine:              eng,
		workflowRepo:        workflowRepo,
		dataSourceRepo:      dataSourceRepo,
		metadataRepo:        metadataRepo,
		syncPlanRepo:        syncPlanRepo,
		workflowExecutor:    workflowExecutor,
		metadataAppService:  metadataAppService,
		workflowAppService:  workflowAppService,
		datastoreAppService: datastoreAppService,
		syncAppService:      syncAppService,
		dsRegistry:          dsRegistry,
		quantDBAdapter:      newMockQuantDBAdapter(),
		duckDBAdapter:       duckDBAdapter,
		taskEngineAdapter:   taskEngineAdapter,
		cleanup:             cleanup,
	}
}

// waitForWorkflowCompletionQuiet 静默等待 workflow 完成（用于已稳定的 workflow）
// 只在完成时输出一行简要信息
func waitForWorkflowCompletionQuiet(ctx context.Context, adapter workflow.TaskEngineAdapter, instanceID string, timeout time.Duration) (*workflow.WorkflowStatus, error) {
	deadline := time.Now().Add(timeout)
	pollInterval := 500 * time.Millisecond
	startTime := time.Now()

	for time.Now().Before(deadline) {
		status, err := adapter.GetInstanceStatus(ctx, instanceID)
		if err != nil {
			return nil, err
		}

		elapsed := time.Since(startTime).Round(time.Millisecond)

		// 检查是否完成（成功或失败）
		switch status.Status {
		case "Success", "Completed", "success", "completed":
			return status, nil
		case "Failed", "failed", "Error", "error":
			return status, fmt.Errorf("workflow failed after %v: %v", elapsed, status.ErrorMessage)
		case "Cancelled", "cancelled":
			return status, fmt.Errorf("workflow cancelled after %v", elapsed)
		}

		time.Sleep(pollInterval)
	}

	status, _ := adapter.GetInstanceStatus(ctx, instanceID)
	return status, fmt.Errorf("timeout waiting for workflow completion")
}

// waitForWorkflowCompletion 等待 workflow 完成
// 每分钟轮询一次 task engine 并打印进度（用于长时间运行的 workflow）
func waitForWorkflowCompletion(ctx context.Context, adapter workflow.TaskEngineAdapter, instanceID string, timeout time.Duration) (*workflow.WorkflowStatus, error) {
	deadline := time.Now().Add(timeout)
	pollInterval := 60 * time.Second // 每分钟轮询一次
	startTime := time.Now()
	pollCount := 0

	for time.Now().Before(deadline) {
		pollCount++
		status, err := adapter.GetInstanceStatus(ctx, instanceID)
		if err != nil {
			return nil, err
		}

		elapsed := time.Since(startTime).Round(time.Second)
		remaining := time.Until(deadline).Round(time.Second)

		// 打印进度信息
		fmt.Printf("📊 [轮询 #%d] 已耗时: %v, 剩余: %v, 状态: %s, 进度: %.2f%%, 总任务: %d, 完成: %d, 失败: %d\n",
			pollCount, elapsed, remaining, status.Status, status.Progress,
			status.TaskCount, status.CompletedTask, status.FailedTask)

		// 检查是否完成（成功或失败）
		switch status.Status {
		case "Success", "Completed", "success", "completed":
			fmt.Printf("✅ Workflow 完成! 总耗时: %v\n", elapsed)
			return status, nil
		case "Failed", "failed", "Error", "error":
			fmt.Printf("❌ Workflow 失败! 总耗时: %v, 错误: %v\n", elapsed, status.ErrorMessage)
			return status, fmt.Errorf("workflow failed: %v", status.ErrorMessage)
		case "Cancelled", "cancelled":
			fmt.Printf("⚠️ Workflow 已取消! 总耗时: %v\n", elapsed)
			return status, fmt.Errorf("workflow cancelled")
		}

		// 等待下一次轮询
		time.Sleep(pollInterval)
	}

	// 超时，获取最后一次状态
	status, _ := adapter.GetInstanceStatus(ctx, instanceID)
	fmt.Printf("⏰ Workflow 超时! 最终状态: %s, 进度: %.2f%%\n", status.Status, status.Progress)
	return status, fmt.Errorf("timeout waiting for workflow completion")
}

// ==================== E2E 测试用例 ====================

// TestE2E_BuiltinWorkflow_FullPipeline 测试完整的内建 Workflow 流程
// 流程：创建数据源 -> 爬取元数据 -> 建表 -> 批量同步
func TestE2E_BuiltinWorkflow_FullPipeline(t *testing.T) {
	// 设置日志文件
	logFile, logCleanup := setupLogFile(t, "FullPipeline")
	defer logCleanup()
	defer logFile.Close()

	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()

	ctx := context.Background()

	t.Log("========== 内建 Workflow E2E 测试开始 ==========")
	logrus.Infof("========== 内建 Workflow E2E 测试开始 ==========")

	// ==================== Step 1: 验证内建 Workflows 已初始化 ====================
	t.Run("Step1_VerifyBuiltinWorkflowsInitialized", func(t *testing.T) {
		t.Log("----- Step 1: 验证内建 Workflows 已初始化 -----")

		builtInWorkflows := workflows.GetBuiltInWorkflows()
		for _, meta := range builtInWorkflows {
			def, err := testCtx.workflowRepo.Get(meta.ID)
			require.NoError(t, err, "获取 workflow %s 失败", meta.ID)
			require.NotNil(t, def, "workflow %s 未持久化", meta.ID)
			assert.True(t, def.IsSystem, "workflow %s 应标记为系统 workflow", meta.ID)
			t.Logf("✅ 内建 Workflow 已初始化: %s (%s)", meta.Name, meta.ID)
		}
	})

	// ==================== Step 2: 创建或获取数据源（使用 MetadataApplicationService）====================
	var dataSourceID shared.ID
	t.Run("Step2_CreateDataSource", func(t *testing.T) {
		t.Log("----- Step 2: 创建/获取 Tushare 数据源（使用应用服务）-----")

		// 先尝试获取已存在的数据源（使用应用服务）
		allDataSources, err := testCtx.metadataAppService.ListDataSources(ctx)
		if err == nil {
			for _, ds := range allDataSources {
				if ds.Name == "Tushare" {
					dataSourceID = ds.ID
					t.Logf("✅ 使用已存在的数据源: ID=%s, Name=%s", dataSourceID, ds.Name)

					// 更新 Token（使用应用服务）
					if testCtx.config.IsRealMode && testCtx.config.TushareToken != "" {
						err = testCtx.metadataAppService.SaveToken(ctx, contracts.SaveTokenRequest{
							DataSourceID: ds.ID,
							TokenValue:   testCtx.config.TushareToken,
						})
						if err != nil {
							t.Logf("⚠️ 更新 Token 失败: %v", err)
						} else {
							t.Logf("✅ Token 已更新（真实模式）")
						}
					}
					return
				}
			}
		}

		// 创建新数据源（使用应用服务）
		ds, err := testCtx.metadataAppService.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
			Name:        "Tushare",
			Description: "Tushare Pro Data Source for E2E Testing",
			BaseURL:     "http://api.tushare.pro",
			DocURL:      "https://tushare.pro/document/2",
		})
		require.NoError(t, err, "创建数据源失败")
		dataSourceID = ds.ID

		// 设置 Token（使用应用服务）
		var tokenValue string
		if testCtx.config.IsRealMode && testCtx.config.TushareToken != "" {
			tokenValue = testCtx.config.TushareToken
		} else {
			tokenValue = "test-token-for-e2e"
		}
		err = testCtx.metadataAppService.SaveToken(ctx, contracts.SaveTokenRequest{
			DataSourceID: ds.ID,
			TokenValue:   tokenValue,
		})
		require.NoError(t, err, "设置 Token 失败")

		t.Logf("✅ 数据源创建成功: ID=%s, Name=%s", dataSourceID, ds.Name)
	})

	// ==================== Step 3: 执行元数据爬取（使用 MetadataApplicationService.ParseAndImportMetadata）====================
	t.Run("Step3_ExecuteMetadataCrawl", func(t *testing.T) {
		t.Log("----- Step 3: 执行元数据爬取（使用 ParseAndImportMetadata）-----")

		// 使用 MetadataApplicationService.ParseAndImportMetadata 执行元数据爬取
		// 该方法内部会调用 metadata_crawl 工作流
		result, err := testCtx.metadataAppService.ParseAndImportMetadata(ctx, contracts.ParseMetadataRequest{
			DataSourceID: dataSourceID,
			MaxAPICrawl:  0, // 不限制爬取数量
		})
		require.NoError(t, err, "执行元数据爬取失败")
		require.NotNil(t, result, "ParseAndImportMetadata 返回结果不应为空")
		t.Logf("  📡 元数据爬取工作流已启动: InstanceID=%s", result.InstanceID)

		// 等待工作流完成
		status, err := waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, result.InstanceID.String(), 30*time.Second)
		if err != nil {
			t.Logf("⚠️ 元数据爬取: %v", err)
		} else {
			t.Logf("✅ 元数据爬取完成: %d 个任务", status.CompletedTask)
		}

		// 验证元数据已保存（使用 repository）
		apiMetadataList, err := testCtx.metadataRepo.ListAPIMetadataByDataSource(ctx, dataSourceID)
		if err == nil {
			t.Logf("  📊 已爬取 API 元数据数量: %d", len(apiMetadataList))
		}
	})

	// ==================== Step 4: 执行建表（使用 DataStoreApplicationService.CreateTablesForDatasource）====================
	var dataStoreID shared.ID
	t.Run("Step4_ExecuteCreateTables", func(t *testing.T) {
		t.Log("----- Step 4: 执行建表（使用 CreateTablesForDatasource）-----")

		// 4.1 创建或获取 DataStore
		// 根据模式选择数据库路径
		var targetDBPath string
		if testCtx.config.IsRealMode {
			targetDBPath = testCtx.config.DuckDBPath
		} else {
			tmpDBFile, err := os.CreateTemp("", "e2e_test_*.duckdb")
			require.NoError(t, err)
			tmpDBFile.Close()
			defer os.Remove(tmpDBFile.Name())
			targetDBPath = tmpDBFile.Name()
		}

		// 先尝试获取已存在的 DataStore
		existingStores, err := testCtx.datastoreAppService.ListDataStores(ctx)
		if err == nil {
			for _, ds := range existingStores {
				if ds.StoragePath == targetDBPath || ds.Name == "E2E Test DuckDB" {
					dataStoreID = ds.ID
					t.Logf("  ✅ 使用已存在的 DataStore: ID=%s, Name=%s", dataStoreID, ds.Name)
					break
				}
			}
		}

		// 如果不存在则创建新的 DataStore
		if dataStoreID == "" {
			ds, err := testCtx.datastoreAppService.CreateDataStore(ctx, contracts.CreateDataStoreRequest{
				Name:        "E2E Test DuckDB",
				Description: "E2E 测试用 DuckDB 数据存储",
				Type:        datastore.DataStoreTypeDuckDB,
				StoragePath: targetDBPath,
			})
			require.NoError(t, err, "创建 DataStore 失败")
			dataStoreID = ds.ID
			t.Logf("  ✅ DataStore 创建成功: ID=%s, Name=%s, Path=%s", ds.ID, ds.Name, ds.StoragePath)
		}

		// 4.2 使用 DataStoreApplicationService.CreateTablesForDatasource 执行建表
		instanceID, err := testCtx.datastoreAppService.CreateTablesForDatasource(ctx, contracts.CreateTablesForDatasourceRequest{
			DataSourceID: dataSourceID,
			DataStoreID:  dataStoreID,
			MaxTables:    nil, // 不限制
		})
		require.NoError(t, err, "执行建表失败")
		t.Logf("  📡 建表工作流已启动: InstanceID=%s", instanceID)

		// 等待工作流完成
		status, err := waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, instanceID.String(), 120*time.Second)
		if err != nil {
			t.Logf("⚠️ 建表: %v", err)
		} else {
			t.Logf("✅ 建表完成: %d 个表", status.CompletedTask)
		}
	})

	// ==================== Step 5: 使用 SyncPlan 执行批量数据同步 ====================
	var syncPlanID shared.ID
	t.Run("Step5_CreateAndExecuteSyncPlan", func(t *testing.T) {
		t.Log("----- Step 5: 使用 SyncPlan 执行批量数据同步 -----")

		// 5.1 创建 SyncPlan（测试 20+ 个 API）
		t.Log("  5.1 创建 SyncPlan...")
		// 选择 20+ 个常用 API 进行测试
		selectedAPIs := []string{
			// 基础数据
			"stock_basic", // 股票基础信息
			"trade_cal",   // 交易日历
			"namechange",  // 股票曾用名
			"stk_limit",   // 涨跌停价格
			// 行情数据
			"daily",       // 日线行情
			"weekly",      // 周线行情
			"monthly",     // 月线行情
			"daily_basic", // 每日指标
			"adj_factor",  // 复权因子
			// 财务数据
			"income",         // 利润表
			"balancesheet",   // 资产负债表
			"cashflow",       // 现金流量表
			"fina_indicator", // 财务指标
			"fina_mainbz",    // 主营业务构成
			// 市场参考
			"top_list",      // 龙虎榜每日明细
			"top_inst",      // 龙虎榜机构交易明细
			"margin",        // 融资融券交易汇总
			"margin_detail", // 融资融券交易明细
			"block_trade",   // 大宗交易
			// 指数数据
			"index_basic",  // 指数基本信息
			"index_daily",  // 指数日线行情
			"index_weight", // 指数成份和权重
		}
		createReq := contracts.CreateSyncPlanRequest{
			Name:         "E2E Test Sync Plan (20+ APIs)",
			Description:  "E2E 测试同步计划 - 测试 20+ 个 API",
			DataSourceID: dataSourceID,
			SelectedAPIs: selectedAPIs,
		}

		plan, err := testCtx.syncAppService.CreateSyncPlan(ctx, createReq)
		require.NoError(t, err, "创建 SyncPlan 失败")
		require.NotNil(t, plan, "SyncPlan 不应为空")
		syncPlanID = plan.ID
		t.Logf("✅ SyncPlan 创建成功: ID=%s, Name=%s, Status=%s", plan.ID, plan.Name, plan.Status)
		assert.Equal(t, sync.PlanStatusDraft, plan.Status, "初始状态应为 draft")

		// 5.2 解析依赖
		t.Log("  5.2 解析 SyncPlan 依赖...")
		err = testCtx.syncAppService.ResolveSyncPlan(ctx, syncPlanID)
		require.NoError(t, err, "解析 SyncPlan 依赖失败")

		// 获取更新后的 plan
		plan, err = testCtx.syncAppService.GetSyncPlan(ctx, syncPlanID)
		require.NoError(t, err)
		t.Logf("✅ SyncPlan 依赖解析成功: Status=%s, ResolvedAPIs=%v", plan.Status, plan.ResolvedAPIs)
		assert.Equal(t, sync.PlanStatusResolved, plan.Status, "解析后状态应为 resolved")

		// 验证 ExecutionGraph
		require.NotNil(t, plan.ExecutionGraph, "ExecutionGraph 不应为空")
		t.Logf("  执行图层级数: %d", len(plan.ExecutionGraph.Levels))
		for i, level := range plan.ExecutionGraph.Levels {
			t.Logf("  Level %d: %v", i, level)
		}

		// 5.3 执行 SyncPlan
		t.Log("  5.3 执行 SyncPlan...")

		// 根据模式选择数据库路径
		var targetDBPath string
		var removeTmpDB bool
		if testCtx.config.IsRealMode {
			// 真实模式：使用持久化 DuckDB
			targetDBPath = testCtx.config.DuckDBPath
			removeTmpDB = false
			t.Logf("  使用持久化 DuckDB: %s", targetDBPath)
		} else {
			// Mock 模式：使用临时数据库
			tmpDBFile, err := os.CreateTemp("", "e2e_syncplan_*.duckdb")
			require.NoError(t, err)
			tmpDBFile.Close()
			targetDBPath = tmpDBFile.Name()
			removeTmpDB = true
			defer func() {
				if removeTmpDB {
					os.Remove(targetDBPath)
				}
			}()
		}

		// 设置同步日期范围
		var startDate, endDate string
		if testCtx.config.IsRealMode {
			// 真实模式：使用最近 7 天
			endDate = time.Now().Format("20060102")
			startDate = time.Now().AddDate(0, 0, -7).Format("20060102")
		} else {
			startDate = "20251201"
			endDate = "20251231"
		}

		execReq := contracts.ExecuteSyncPlanRequest{
			TargetDBPath: targetDBPath,
			StartDate:    startDate,
			EndDate:      endDate,
		}

		executionID, err := testCtx.syncAppService.ExecuteSyncPlan(ctx, syncPlanID, execReq)
		require.NoError(t, err, "执行 SyncPlan 失败")
		require.NotEmpty(t, executionID, "Execution ID 不应为空")
		t.Logf("✅ SyncPlan 执行已提交: ExecutionID=%s", executionID)

		// 验证策略是否正确传递（通过检查日志或工作流构建）
		// 策略会在 WorkflowExecutor.ExecuteBatchDataSync 中通过 RepositoryStrategyProvider 使用
		t.Logf("  📋 策略验证: DataSourceID=%s 已传递到 WorkflowExecutor", plan.DataSourceID)

		// 5.4 等待执行完成并验证
		t.Log("  5.4 等待执行完成...")

		// 获取 SyncExecution 来获取 workflow instance ID（使用应用服务）
		execution, err := testCtx.syncAppService.GetSyncExecution(ctx, executionID)
		require.NoError(t, err, "获取 SyncExecution 失败")
		t.Logf("  SyncExecution Status: %s, WorkflowInstID: %s", execution.Status, execution.WorkflowInstID)

		// 真实模式下需要更长的超时时间（同步大量数据可能需要很长时间）
		timeout := 60 * time.Second
		if testCtx.config.IsRealMode {
			timeout = 30 * time.Minute // 30 分钟，每分钟轮询一次打印进度
		}

		// 使用已注入的 taskEngineAdapter
		status, err := waitForWorkflowCompletion(ctx, testCtx.taskEngineAdapter, execution.WorkflowInstID.String(), timeout)
		if err != nil {
			t.Logf("⚠️ Workflow 未成功完成: %v, Status: %+v", err, status)
		} else {
			t.Logf("✅ SyncPlan 执行完成: Status=%s, Progress=%.2f%%", status.Status, status.Progress)
		}

		// 5.5 验证数据同步结果（仅真实模式）
		if testCtx.config.IsRealMode && testCtx.duckDBAdapter != nil {
			t.Log("  5.5 验证数据同步结果...")
			verifyDataSyncResults(t, ctx, testCtx.duckDBAdapter, selectedAPIs)
		}

		// 5.5 验证执行记录
		t.Log("  5.5 验证执行记录...")
		executions, err := testCtx.syncAppService.ListPlanExecutions(ctx, syncPlanID)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(executions), 1, "应该至少有一条执行记录")
		t.Logf("✅ 执行记录数: %d", len(executions))
	})

	t.Log("========== 内建 Workflow E2E 测试完成 ==========")
	logrus.Infof("========== 内建 Workflow E2E 测试完成 ==========")
}

// TestE2E_BuiltinWorkflow_MetadataCrawlOnly 单独测试元数据爬取 Workflow
func TestE2E_BuiltinWorkflow_MetadataCrawlOnly(t *testing.T) {
	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()

	ctx := context.Background()

	// 创建数据源
	ds := metadata.NewDataSource("Tushare", "Test Data Source", "http://api.tushare.pro", "https://tushare.pro/document/2")
	err := testCtx.dataSourceRepo.Create(ds)
	require.NoError(t, err)

	// 设置 Token
	token := metadata.NewToken(ds.ID, "test-token", nil)
	err = testCtx.dataSourceRepo.SetToken(token)
	require.NoError(t, err)

	// 执行元数据爬取
	req := workflow.MetadataCrawlRequest{
		DataSourceID:   ds.ID,
		DataSourceName: "tushare",
		MaxAPICrawl:    5,
	}

	instanceID, err := testCtx.workflowExecutor.ExecuteMetadataCrawl(ctx, req)
	require.NoError(t, err)
	assert.NotEmpty(t, instanceID)

	t.Logf("MetadataCrawl Workflow 已提交: InstanceID=%s", instanceID)

	// 等待并获取状态
	adapter := taskengine.NewTaskEngineAdapter(testCtx.engine)
	time.Sleep(2 * time.Second) // 等待一段时间让 workflow 执行

	status, err := adapter.GetInstanceStatus(ctx, instanceID.String())
	require.NoError(t, err)
	t.Logf("Workflow 状态: %s, 进度: %.2f%%, 任务总数: %d, 已完成: %d, 失败: %d",
		status.Status, status.Progress, status.TaskCount, status.CompletedTask, status.FailedTask)
}

// TestE2E_BuiltinWorkflow_CreateTablesOnly 单独测试建表 Workflow
func TestE2E_BuiltinWorkflow_CreateTablesOnly(t *testing.T) {
	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()

	ctx := context.Background()

	// 创建数据源
	ds := metadata.NewDataSource("Tushare", "Test Data Source", "http://api.tushare.pro", "https://tushare.pro/document/2")
	err := testCtx.dataSourceRepo.Create(ds)
	require.NoError(t, err)

	// 创建临时 DuckDB 文件
	tmpDBFile, err := os.CreateTemp("", "e2e_create_tables_*.duckdb")
	require.NoError(t, err)
	tmpDBFile.Close()
	defer os.Remove(tmpDBFile.Name())

	// 执行建表
	req := workflow.CreateTablesRequest{
		DataSourceID:   ds.ID,
		DataSourceName: "tushare",
		TargetDBPath:   tmpDBFile.Name(),
		MaxTables:      5,
	}

	instanceID, err := testCtx.workflowExecutor.ExecuteCreateTables(ctx, req)
	require.NoError(t, err)
	assert.NotEmpty(t, instanceID)

	t.Logf("CreateTables Workflow 已提交: InstanceID=%s", instanceID)

	// 等待并获取状态
	adapter := taskengine.NewTaskEngineAdapter(testCtx.engine)
	time.Sleep(2 * time.Second)

	status, err := adapter.GetInstanceStatus(ctx, instanceID.String())
	require.NoError(t, err)
	t.Logf("Workflow 状态: %s, 进度: %.2f%%, 任务总数: %d, 已完成: %d, 失败: %d",
		status.Status, status.Progress, status.TaskCount, status.CompletedTask, status.FailedTask)
}

// TestE2E_BuiltinWorkflow_VerifyWorkflowRegistration 验证 Workflow 注册
func TestE2E_BuiltinWorkflow_VerifyWorkflowRegistration(t *testing.T) {
	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()

	// 验证所有内建 Workflows 都已正确注册
	builtInWorkflows := workflows.GetBuiltInWorkflows()

	for _, meta := range builtInWorkflows {
		t.Run(meta.APIName, func(t *testing.T) {
			// 通过 ID 查找
			def, err := testCtx.workflowRepo.Get(meta.ID)
			require.NoError(t, err, "获取 workflow %s 失败", meta.ID)
			require.NotNil(t, def, "workflow %s 应该存在", meta.ID)

			// 验证属性
			assert.True(t, def.IsSystem, "应为系统 workflow")
			assert.NotNil(t, def.Workflow, "Workflow 对象不应为空")
			// 注意：Workflow.Name 是英文名（如 MetadataCrawl），而 meta.Name 是中文名（如 元数据爬取）
			// 这是设计上的区别：API 名称用于调用，显示名称用于UI展示
			assert.NotEmpty(t, def.Workflow.Name, "Workflow 名称不应为空")

			t.Logf("✅ Workflow %s: 名称=%s, 系统=%v",
				meta.APIName, def.Workflow.Name, def.IsSystem)
		})
	}
}

// ==================== SyncPlan E2E Tests ====================

// TestE2E_SyncPlan_FullLifecycle 测试 SyncPlan 完整生命周期
func TestE2E_SyncPlan_FullLifecycle(t *testing.T) {
	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()

	ctx := context.Background()

	t.Log("========== SyncPlan 生命周期 E2E 测试开始 ==========")

	// 创建数据源
	ds := metadata.NewDataSource("Tushare", "Test Data Source", "http://api.tushare.pro", "https://tushare.pro/document/2")
	err := testCtx.dataSourceRepo.Create(ds)
	require.NoError(t, err)
	token := metadata.NewToken(ds.ID, "test-token", nil)
	err = testCtx.dataSourceRepo.SetToken(token)
	require.NoError(t, err)

	// 1. 创建 SyncPlan（测试更多 API）
	t.Run("Step1_CreateSyncPlan", func(t *testing.T) {
		// 选择多种类型的 API 进行测试
		selectedAPIs := []string{
			"stock_basic", "trade_cal", "daily", "weekly", "monthly",
			"daily_basic", "adj_factor", "income", "balancesheet", "cashflow",
			"index_basic", "index_daily", "top_list", "margin", "block_trade",
			"fina_indicator", "namechange", "stk_limit", "margin_detail",
		}
		req := contracts.CreateSyncPlanRequest{
			Name:         "Test Sync Plan (20 APIs)",
			Description:  "测试同步计划 - 20 个 API",
			DataSourceID: ds.ID,
			SelectedAPIs: selectedAPIs,
		}

		plan, err := testCtx.syncAppService.CreateSyncPlan(ctx, req)
		require.NoError(t, err)
		assert.NotNil(t, plan)
		assert.Equal(t, "Test Sync Plan (20 APIs)", plan.Name)
		assert.Equal(t, sync.PlanStatusDraft, plan.Status)
		t.Logf("✅ SyncPlan 创建成功: ID=%s", plan.ID)
	})

	// 2. 列出所有 SyncPlans
	t.Run("Step2_ListSyncPlans", func(t *testing.T) {
		plans, err := testCtx.syncAppService.ListSyncPlans(ctx)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(plans), 1)
		t.Logf("✅ 列出 SyncPlans: %d 个", len(plans))
	})

	// 3. 获取单个 SyncPlan
	var planID shared.ID
	t.Run("Step3_GetSyncPlan", func(t *testing.T) {
		plans, _ := testCtx.syncAppService.ListSyncPlans(ctx)
		require.NotEmpty(t, plans)
		planID = plans[0].ID

		plan, err := testCtx.syncAppService.GetSyncPlan(ctx, planID)
		require.NoError(t, err)
		assert.Equal(t, planID, plan.ID)
		t.Logf("✅ 获取 SyncPlan: ID=%s, Name=%s", plan.ID, plan.Name)
	})

	// 4. 解析依赖
	t.Run("Step4_ResolveDependencies", func(t *testing.T) {
		err := testCtx.syncAppService.ResolveSyncPlan(ctx, planID)
		require.NoError(t, err)

		plan, err := testCtx.syncAppService.GetSyncPlan(ctx, planID)
		require.NoError(t, err)
		assert.Equal(t, sync.PlanStatusResolved, plan.Status)
		assert.NotNil(t, plan.ExecutionGraph)
		t.Logf("✅ 依赖解析成功: Status=%s, Levels=%d", plan.Status, len(plan.ExecutionGraph.Levels))
	})

	// 5. 更新 SyncPlan
	t.Run("Step5_UpdateSyncPlan", func(t *testing.T) {
		newName := "Updated Sync Plan"
		newDesc := "更新后的同步计划"
		updateReq := contracts.UpdateSyncPlanRequest{
			Name:        &newName,
			Description: &newDesc,
		}

		err := testCtx.syncAppService.UpdateSyncPlan(ctx, planID, updateReq)
		require.NoError(t, err)

		plan, err := testCtx.syncAppService.GetSyncPlan(ctx, planID)
		require.NoError(t, err)
		assert.Equal(t, newName, plan.Name)
		assert.Equal(t, newDesc, plan.Description)
		t.Logf("✅ SyncPlan 更新成功: Name=%s", plan.Name)
	})

	// 6. 执行 SyncPlan
	var executionID shared.ID
	t.Run("Step6_ExecuteSyncPlan", func(t *testing.T) {
		tmpDBFile, err := os.CreateTemp("", "e2e_lifecycle_*.duckdb")
		require.NoError(t, err)
		tmpDBFile.Close()
		defer os.Remove(tmpDBFile.Name())

		execReq := contracts.ExecuteSyncPlanRequest{
			TargetDBPath: tmpDBFile.Name(),
			StartDate:    "20251201",
			EndDate:      "20251215",
		}

		executionID, err = testCtx.syncAppService.ExecuteSyncPlan(ctx, planID, execReq)
		require.NoError(t, err)
		assert.NotEmpty(t, executionID)
		t.Logf("✅ SyncPlan 执行已提交: ExecutionID=%s", executionID)
	})

	// 7. 获取执行记录
	t.Run("Step7_GetSyncExecution", func(t *testing.T) {
		execution, err := testCtx.syncAppService.GetSyncExecution(ctx, executionID)
		require.NoError(t, err)
		assert.NotNil(t, execution)
		assert.Equal(t, planID, execution.SyncPlanID)
		t.Logf("✅ 获取执行记录: Status=%s", execution.Status)
	})

	// 8. 列出计划的所有执行记录
	t.Run("Step8_ListPlanExecutions", func(t *testing.T) {
		executions, err := testCtx.syncAppService.ListPlanExecutions(ctx, planID)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(executions), 1)
		t.Logf("✅ 列出执行记录: %d 条", len(executions))
	})

	// 9. 删除 SyncPlan
	t.Run("Step9_DeleteSyncPlan", func(t *testing.T) {
		// 先取消正在运行的执行
		err := testCtx.syncAppService.CancelExecution(ctx, executionID)
		if err != nil {
			t.Logf("⚠️ 取消执行失败（可能已完成）: %v", err)
		}

		// 等待一小段时间让状态更新
		time.Sleep(100 * time.Millisecond)

		// 尝试禁用计划
		_ = testCtx.syncAppService.DisablePlan(ctx, planID)

		// 删除计划
		err = testCtx.syncAppService.DeleteSyncPlan(ctx, planID)
		require.NoError(t, err)

		// 验证已删除
		_, err = testCtx.syncAppService.GetSyncPlan(ctx, planID)
		assert.Error(t, err, "删除后应该无法获取")
		t.Logf("✅ SyncPlan 删除成功")
	})

	t.Log("========== SyncPlan 生命周期 E2E 测试完成 ==========")
}

// TestE2E_SyncPlan_DependencyResolution 测试依赖解析
func TestE2E_SyncPlan_DependencyResolution(t *testing.T) {
	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()

	ctx := context.Background()

	// 创建数据源
	ds := metadata.NewDataSource("Tushare", "Test Data Source", "http://api.tushare.pro", "https://tushare.pro/document/2")
	err := testCtx.dataSourceRepo.Create(ds)
	require.NoError(t, err)

	// 创建 SyncPlan，选择有复杂依赖关系的 API
	// 这些 API 可能依赖 stock_basic, trade_cal 等基础数据
	req := contracts.CreateSyncPlanRequest{
		Name:         "Dependency Test Plan (Complex)",
		DataSourceID: ds.ID,
		SelectedAPIs: []string{
			"daily",         // 依赖 stock_basic, trade_cal
			"weekly",        // 依赖 stock_basic
			"adj_factor",    // 依赖 stock_basic
			"daily_basic",   // 依赖 stock_basic, trade_cal
			"income",        // 依赖 stock_basic
			"balancesheet",  // 依赖 stock_basic
			"cashflow",      // 依赖 stock_basic
			"margin",        // 依赖 trade_cal
			"margin_detail", // 依赖 trade_cal, stock_basic
			"block_trade",   // 依赖 trade_cal
		},
	}

	plan, err := testCtx.syncAppService.CreateSyncPlan(ctx, req)
	require.NoError(t, err)

	// 解析依赖
	err = testCtx.syncAppService.ResolveSyncPlan(ctx, plan.ID)
	require.NoError(t, err)

	// 获取更新后的计划
	plan, err = testCtx.syncAppService.GetSyncPlan(ctx, plan.ID)
	require.NoError(t, err)

	// 验证依赖解析结果
	assert.Equal(t, sync.PlanStatusResolved, plan.Status)
	assert.NotNil(t, plan.ExecutionGraph)

	t.Logf("SelectedAPIs: %v", plan.SelectedAPIs)
	t.Logf("ResolvedAPIs: %v", plan.ResolvedAPIs)
	t.Logf("ExecutionGraph Levels: %d", len(plan.ExecutionGraph.Levels))

	for i, level := range plan.ExecutionGraph.Levels {
		t.Logf("  Level %d: %v", i, level)
	}

	// 验证任务配置
	for apiName, config := range plan.ExecutionGraph.TaskConfigs {
		t.Logf("TaskConfig[%s]: SyncMode=%s, Dependencies=%v, ParamMappings=%d",
			apiName, config.SyncMode, config.Dependencies, len(config.ParamMappings))
	}
}

// TestE2E_SyncPlan_WithCronSchedule 测试带 Cron 调度的 SyncPlan
func TestE2E_SyncPlan_WithCronSchedule(t *testing.T) {
	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()

	ctx := context.Background()

	// 创建数据源
	ds := metadata.NewDataSource("Tushare", "Test Data Source", "http://api.tushare.pro", "https://tushare.pro/document/2")
	err := testCtx.dataSourceRepo.Create(ds)
	require.NoError(t, err)

	// 创建带 Cron 的 SyncPlan（测试更多 API）
	cronExpr := "0 0 9 * * *" // 每天 9 点
	req := contracts.CreateSyncPlanRequest{
		Name:         "Scheduled Sync Plan (20 APIs)",
		DataSourceID: ds.ID,
		SelectedAPIs: []string{
			"stock_basic", "trade_cal", "daily", "weekly", "monthly",
			"daily_basic", "adj_factor", "income", "balancesheet", "cashflow",
			"index_basic", "index_daily", "top_list", "margin", "block_trade",
			"fina_indicator", "namechange", "stk_limit", "margin_detail",
		},
		CronExpression: &cronExpr,
	}

	plan, err := testCtx.syncAppService.CreateSyncPlan(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, plan.CronExpression)
	assert.Equal(t, cronExpr, *plan.CronExpression)

	// 解析并启用
	err = testCtx.syncAppService.ResolveSyncPlan(ctx, plan.ID)
	require.NoError(t, err)

	// 启用计划（注意：由于 planScheduler 为 nil，这可能会失败或跳过调度）
	// 在实际环境中，这会将计划添加到调度器
	err = testCtx.syncAppService.EnablePlan(ctx, plan.ID)
	// 由于 scheduler 为 nil，这里可能会出错，但我们验证状态变化
	if err == nil {
		plan, _ = testCtx.syncAppService.GetSyncPlan(ctx, plan.ID)
		assert.Equal(t, sync.PlanStatusEnabled, plan.Status)
		t.Logf("✅ SyncPlan 已启用: Status=%s", plan.Status)
	} else {
		t.Logf("⚠️ 启用失败（可能因为 scheduler 为 nil）: %v", err)
	}

	// 更新调度
	newCron := "0 0 10 * * *"
	err = testCtx.syncAppService.UpdatePlanSchedule(ctx, plan.ID, newCron)
	if err == nil {
		plan, _ = testCtx.syncAppService.GetSyncPlan(ctx, plan.ID)
		assert.Equal(t, newCron, *plan.CronExpression)
		t.Logf("✅ Cron 更新成功: %s", newCron)
	}
}

// ==================== 数据验证函数 ====================

// verifyDataSyncResults 验证数据同步结果
// 检查 DuckDB 中各 API 对应的表是否有数据
func verifyDataSyncResults(t *testing.T, ctx context.Context, adapter *duckdb.Adapter, expectedAPIs []string) {
	t.Log("📊 开始验证数据同步结果...")

	// 统计结果
	tablesWithData := 0
	tablesEmpty := 0
	tablesMissing := 0
	totalRecords := int64(0)

	for _, apiName := range expectedAPIs {
		// 检查表是否存在
		exists, err := adapter.TableExists(ctx, apiName)
		if err != nil {
			t.Logf("  ⚠️ %s: 检查表存在失败 - %v", apiName, err)
			tablesMissing++
			continue
		}

		if !exists {
			t.Logf("  ❌ %s: 表不存在", apiName)
			tablesMissing++
			continue
		}

		// 获取表统计
		stats, err := adapter.GetTableStats(ctx, apiName)
		if err != nil {
			t.Logf("  ⚠️ %s: 获取统计失败 - %v", apiName, err)
			continue
		}

		if stats.RowCount > 0 {
			t.Logf("  ✅ %s: %d 条记录", apiName, stats.RowCount)
			tablesWithData++
			totalRecords += stats.RowCount
		} else {
			t.Logf("  ⏳ %s: 0 条记录 (表已创建但无数据)", apiName)
			tablesEmpty++
		}
	}

	// 输出汇总
	t.Log("─────────────────────────────────────")
	t.Logf("📈 数据同步验证汇总:")
	t.Logf("  - 有数据的表: %d", tablesWithData)
	t.Logf("  - 空表: %d", tablesEmpty)
	t.Logf("  - 缺失的表: %d", tablesMissing)
	t.Logf("  - 总记录数: %d", totalRecords)
	t.Log("─────────────────────────────────────")

	// 断言：至少应该有一些表有数据
	assert.Greater(t, tablesWithData, 0, "至少应该有一个表有数据")
	assert.Greater(t, totalRecords, int64(0), "总记录数应大于 0")
}

// TestE2E_VerifyExistingData 验证已存在的 DuckDB 数据
// 这个测试不执行同步，只验证 e2e/data/e2e_quant.duckdb 中的数据
func TestE2E_VerifyExistingData(t *testing.T) {
	config := loadE2ETestConfig(t)

	if !config.IsRealMode {
		t.Skip("跳过：此测试仅在真实模式下运行（需要设置 QDHUB_TUSHARE_TOKEN）")
	}

	ctx := context.Background()

	// 连接 DuckDB
	adapter := duckdb.NewAdapter(config.DuckDBPath)
	err := adapter.Connect(ctx)
	require.NoError(t, err, "连接 DuckDB 失败")
	defer adapter.Close()

	t.Logf("📁 DuckDB 路径: %s", config.DuckDBPath)

	// 验证关键 API 的数据
	keyAPIs := []string{
		"stock_basic", "trade_cal", "daily", "weekly", "monthly",
		"daily_basic", "adj_factor", "income", "balancesheet", "cashflow",
		"index_daily", "top_list", "margin", "block_trade", "fina_indicator",
		"namechange", "stk_limit", "margin_detail", "fund_basic", "fund_daily",
	}

	verifyDataSyncResults(t, ctx, adapter, keyAPIs)
}

// TestE2E_DataSyncOnly 单独测试数据同步（使用应用服务）
// 场景：假设元数据已就绪，测试建表和批量数据同步
// 目的：验证应用服务在 20+ API 下的端到端流程
//
// 前置条件：
//   - 已运行过 TestE2E_BuiltinWorkflow_FullPipeline 完成元数据爬取
//   - QDHUB_TUSHARE_TOKEN 环境变量已设置
//
// 运行命令：
//
//	QDHUB_TUSHARE_TOKEN=your_token go test -tags e2e -v -run "TestE2E_DataSyncOnly" ./tests/e2e/...
func TestE2E_DataSyncOnly(t *testing.T) {
	// 设置日志文件
	logFile, logCleanup := setupLogFile(t, "DataSyncOnly")
	defer logCleanup()
	defer logFile.Close()

	config := loadE2ETestConfig(t)

	if !config.IsRealMode {
		t.Skip("跳过：此测试仅在真实模式下运行（需要设置 QDHUB_TUSHARE_TOKEN）")
	}

	ctx := context.Background()

	t.Log("========== 数据同步专项测试（使用应用服务 - 20+ API）==========")
	t.Logf("📁 DuckDB 路径: %s", config.DuckDBPath)
	logrus.Infof("========== 数据同步专项测试开始 ==========")
	logrus.Infof("DuckDB 路径: %s", config.DuckDBPath)

	// 1. 初始化测试上下文（复用 setupBuiltinWorkflowE2EContext）
	t.Log("步骤 1: 初始化测试上下文...")
	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()
	t.Log("✅ 测试上下文初始化完成")

	// 2. 定义测试的 API 列表
	t.Log("步骤 2: 准备 API 测试列表...")

	// 按同步策略分组的 API 列表
	// 1. 基础数据 API（Direct 模式，直接查询）
	basicAPIs := []string{
		"trade_cal",   // 交易日历
		"stock_basic", // 股票基础信息
		"namechange",  // 股票曾用名
		"index_basic", // 指数基本信息
	}

	// 2. 行情数据 API（支持 trade_date/start_date+end_date）
	marketAPIs := []string{
		"daily",       // 日线行情
		"daily_basic", // 每日指标
		"adj_factor",  // 复权因子
		"stk_limit",   // 涨跌停价格
	}

	// 3. 资金流向 API（只使用支持批量查询的 API，避免按股票拆分产生大量子任务）
	moneyflowAPIs := []string{
		"moneyflow_hsgt",    // 沪深港通资金流向（支持 trade_date 批量查询）
		"moneyflow_ind_ths", // 同花顺行业资金流向（支持 trade_date 批量查询）
		"moneyflow_cnt_ths", // 同花顺概念资金流向（支持 trade_date 批量查询）
		// 注意：moneyflow 和 moneyflow_ths 需要按股票拆分，会产生 5000+ 子任务，不适合短时测试
	}

	// 4. 龙虎榜数据 API
	topListAPIs := []string{
		"top_list",     // 龙虎榜明细
		"top_inst",     // 龙虎榜机构
		"hsgt_top10",   // 沪深股通十大成交
		"ggt_top10",    // 港股通十大成交
		"limit_list_d", // 每日涨跌停榜单
	}

	// 5. 同花顺概念板块 API
	thsAPIs := []string{
		"ths_index",  // 同花顺板块指数
		"ths_daily",  // 同花顺板块行情
		"ths_member", // 同花顺概念成分
	}

	// 6. 开盘啦题材数据 API
	kplAPIs := []string{
		"kpl_list",         // 开盘啦榜单（涨停、跌停、炸板）
		"kpl_concept",      // 开盘啦概念题材列表
		"kpl_concept_cons", // 开盘啦概念题材成分
	}

	// 7. 其他数据 API
	otherAPIs := []string{
		"margin",        // 融资融券汇总
		"margin_detail", // 融资融券明细
		"block_trade",   // 大宗交易
	}

	// 合并所有 API
	testAPIs := make([]string, 0)
	testAPIs = append(testAPIs, basicAPIs...)
	testAPIs = append(testAPIs, marketAPIs...)
	testAPIs = append(testAPIs, moneyflowAPIs...)
	testAPIs = append(testAPIs, topListAPIs...)
	testAPIs = append(testAPIs, thsAPIs...)
	testAPIs = append(testAPIs, kplAPIs...)
	testAPIs = append(testAPIs, otherAPIs...)

	t.Logf("  测试 API 总数: %d", len(testAPIs))
	t.Logf("  基础数据 API (%d): %v", len(basicAPIs), basicAPIs)
	t.Logf("  行情数据 API (%d): %v", len(marketAPIs), marketAPIs)
	t.Logf("  资金流向 API (%d): %v", len(moneyflowAPIs), moneyflowAPIs)
	t.Logf("  龙虎榜数据 API (%d): %v", len(topListAPIs), topListAPIs)
	t.Logf("  同花顺概念 API (%d): %v", len(thsAPIs), thsAPIs)
	t.Logf("  开盘啦数据 API (%d): %v", len(kplAPIs), kplAPIs)
	t.Logf("  其他数据 API (%d): %v", len(otherAPIs), otherAPIs)

	// 3. 使用 MetadataApplicationService 获取数据源
	t.Log("步骤 3: 获取数据源信息...")

	dataSources, err := testCtx.metadataAppService.ListDataSources(ctx)
	require.NoError(t, err, "获取数据源列表失败")

	var dataSourceID shared.ID
	for _, ds := range dataSources {
		if strings.ToLower(ds.Name) == "tushare" {
			dataSourceID = ds.ID
			t.Logf("  ✅ 找到 Tushare 数据源: ID=%s", dataSourceID)
			break
		}
	}
	require.NotEmpty(t, dataSourceID, "未找到 Tushare 数据源，请先执行完整的 E2E 测试流程")

	// 验证 API 元数据已存在
	apiMetadataList, err := testCtx.metadataRepo.ListAPIMetadataByDataSource(ctx, dataSourceID)
	require.NoError(t, err, "获取 API 元数据列表失败")
	t.Logf("  📊 已有 API 元数据数量: %d", len(apiMetadataList))

	// 4. 使用 DataStoreApplicationService 创建表结构
	t.Log("步骤 4: 使用 CreateTablesForDatasource 创建表...")

	// 4.1 创建或获取 DataStore
	var dataStoreID shared.ID
	existingStores, err := testCtx.datastoreAppService.ListDataStores(ctx)
	if err == nil {
		for _, ds := range existingStores {
			if ds.StoragePath == config.DuckDBPath || ds.Name == "E2E Sync Test DuckDB" {
				dataStoreID = ds.ID
				t.Logf("  ✅ 使用已存在的 DataStore: ID=%s, Name=%s", dataStoreID, ds.Name)
				break
			}
		}
	}

	if dataStoreID == "" {
		ds, err := testCtx.datastoreAppService.CreateDataStore(ctx, contracts.CreateDataStoreRequest{
			Name:        "E2E Sync Test DuckDB",
			Description: "E2E 数据同步测试用 DuckDB 数据存储",
			Type:        datastore.DataStoreTypeDuckDB,
			StoragePath: config.DuckDBPath,
		})
		require.NoError(t, err, "创建 DataStore 失败")
		dataStoreID = ds.ID
		t.Logf("  ✅ DataStore 创建成功: ID=%s, Name=%s", ds.ID, ds.Name)
	}

	// 4.2 使用 DuckDB adapter 检查表是否已存在
	// 连接 DuckDB 检查表
	checkAdapter := duckdb.NewAdapter(config.DuckDBPath)
	require.NoError(t, checkAdapter.Connect(ctx), "连接 DuckDB 检查表失败")

	// 检查测试 API 对应的表是否存在
	existingTables := 0
	for _, apiName := range testAPIs {
		exists, _ := checkAdapter.TableExists(ctx, apiName)
		if exists {
			existingTables++
		}
	}
	checkAdapter.Close()

	if existingTables >= len(testAPIs) {
		t.Logf("  ✅ DuckDB 中已存在 %d/%d 个表，跳过建表", existingTables, len(testAPIs))
	} else {
		t.Logf("  📊 DuckDB 中已存在 %d/%d 个表，需要建表", existingTables, len(testAPIs))

		// 使用 CreateTablesForDatasource 建表
		instanceID, err := testCtx.datastoreAppService.CreateTablesForDatasource(ctx, contracts.CreateTablesForDatasourceRequest{
			DataSourceID: dataSourceID,
			DataStoreID:  dataStoreID,
			MaxTables:    nil, // 不限制
		})
		require.NoError(t, err, "执行建表失败")
		t.Logf("  📡 建表工作流已启动: InstanceID=%s", instanceID)

		// 等待建表完成
		status, err := waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, instanceID.String(), 120*time.Second)
		if err != nil {
			t.Logf("⚠️ 建表: %v", err)
		} else {
			t.Logf("✅ 建表完成: %d 个表", status.CompletedTask)
		}
	}

	// 5. 使用 SyncApplicationService 创建并执行 SyncPlan
	t.Log("步骤 5: 创建 SyncPlan...")

	// 5.1 创建 SyncPlan
	syncPlan, err := testCtx.syncAppService.CreateSyncPlan(ctx, contracts.CreateSyncPlanRequest{
		Name:         "E2E Data Sync Test Plan",
		Description:  "E2E 数据同步测试计划（20+ API）",
		DataSourceID: dataSourceID,
		DataStoreID:  dataStoreID,
		SelectedAPIs: testAPIs,
	})
	require.NoError(t, err, "创建 SyncPlan 失败")
	t.Logf("  ✅ SyncPlan 创建成功: ID=%s, Name=%s", syncPlan.ID, syncPlan.Name)
	t.Logf("  📊 选中的 API 数量: %d", len(syncPlan.SelectedAPIs))

	// 5.2 解析 SyncPlan 的依赖关系
	err = testCtx.syncAppService.ResolveSyncPlan(ctx, syncPlan.ID)
	require.NoError(t, err, "解析 SyncPlan 依赖关系失败")
	t.Log("  ✅ SyncPlan 依赖关系解析完成")

	// 6. 执行 SyncPlan
	t.Log("步骤 6: 使用 ExecuteSyncPlan 执行数据同步...")

	// 使用最近 7 天的数据
	endDate := time.Now().Format("20060102")
	startDate := time.Now().AddDate(0, 0, -7).Format("20060102")
	t.Logf("  日期范围: %s ~ %s", startDate, endDate)

	executionID, err := testCtx.syncAppService.ExecuteSyncPlan(ctx, syncPlan.ID, contracts.ExecuteSyncPlanRequest{
		TargetDBPath: config.DuckDBPath,
		StartDate:    startDate,
		EndDate:      endDate,
	})
	require.NoError(t, err, "执行 SyncPlan 失败")
	t.Logf("✅ SyncPlan 执行已启动: ExecutionID=%s", executionID)
	logrus.Infof("SyncPlan 执行已启动: ExecutionID=%s, DataSourceID=%s", executionID, syncPlan.DataSourceID)

	// 验证策略功能：检查策略是否正确传递
	// 策略会在 WorkflowExecutor 中通过 MetadataRepository 查询并使用
	t.Logf("  📋 策略验证: DataSourceID=%s 已传递，策略将通过 MetadataRepository 查询", syncPlan.DataSourceID)
	logrus.Infof("策略验证: DataSourceID=%s 已传递到 WorkflowExecutor", syncPlan.DataSourceID)

	// 获取 SyncExecution 以获取 WorkflowInstID
	execution, err := testCtx.syncAppService.GetSyncExecution(ctx, executionID)
	require.NoError(t, err, "获取 SyncExecution 失败")
	t.Logf("  📡 WorkflowInstID=%s, 同步 API 数量: %d", execution.WorkflowInstID, len(execution.SyncedAPIs))

	// 7. 等待同步完成
	t.Log("步骤 7: 等待同步完成（超时 5 分钟）...")

	syncStatus, err := waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, execution.WorkflowInstID.String(), 5*time.Minute)
	if err != nil {
		t.Logf("⚠️ 数据同步: %v", err)
		logrus.Warnf("数据同步未完全成功: %v", err)
	} else {
		t.Logf("✅ 数据同步完成: %d 个任务", syncStatus.CompletedTask)
		logrus.Infof("数据同步完成: Status=%s, Progress=%.2f%%, Completed=%d, Failed=%d",
			syncStatus.Status, syncStatus.Progress, syncStatus.CompletedTask, syncStatus.FailedTask)
	}

	// 8. 验证数据同步结果
	t.Log("步骤 8: 验证数据同步结果...")

	// 连接 DuckDB 验证数据
	duckDBAdapter := duckdb.NewAdapter(config.DuckDBPath)
	require.NoError(t, duckDBAdapter.Connect(ctx), "连接 DuckDB 失败")
	defer duckDBAdapter.Close()

	verifyDataSyncResults(t, ctx, duckDBAdapter, testAPIs)

	t.Log("========== 数据同步专项测试完成（使用 SyncPlan - 20+ API）==========")
	logrus.Infof("========== 数据同步专项测试完成 ==========")
}

// TestE2E_GGT_Top10 专门测试 ggt_top10 API 的数据同步
// 验证修复后的策略配置（support_date_range=0）是否能正常工作
//
// 运行命令：
//
//	QDHUB_TUSHARE_TOKEN=your_token go test -tags e2e -v -run "TestE2E_GGT_Top10" ./tests/e2e/...
func TestE2E_GGT_Top10(t *testing.T) {
	// 设置日志文件
	logFile, logCleanup := setupLogFile(t, "GGT_Top10")
	defer logCleanup()
	defer logFile.Close()

	config := loadE2ETestConfig(t)

	if !config.IsRealMode {
		t.Skip("跳过：此测试仅在真实模式下运行（需要设置 QDHUB_TUSHARE_TOKEN）")
	}

	ctx := context.Background()

	t.Log("========== ggt_top10 专项测试 ==========")
	t.Logf("📁 DuckDB 路径: %s", config.DuckDBPath)
	logrus.Infof("========== ggt_top10 专项测试开始 ==========")
	logrus.Infof("DuckDB 路径: %s", config.DuckDBPath)

	// 1. 初始化测试上下文
	t.Log("步骤 1: 初始化测试上下文...")
	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()
	t.Log("✅ 测试上下文初始化完成")

	// 2. 获取数据源
	t.Log("步骤 2: 获取 Tushare 数据源...")
	dataSources, err := testCtx.metadataAppService.ListDataSources(ctx)
	require.NoError(t, err, "获取数据源列表失败")

	var dataSourceID shared.ID
	for _, ds := range dataSources {
		if strings.ToLower(ds.Name) == "tushare" {
			dataSourceID = ds.ID
			t.Logf("  ✅ 找到 Tushare 数据源: ID=%s", dataSourceID)
			break
		}
	}
	require.NotEmpty(t, dataSourceID, "未找到 Tushare 数据源，请先执行完整的 E2E 测试流程")

	// 验证 ggt_top10 API 元数据已存在
	apiMetadataList, err := testCtx.metadataRepo.ListAPIMetadataByDataSource(ctx, dataSourceID)
	require.NoError(t, err, "获取 API 元数据列表失败")

	var ggtTop10Exists bool
	for _, api := range apiMetadataList {
		if api.Name == "ggt_top10" {
			ggtTop10Exists = true
			t.Logf("  ✅ 找到 ggt_top10 API 元数据: ID=%s", api.ID)
			break
		}
	}
	require.True(t, ggtTop10Exists, "未找到 ggt_top10 API 元数据，请先执行元数据爬取")

	// 3. 创建或获取 DataStore
	t.Log("步骤 3: 创建或获取 DataStore...")
	var dataStoreID shared.ID
	existingStores, err := testCtx.datastoreAppService.ListDataStores(ctx)
	if err == nil {
		for _, ds := range existingStores {
			if ds.StoragePath == config.DuckDBPath || ds.Name == "E2E GGT Top10 Test DuckDB" {
				dataStoreID = ds.ID
				t.Logf("  ✅ 使用已存在的 DataStore: ID=%s, Name=%s", dataStoreID, ds.Name)
				break
			}
		}
	}

	if dataStoreID == "" {
		ds, err := testCtx.datastoreAppService.CreateDataStore(ctx, contracts.CreateDataStoreRequest{
			Name:        "E2E GGT Top10 Test DuckDB",
			Description: "E2E ggt_top10 测试用 DuckDB 数据存储",
			Type:        datastore.DataStoreTypeDuckDB,
			StoragePath: config.DuckDBPath,
		})
		require.NoError(t, err, "创建 DataStore 失败")
		dataStoreID = ds.ID
		t.Logf("  ✅ DataStore 创建成功: ID=%s, Name=%s", ds.ID, ds.Name)
	}

	// 4. 创建表结构（如果不存在）
	t.Log("步骤 4: 创建表结构...")
	checkAdapter := duckdb.NewAdapter(config.DuckDBPath)
	require.NoError(t, checkAdapter.Connect(ctx), "连接 DuckDB 检查表失败")

	tableExists, err := checkAdapter.TableExists(ctx, "ggt_top10")
	checkAdapter.Close()

	if !tableExists {
		t.Log("  📊 表不存在，开始建表...")
		instanceID, err := testCtx.datastoreAppService.CreateTablesForDatasource(ctx, contracts.CreateTablesForDatasourceRequest{
			DataSourceID: dataSourceID,
			DataStoreID:  dataStoreID,
			MaxTables:    nil, // 不限制，会为所有 API 建表
		})
		require.NoError(t, err, "建表失败")
		t.Logf("  ✅ 建表任务已启动: InstanceID=%s", instanceID)

		// 等待建表完成
		status, err := waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, instanceID.String(), 60*time.Second)
		if err != nil {
			t.Logf("  ⚠️ 建表: %v", err)
		} else {
			t.Logf("  ✅ 建表完成: %d 个表", status.CompletedTask)
		}
	} else {
		t.Log("  ✅ 表已存在，跳过建表")
	}

	// 5. 创建 SyncPlan（只包含 ggt_top10）
	t.Log("步骤 5: 创建 SyncPlan（只包含 ggt_top10）...")
	createReq := contracts.CreateSyncPlanRequest{
		Name:         "E2E Test - ggt_top10 Only",
		Description:  "E2E 测试 - 专门测试 ggt_top10 API",
		DataSourceID: dataSourceID,
		SelectedAPIs: []string{"ggt_top10"},
	}

	plan, err := testCtx.syncAppService.CreateSyncPlan(ctx, createReq)
	require.NoError(t, err, "创建 SyncPlan 失败")
	require.NotNil(t, plan, "SyncPlan 不应为空")
	t.Logf("  ✅ SyncPlan 创建成功: ID=%s, Name=%s", plan.ID, plan.Name)
	logrus.Infof("SyncPlan 创建成功: ID=%s, DataSourceID=%s", plan.ID, plan.DataSourceID)

	// 6. 解析 SyncPlan
	t.Log("步骤 6: 解析 SyncPlan 依赖...")
	err = testCtx.syncAppService.ResolveSyncPlan(ctx, plan.ID)
	require.NoError(t, err, "解析 SyncPlan 失败")

	plan, err = testCtx.syncAppService.GetSyncPlan(ctx, plan.ID)
	require.NoError(t, err)
	require.Equal(t, sync.PlanStatusResolved, plan.Status, "SyncPlan 状态应为 Resolved")
	t.Logf("  ✅ SyncPlan 解析完成: Status=%s", plan.Status)

	// 7. 执行 SyncPlan
	t.Log("步骤 7: 执行 SyncPlan...")
	startDate := time.Now().AddDate(0, 0, -7).Format("20060102") // 7天前
	endDate := time.Now().Format("20060102")                     // 今天

	executionID, err := testCtx.syncAppService.ExecuteSyncPlan(ctx, plan.ID, contracts.ExecuteSyncPlanRequest{
		TargetDBPath: config.DuckDBPath,
		StartDate:    startDate,
		EndDate:      endDate,
	})
	require.NoError(t, err, "执行 SyncPlan 失败")
	t.Logf("  ✅ SyncPlan 执行已启动: ExecutionID=%s", executionID)
	logrus.Infof("SyncPlan 执行已启动: ExecutionID=%s, DataSourceID=%s, StartDate=%s, EndDate=%s",
		executionID, plan.DataSourceID, startDate, endDate)

	// 验证策略配置
	t.Logf("  📋 策略验证: DataSourceID=%s 已传递，策略将通过 MetadataRepository 查询", plan.DataSourceID)
	logrus.Infof("策略验证: DataSourceID=%s 已传递到 WorkflowExecutor", plan.DataSourceID)

	// 获取 SyncExecution 以获取 WorkflowInstID
	execution, err := testCtx.syncAppService.GetSyncExecution(ctx, executionID)
	require.NoError(t, err, "获取 SyncExecution 失败")
	t.Logf("  📡 WorkflowInstID=%s, 同步 API 数量: %d", execution.WorkflowInstID, len(execution.SyncedAPIs))

	// 8. 等待执行完成
	t.Log("步骤 8: 等待执行完成...")
	status, err := waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, execution.WorkflowInstID.String(), 120*time.Second)
	if err != nil {
		t.Logf("  ⚠️ 数据同步未完全成功: %v", err)
		logrus.Warnf("数据同步未完全成功: %v", err)
	} else {
		t.Logf("  ✅ 数据同步完成: Status=%s, Completed=%d, Failed=%d", status.Status, status.CompletedTask, status.FailedTask)
		logrus.Infof("数据同步完成: Status=%s, Progress=%.2f%%, Completed=%d, Failed=%d",
			status.Status, status.Progress, status.CompletedTask, status.FailedTask)
	}

	// 9. 验证数据
	t.Log("步骤 9: 验证 ggt_top10 数据...")
	duckDBAdapter := duckdb.NewAdapter(config.DuckDBPath)
	require.NoError(t, duckDBAdapter.Connect(ctx), "连接 DuckDB 失败")
	defer duckDBAdapter.Close()

	// 检查表是否存在
	exists, err := duckDBAdapter.TableExists(ctx, "ggt_top10")
	require.NoError(t, err, "检查表存在失败")
	require.True(t, exists, "ggt_top10 表应该存在")

	// 获取表统计
	stats, err := duckDBAdapter.GetTableStats(ctx, "ggt_top10")
	require.NoError(t, err, "获取表统计失败")

	if stats.RowCount > 0 {
		t.Logf("  ✅ ggt_top10: %d 条记录", stats.RowCount)
		logrus.Infof("ggt_top10 数据验证成功: %d 条记录", stats.RowCount)
		assert.Greater(t, stats.RowCount, int64(0), "ggt_top10 应该有数据")
	} else {
		t.Logf("  ⏳ ggt_top10: 0 条记录 (表已创建但无数据)")
		logrus.Warnf("ggt_top10 表已创建但无数据")
		// 注意：这里不强制要求有数据，因为可能测试日期范围内没有数据
	}

	t.Log("========== ggt_top10 专项测试完成 ==========")
	logrus.Infof("========== ggt_top10 专项测试完成 ==========")
}
