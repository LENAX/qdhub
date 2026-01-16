//go:build e2e
// +build e2e

// Package e2e 提供端到端测试
// 支持两种模式：mock模式和真实模式
// 通过环境变量 E2E_MODE 控制：mock（默认）或 real
// 真实模式会启动真实的 HTTP 服务器并通过 HTTP 客户端调用 API
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/impl"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/scheduler"
	httphandler "qdhub/internal/interfaces/http"
)

// ==================== 测试配置 ====================

// E2EConfig E2E测试配置
type E2EConfig struct {
	Mode          string // mock 或 real
	BaseURL       string // API 服务器基础 URL（真实模式需要）
	DBPath        string // 数据库路径
	Timeout       time.Duration
	TushareToken  string // Tushare API Token（从 QDHUB_TUSHARE_TOKEN 环境变量读取）
	EncryptionKey string // 加密密钥（从 QDHUB_ENCRYPTION_KEY 环境变量读取）
}

// getE2EConfig 获取E2E测试配置
func getE2EConfig(t *testing.T) *E2EConfig {
	mode := os.Getenv("E2E_MODE")
	if mode == "" {
		mode = "mock"
	}

	cfg := &E2EConfig{
		Mode:    mode,
		Timeout: 30 * time.Second,
	}

	// 从环境变量读取 Tushare Token
	// 优先使用 QDHUB_TUSHARE_TOKEN，其次 TUSHARE_TOKEN
	cfg.TushareToken = os.Getenv("QDHUB_TUSHARE_TOKEN")
	if cfg.TushareToken == "" {
		cfg.TushareToken = os.Getenv("TUSHARE_TOKEN")
	}
	// 清理 token 中可能的换行符
	cfg.TushareToken = strings.TrimSpace(cfg.TushareToken)

	// 从环境变量读取加密密钥
	cfg.EncryptionKey = os.Getenv("QDHUB_ENCRYPTION_KEY")
	// 清理密钥中可能的换行符
	cfg.EncryptionKey = strings.TrimSpace(cfg.EncryptionKey)

	if mode == "real" {
		// 真实模式：从环境变量获取服务器地址，或使用默认值
		cfg.BaseURL = os.Getenv("QDHUB_API_URL")
		if cfg.BaseURL == "" {
			cfg.BaseURL = "http://127.0.0.1:8080"
		}
		// 确保 URL 不以 / 结尾
		cfg.BaseURL = strings.TrimSuffix(cfg.BaseURL, "/")
	}

	return cfg
}

// ==================== E2E测试上下文 ====================

// E2ETestContext E2E测试上下文
type E2ETestContext struct {
	Config            *E2EConfig
	DB                *persistence.DB
	Router            *gin.Engine
	Server            *httphandler.Server
	HTTPClient        *http.Client
	BaseURL           string
	TaskEngineAdapter *e2eTaskEngineAdapter
	JobScheduler      *e2eJobScheduler
	cleanup           func()
}

// setupE2EFullTestContext 设置E2E测试环境（支持Mock和Real模式）
func setupE2EFullTestContext(t *testing.T) *E2ETestContext {
	cfg := getE2EConfig(t)

	ctx := &E2ETestContext{
		Config: cfg,
	}

	// 创建临时数据库文件
	tmpfile, err := os.CreateTemp("", "e2e_test_*.db")
	require.NoError(t, err)
	tmpfile.Close()
	dsn := tmpfile.Name()
	ctx.Config.DBPath = dsn

	// 创建 SQLite 数据库
	db, err := persistence.NewDB(dsn)
	require.NoError(t, err)
	ctx.DB = db

	// 读取并执行迁移
	migrationSQL, err := os.ReadFile("../../migrations/001_init_schema.up.sql")
	if err != nil {
		db.Close()
		os.Remove(dsn)
		t.Fatalf("读取迁移文件失败: %v", err)
	}

	_, err = db.Exec(string(migrationSQL))
	if err != nil {
		db.Close()
		os.Remove(dsn)
		t.Fatalf("执行迁移失败: %v", err)
	}

	// 创建 repositories
	dataSourceRepo := repository.NewDataSourceRepository(db)
	dsRepo := repository.NewQuantDataStoreRepository(db)
	mappingRuleRepo := repository.NewDataTypeMappingRuleRepository(db)
	syncJobRepo := repository.NewSyncJobRepository(db)
	workflowRepo, err := repository.NewWorkflowDefinitionRepository(db)
	require.NoError(t, err)

	// 创建 adapters
	quantDBAdapter := &e2eQuantDBAdapter{}
	parserFactory := newE2EDocumentParserFactory()
	taskEngineAdapter := newE2ETaskEngineAdapter()
	ctx.TaskEngineAdapter = taskEngineAdapter
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	jobScheduler := newE2EJobScheduler()
	ctx.JobScheduler = jobScheduler

	// 创建 application services
	metadataSvc := impl.NewMetadataApplicationService(dataSourceRepo, parserFactory)
	dataStoreSvc := impl.NewDataStoreApplicationService(dsRepo, mappingRuleRepo, dataSourceRepo, quantDBAdapter)
	syncSvc := impl.NewSyncApplicationService(syncJobRepo, workflowRepo, taskEngineAdapter, cronCalculator, jobScheduler)
	workflowSvc := impl.NewWorkflowApplicationService(workflowRepo, taskEngineAdapter)

	if cfg.Mode == "mock" {
		// Mock 模式：使用 httptest
		gin.SetMode(gin.TestMode)
		config := httphandler.ServerConfig{
			Host: "127.0.0.1",
			Port: 0,
			Mode: gin.TestMode,
		}
		server := httphandler.NewServer(config, metadataSvc, dataStoreSvc, syncSvc, workflowSvc)
		ctx.Server = server
		ctx.Router = server.Engine()
		ctx.BaseURL = "" // Mock 模式不需要 BaseURL
		ctx.HTTPClient = &http.Client{
			Timeout: cfg.Timeout,
		}
	} else {
		// Real 模式：启动真实的 HTTP 服务器
		gin.SetMode(gin.DebugMode)

		// 查找可用端口
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)
		port := listener.Addr().(*net.TCPAddr).Port
		listener.Close()

		config := httphandler.ServerConfig{
			Host:         "127.0.0.1",
			Port:         port,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
			Mode:         gin.DebugMode,
		}
		server := httphandler.NewServer(config, metadataSvc, dataStoreSvc, syncSvc, workflowSvc)
		ctx.Server = server
		ctx.Router = server.Engine()
		ctx.BaseURL = fmt.Sprintf("http://127.0.0.1:%d", port)
		ctx.HTTPClient = &http.Client{
			Timeout: cfg.Timeout,
		}

		// 在 goroutine 中启动服务器
		errChan := make(chan error, 1)
		go func() {
			if err := server.Start(); err != nil {
				errChan <- err
			}
		}()

		// 等待服务器启动
		time.Sleep(100 * time.Millisecond)
		select {
		case err := <-errChan:
			t.Fatalf("服务器启动失败: %v", err)
		default:
			// 服务器启动成功
		}

		// 验证服务器是否可访问
		for i := 0; i < 10; i++ {
			resp, err := ctx.HTTPClient.Get(ctx.BaseURL + "/health")
			if err == nil && resp.StatusCode == http.StatusOK {
				resp.Body.Close()
				break
			}
			if i < 9 {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	ctx.cleanup = func() {
		if ctx.Server != nil && cfg.Mode == "real" {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ctx.Server.Shutdown(shutdownCtx)
		}
		db.Close()
		os.Remove(dsn)
	}

	return ctx
}

// ==================== HTTP 请求辅助函数 ====================

// doRequest 执行 HTTP 请求
func (ctx *E2ETestContext) doRequest(method, path string, body interface{}) (*http.Response, error) {
	var reqBody *bytes.Buffer
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reqBody = bytes.NewBuffer(jsonBody)
	} else {
		reqBody = bytes.NewBuffer(nil)
	}

	if ctx.Config.Mode == "mock" {
		// Mock 模式：使用 httptest
		req, err := http.NewRequest(method, path, reqBody)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		ctx.Router.ServeHTTP(w, req)
		// ResponseRecorder.Result() 返回 *http.Response
		return w.Result(), nil
	} else {
		// Real 模式：使用 HTTP 客户端
		fullURL := ctx.BaseURL + path
		req, err := http.NewRequest(method, fullURL, reqBody)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		return ctx.HTTPClient.Do(req)
	}
}

// parseFullResponse 解析响应
func parseFullResponse(resp *http.Response) (map[string]interface{}, error) {
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应体失败: %w", err)
	}
	var response map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &response); err != nil {
		return nil, fmt.Errorf("解析响应失败: %w, body: %s", err, string(bodyBytes))
	}
	return response, nil
}

// getFullResponseData 获取响应中的 data 字段
func getFullResponseData(resp *http.Response) (map[string]interface{}, error) {
	response, err := parseFullResponse(resp)
	if err != nil {
		return nil, err
	}
	if data, ok := response["data"].(map[string]interface{}); ok {
		return data, nil
	}
	return nil, fmt.Errorf("响应中没有 data 字段")
}

// getFullResponseDataList 获取响应中的 data 数组
func getFullResponseDataList(resp *http.Response) ([]interface{}, error) {
	response, err := parseFullResponse(resp)
	if err != nil {
		return nil, err
	}
	if data, ok := response["data"].([]interface{}); ok {
		return data, nil
	}
	return nil, fmt.Errorf("响应中没有 data 数组")
}

// getFullStringField 从 map 中获取字符串字段（支持多种命名格式）
func getFullStringField(m map[string]interface{}, key string) string {
	if m == nil {
		return ""
	}
	// 尝试原始 key
	if v, ok := m[key].(string); ok {
		return v
	}
	// 尝试首字母大写
	capitalizedKey := strings.ToUpper(key[:1]) + key[1:]
	if v, ok := m[capitalizedKey].(string); ok {
		return v
	}
	// 尝试全大写（如 ID）
	upperKey := strings.ToUpper(key)
	if v, ok := m[upperKey].(string); ok {
		return v
	}
	return ""
}

// readResponseBody 读取响应体内容（用于错误消息）
// 注意：此函数会读取并关闭响应体，调用后不能再使用响应体
func readResponseBody(resp *http.Response) string {
	if resp.Body == nil {
		return ""
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Sprintf("读取失败: %v", err)
	}
	return string(bodyBytes)
}

// ==================== E2E测试用例 ====================

// TestE2E_DataSourceWorkflow 测试数据源完整工作流
func TestE2E_DataSourceWorkflow(t *testing.T) {
	ctx := setupE2EFullTestContext(t)
	defer ctx.cleanup()

	t.Logf("测试模式: %s", ctx.Config.Mode)
	if ctx.Config.Mode == "real" {
		t.Logf("API 地址: %s", ctx.BaseURL)
	}
	if ctx.Config.TushareToken != "" {
		t.Logf("Tushare Token: 已设置（长度: %d）", len(ctx.Config.TushareToken))
	}
	if ctx.Config.EncryptionKey != "" {
		t.Logf("Encryption Key: 已设置（长度: %d）", len(ctx.Config.EncryptionKey))
	}

	// Step 1: 创建数据源
	createReq := map[string]string{
		"name":        "Tushare",
		"description": "Tushare Pro Data Source",
		"base_url":    "http://api.tushare.pro",
		"doc_url":     "https://tushare.pro/document/2",
	}
	resp, err := ctx.doRequest("POST", "/api/v1/datasources", createReq)
	require.NoError(t, err)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("期望状态码 %d，实际 %d，响应体: %s", http.StatusCreated, resp.StatusCode, readResponseBody(resp))
	}

	data, err := getFullResponseData(resp)
	require.NoError(t, err)
	require.NotNil(t, data)
	dataSourceID := getFullStringField(data, "id")
	require.NotEmpty(t, dataSourceID, "响应数据: %+v", data)

	// Step 2: 获取数据源验证创建
	resp, err = ctx.doRequest("GET", "/api/v1/datasources/"+dataSourceID, nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	data, err = getFullResponseData(resp)
	require.NoError(t, err)
	assert.Equal(t, "Tushare", getFullStringField(data, "name"))

	// Step 3: 更新数据源
	updateReq := map[string]string{
		"description": "Updated description",
	}
	resp, err = ctx.doRequest("PUT", "/api/v1/datasources/"+dataSourceID, updateReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Step 4: 列出所有数据源
	resp, err = ctx.doRequest("GET", "/api/v1/datasources", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	sources, err := getFullResponseDataList(resp)
	require.NoError(t, err)
	assert.Len(t, sources, 1)

	// Step 5: 设置 token
	tokenReq := map[string]string{
		"token": "test-token-12345",
	}
	resp, err = ctx.doRequest("POST", "/api/v1/datasources/"+dataSourceID+"/token", tokenReq)
	require.NoError(t, err)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("期望状态码 %d，实际 %d，响应体: %s", http.StatusOK, resp.StatusCode, readResponseBody(resp))
	}

	// Step 6: 验证 token 已设置
	resp, err = ctx.doRequest("GET", "/api/v1/datasources/"+dataSourceID+"/token", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Step 7: 删除 token
	resp, err = ctx.doRequest("DELETE", "/api/v1/datasources/"+dataSourceID+"/token", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// Step 8: 删除数据源
	resp, err = ctx.doRequest("DELETE", "/api/v1/datasources/"+dataSourceID, nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// Step 9: 验证删除
	resp, err = ctx.doRequest("GET", "/api/v1/datasources/"+dataSourceID, nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// TestE2E_DataStoreWorkflow 测试数据存储完整工作流
func TestE2E_DataStoreWorkflow(t *testing.T) {
	ctx := setupE2EFullTestContext(t)
	defer ctx.cleanup()

	// Step 1: 创建数据存储
	createReq := map[string]string{
		"name":         "Test DuckDB",
		"type":         "duckdb",
		"storage_path": "/tmp/test.duckdb",
	}
	resp, err := ctx.doRequest("POST", "/api/v1/datastores", createReq)
	require.NoError(t, err)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("期望状态码 %d，实际 %d，响应体: %s", http.StatusCreated, resp.StatusCode, readResponseBody(resp))
	}

	data, err := getFullResponseData(resp)
	require.NoError(t, err)
	require.NotNil(t, data)
	dataStoreID := getFullStringField(data, "id")
	require.NotEmpty(t, dataStoreID)

	// Step 2: 获取数据存储
	resp, err = ctx.doRequest("GET", "/api/v1/datastores/"+dataStoreID, nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	data, err = getFullResponseData(resp)
	require.NoError(t, err)
	assert.Equal(t, "Test DuckDB", getFullStringField(data, "name"))
	assert.Equal(t, "duckdb", getFullStringField(data, "type"))

	// Step 3: 测试连接
	resp, err = ctx.doRequest("POST", "/api/v1/datastores/"+dataStoreID+"/test", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Step 4: 列出数据存储
	resp, err = ctx.doRequest("GET", "/api/v1/datastores", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	stores, err := getFullResponseDataList(resp)
	require.NoError(t, err)
	assert.Len(t, stores, 1)

	// Step 5: 删除数据存储
	resp, err = ctx.doRequest("DELETE", "/api/v1/datastores/"+dataStoreID, nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

// TestE2E_WorkflowLifecycle 测试工作流完整生命周期
func TestE2E_WorkflowLifecycle(t *testing.T) {
	ctx := setupE2EFullTestContext(t)
	defer ctx.cleanup()

	// Step 1: 创建工作流定义
	createReq := map[string]interface{}{
		"name":            "metadata_crawl",
		"description":     "Crawl metadata from data source",
		"category":        "metadata",
		"definition_yaml": "name: metadata_crawl\ntasks:\n  - name: crawl\n    handler: crawl_handler",
	}
	resp, err := ctx.doRequest("POST", "/api/v1/workflows", createReq)
	require.NoError(t, err)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("期望状态码 %d，实际 %d，响应: %s", http.StatusCreated, resp.StatusCode, readResponseBody(resp))
	}

	data, err := getFullResponseData(resp)
	require.NoError(t, err)
	require.NotNil(t, data)
	workflowID := getFullStringField(data, "id")
	require.NotEmpty(t, workflowID)

	// Step 2: 获取工作流
	resp, err = ctx.doRequest("GET", "/api/v1/workflows/"+workflowID, nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	wf, err := getFullResponseData(resp)
	require.NoError(t, err)
	assert.NotNil(t, wf)
	assert.Equal(t, "metadata_crawl", getFullStringField(wf, "name"))

	// Step 3: 执行工作流
	execReq := map[string]interface{}{
		"params": map[string]string{
			"data_source_id": "test-ds-id",
		},
	}
	resp, err = ctx.doRequest("POST", "/api/v1/workflows/"+workflowID+"/execute", execReq)
	require.NoError(t, err)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("期望状态码 %d，实际 %d，响应: %s", http.StatusOK, resp.StatusCode, readResponseBody(resp))
	}

	// Step 4: 列出所有工作流
	resp, err = ctx.doRequest("GET", "/api/v1/workflows", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	workflows, err := getFullResponseDataList(resp)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(workflows), 1)

	// Step 5: 禁用工作流
	resp, err = ctx.doRequest("POST", "/api/v1/workflows/"+workflowID+"/disable", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Step 6: 启用工作流
	resp, err = ctx.doRequest("POST", "/api/v1/workflows/"+workflowID+"/enable", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Step 7: 删除工作流
	resp, err = ctx.doRequest("DELETE", "/api/v1/workflows/"+workflowID, nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

// TestE2E_FullHealthCheck 测试健康检查（完整模式）
func TestE2E_FullHealthCheck(t *testing.T) {
	ctx := setupE2EFullTestContext(t)
	defer ctx.cleanup()

	resp, err := ctx.doRequest("GET", "/health", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	response, err := parseFullResponse(resp)
	require.NoError(t, err)
	assert.Equal(t, "healthy", response["status"])
}

// TestE2E_FullErrorHandling 测试错误处理（完整模式）
func TestE2E_FullErrorHandling(t *testing.T) {
	ctx := setupE2EFullTestContext(t)
	defer ctx.cleanup()

	t.Run("获取不存在的数据源返回 404", func(t *testing.T) {
		resp, err := ctx.doRequest("GET", "/api/v1/datasources/non-existent-id", nil)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("创建数据源缺少必填字段返回 400", func(t *testing.T) {
		req := map[string]string{
			"description": "缺少 name 字段",
		}
		resp, err := ctx.doRequest("POST", "/api/v1/datasources", req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
}

// TestE2E_BatchOperations 测试批量操作
func TestE2E_BatchOperations(t *testing.T) {
	ctx := setupE2EFullTestContext(t)
	defer ctx.cleanup()

	// 批量创建数据源
	numSources := 5
	createdIDs := make([]string, 0, numSources)

	for i := 0; i < numSources; i++ {
		req := map[string]string{
			"name":        fmt.Sprintf("Batch Source %d", i),
			"description": fmt.Sprintf("批量创建 %d", i),
			"base_url":    fmt.Sprintf("http://api%d.test.com", i),
		}
		resp, err := ctx.doRequest("POST", "/api/v1/datasources", req)
		require.NoError(t, err)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("创建源 %d 失败: 期望状态码 %d，实际 %d，响应: %s", i, http.StatusCreated, resp.StatusCode, readResponseBody(resp))
		}

		data, err := getFullResponseData(resp)
		require.NoError(t, err)
		id := getFullStringField(data, "id")
		require.NotEmpty(t, id)
		createdIDs = append(createdIDs, id)
	}

	assert.Len(t, createdIDs, numSources)

	// 验证所有都已创建
	resp, err := ctx.doRequest("GET", "/api/v1/datasources", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	sources, err := getFullResponseDataList(resp)
	require.NoError(t, err)
	assert.Len(t, sources, numSources)
}
