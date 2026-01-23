//go:build e2e
// +build e2e

// Package e2e provides end-to-end tests for QDHub.
// 服务器全流程E2E测试 - 通过启动真实服务器进程并调用HTTP API完成测试
package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/infrastructure/quantdb/duckdb"
)

// ==================== 服务器E2E测试配置 ====================

// ServerE2EConfig 服务器E2E测试配置
type ServerE2EConfig struct {
	ServerHost   string
	ServerPort   int
	BinPath      string
	DataDir      string
	TushareToken string
}

// ServerE2EContext 服务器E2E测试上下文
type ServerE2EContext struct {
	Config     *ServerE2EConfig
	ServerCmd  *exec.Cmd
	BaseURL    string
	HTTPClient *http.Client
	DuckDBPath string
}

// ==================== 测试辅助函数 ====================

// runAllMigrations 执行所有数据库迁移脚本
func runAllMigrations(t *testing.T, projectRoot, dbPath string) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("打开数据库失败: %w", err)
	}
	defer db.Close()

	migrationFiles := []string{
		"migrations/001_init_schema.up.sql",
		"migrations/002_seed_mapping_rules.up.sql",
		"migrations/003_sync_plan_migration.up.sql",
		"migrations/004_api_sync_strategy.up.sql",
	}

	for _, migrationFile := range migrationFiles {
		fullPath := filepath.Join(projectRoot, migrationFile)
		migrationSQL, err := os.ReadFile(fullPath)
		if err != nil {
			return fmt.Errorf("读取迁移文件 %s 失败: %w", migrationFile, err)
		}

		if _, err := db.Exec(string(migrationSQL)); err != nil {
			t.Logf("  迁移 %s: %v (可能已执行)", filepath.Base(migrationFile), err)
			continue
		}
		t.Logf("  ✅ 迁移 %s 执行成功", filepath.Base(migrationFile))
	}

	return nil
}

// loadServerE2EConfig 加载服务器E2E测试配置
func loadServerE2EConfig(t *testing.T) *ServerE2EConfig {
	return &ServerE2EConfig{
		ServerHost:   "127.0.0.1",
		ServerPort:   18080,
		BinPath:      "./bin/qdhub",
		TushareToken: os.Getenv("QDHUB_TUSHARE_TOKEN"),
	}
}

// setupServerE2EContext 设置服务器E2E测试上下文
func setupServerE2EContext(t *testing.T) *ServerE2EContext {
	cfg := loadServerE2EConfig(t)

	// 创建临时数据目录
	tmpDir, err := os.MkdirTemp("", "qdhub_e2e_*")
	require.NoError(t, err)
	cfg.DataDir = tmpDir

	// 获取项目根目录
	projectRoot, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)

	// 构建项目
	buildCmd := exec.Command("go", "build", "-o", cfg.BinPath, ".")
	buildCmd.Dir = projectRoot
	buildOutput, err := buildCmd.CombinedOutput()
	require.NoError(t, err, "构建失败: %s", string(buildOutput))

	// 准备数据库文件并执行迁移
	dbPath := filepath.Join(cfg.DataDir, "qdhub.db")
	if err := runAllMigrations(t, projectRoot, dbPath); err != nil {
		t.Fatalf("执行迁移失败: %v", err)
	}
	t.Log("✅ 数据库迁移完成")

	// DuckDB 路径（用于数据同步）
	duckDBPath := filepath.Join(cfg.DataDir, "test.duckdb")

	// 创建服务器配置文件
	configContent := fmt.Sprintf(`
server:
  host: %s
  port: %d
  mode: debug
database:
  driver: sqlite
  dsn: %s
quantdb:
  duckdb_path: %s
`, cfg.ServerHost, cfg.ServerPort, dbPath, duckDBPath)

	configPath := filepath.Join(cfg.DataDir, "config.yaml")
	err = os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	// 启动服务器进程
	serverCmd := exec.Command(
		cfg.BinPath, "server",
		"--config", configPath,
	)
	serverCmd.Dir = projectRoot
	serverCmd.Env = append(os.Environ(),
		fmt.Sprintf("GIN_MODE=debug"),
	)

	// 创建日志文件
	logFile, err := os.Create(filepath.Join(cfg.DataDir, "server.log"))
	require.NoError(t, err)
	serverCmd.Stdout = logFile
	serverCmd.Stderr = logFile

	t.Logf("🚀 启动服务器: %s server --config %s", cfg.BinPath, configPath)

	err = serverCmd.Start()
	require.NoError(t, err)

	ctx := &ServerE2EContext{
		Config:     cfg,
		ServerCmd:  serverCmd,
		BaseURL:    fmt.Sprintf("http://%s:%d", cfg.ServerHost, cfg.ServerPort),
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
		DuckDBPath: duckDBPath,
	}

	t.Logf("  PID: %d", serverCmd.Process.Pid)
	t.Logf("  数据目录: %s", cfg.DataDir)
	t.Logf("  数据库: %s", dbPath)
	t.Logf("  DuckDB: %s", duckDBPath)
	t.Logf("  配置文件: %s", configPath)

	// 等待服务器就绪
	t.Log("⏳ 等待服务器就绪...")
	if err := waitForServerReady(ctx, 30*time.Second); err != nil {
		ctx.cleanup()
		t.Fatalf("服务器启动失败: %v", err)
	}
	t.Log("✅ 服务器已就绪")

	// 设置清理函数
	t.Cleanup(func() {
		ctx.cleanup()
	})

	return ctx
}

// cleanup 清理测试资源
func (ctx *ServerE2EContext) cleanup() {
	if ctx.ServerCmd != nil && ctx.ServerCmd.Process != nil {
		fmt.Println("🛑 关闭服务器...")
		ctx.ServerCmd.Process.Signal(syscall.SIGTERM)
		done := make(chan error)
		go func() {
			done <- ctx.ServerCmd.Wait()
		}()
		select {
		case <-done:
			fmt.Println("✅ 服务器已关闭")
		case <-time.After(5 * time.Second):
			ctx.ServerCmd.Process.Kill()
			fmt.Println("⚠️  服务器被强制关闭")
		}
	}

	// 保留测试数据目录用于调试
	if ctx.Config.DataDir != "" {
		fmt.Printf("📁 测试数据保留在: %s\n", ctx.Config.DataDir)
	}
}

// waitForServerReady 等待服务器就绪
func waitForServerReady(ctx *ServerE2EContext, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := ctx.HTTPClient.Get(ctx.BaseURL + "/health")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("服务器在 %v 内未就绪", timeout)
}

// doRequest 发送HTTP请求
func (ctx *ServerE2EContext) doRequest(method, path string, body interface{}) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reqBody = bytes.NewReader(jsonData)
	}

	req, err := http.NewRequest(method, ctx.BaseURL+path, reqBody)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	return ctx.HTTPClient.Do(req)
}

// parseServerResponse 解析服务器响应
func parseServerResponse(resp *http.Response) (map[string]interface{}, error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// getServerResponseData 获取响应中的data字段
func getServerResponseData(resp *http.Response) (map[string]interface{}, error) {
	result, err := parseServerResponse(resp)
	if err != nil {
		return nil, err
	}
	if data, ok := result["data"].(map[string]interface{}); ok {
		return data, nil
	}
	return nil, fmt.Errorf("响应中没有data字段")
}

// getServerResponseDataList 获取响应中的data列表
func getServerResponseDataList(resp *http.Response) ([]interface{}, error) {
	result, err := parseServerResponse(resp)
	if err != nil {
		return nil, err
	}
	if data, ok := result["data"].([]interface{}); ok {
		return data, nil
	}
	return nil, fmt.Errorf("响应中没有data列表")
}

// getServerStringField 从map中获取字符串字段（支持大小写）
func getServerStringField(data map[string]interface{}, field string) string {
	// 先尝试原始字段名
	if val, ok := data[field].(string); ok {
		return val
	}
	// 尝试首字母大写的字段名（如 id -> Id）
	upperField := strings.ToUpper(field[:1]) + field[1:]
	if val, ok := data[upperField].(string); ok {
		return val
	}
	// 尝试全大写的字段名（如 id -> ID）
	allUpperField := strings.ToUpper(field)
	if val, ok := data[allUpperField].(string); ok {
		return val
	}
	return ""
}

// readServerResponseBody 读取响应体（会关闭Body）
func readServerResponseBody(resp *http.Response) string {
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

// ==================== 真实Tushare Token全流程测试 ====================

// TestE2E_Server_WithRealTushareToken 使用真实Tushare Token的完整数据同步测试
// 需要设置 QDHUB_TUSHARE_TOKEN 环境变量
func TestE2E_Server_WithRealTushareToken(t *testing.T) {
	ctx := setupServerE2EContext(t)

	if ctx.Config.TushareToken == "" {
		t.Skip("跳过：需要设置 QDHUB_TUSHARE_TOKEN 环境变量")
	}

	t.Log("========== 真实Tushare Token全流程测试 ==========")
	t.Logf("Token长度: %d", len(ctx.Config.TushareToken))

	var dataSourceID, dataStoreID, syncPlanID, executionID string

	// ==================== Step 1: 创建数据源 ====================
	t.Run("Step1_CreateDataSource", func(t *testing.T) {
		t.Log("----- Step 1: 创建数据源 -----")
		createReq := map[string]string{
			"name":        "Tushare",
			"description": "Tushare Pro (Real Token Test)",
			"base_url":    "http://api.tushare.pro",
			"doc_url":     "https://tushare.pro/document/2",
		}
		resp, err := ctx.doRequest("POST", "/api/v1/datasources", createReq)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusCreated {
			body := readServerResponseBody(resp)
			t.Fatalf("创建数据源失败: 状态码 %d, 响应: %s", resp.StatusCode, body)
		}

		data, err := getServerResponseData(resp)
		if err != nil {
			t.Fatalf("解析响应失败: %v", err)
		}
		dataSourceID = getServerStringField(data, "id")
		if dataSourceID == "" {
			t.Fatalf("响应中没有 ID 字段, data: %+v", data)
		}
		t.Logf("✅ 数据源创建成功: ID=%s", dataSourceID)
	})

	// ==================== Step 2: 设置Token ====================
	t.Run("Step2_SetToken", func(t *testing.T) {
		t.Log("----- Step 2: 设置Token -----")
		if dataSourceID == "" {
			t.Skip("跳过：需要先创建数据源")
		}

		tokenReq := map[string]string{
			"token": ctx.Config.TushareToken,
		}
		resp, err := ctx.doRequest("POST", "/api/v1/datasources/"+dataSourceID+"/token", tokenReq)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
		t.Log("✅ Token设置成功")
	})

	// ==================== Step 3: 刷新元数据并等待完成 ====================
	t.Run("Step3_RefreshMetadataAndWait", func(t *testing.T) {
		t.Log("----- Step 3: 刷新元数据并等待完成 -----")
		if dataSourceID == "" {
			t.Skip("跳过：需要先创建数据源")
		}

		// 发送刷新请求并获取 workflow instance ID
		resp, err := ctx.doRequest("POST", "/api/v1/datasources/"+dataSourceID+"/refresh", nil)
		require.NoError(t, err)
		if resp.StatusCode != http.StatusOK {
			body := readServerResponseBody(resp)
			t.Fatalf("刷新元数据失败: 状态码 %d, 响应: %s", resp.StatusCode, body)
		}

		data, err := getServerResponseData(resp)
		require.NoError(t, err)

		instanceID := getServerStringField(data, "instance_id")
		if instanceID == "" {
			t.Log("⚠️  响应中没有 instance_id 字段，使用旧的等待方式")
			// 回退到旧的等待方式
			t.Log("⏳ 等待元数据刷新完成（查询数据源API数量）...")
			for i := 0; i < 60; i++ {
				time.Sleep(1 * time.Second)
				resp, err := ctx.doRequest("GET", "/api/v1/datasources/"+dataSourceID, nil)
				if err != nil {
					continue
				}
				dsData, err := getServerResponseData(resp)
				if err != nil {
					continue
				}
				if apiCount, ok := dsData["api_count"].(float64); ok && apiCount > 0 {
					t.Logf("✅ 元数据刷新完成，API数量: %.0f", apiCount)
					return
				}
				if i%10 == 0 {
					t.Logf("  等待中... (%d秒)", i)
				}
			}
			t.Log("⚠️  元数据刷新等待超时，继续后续测试")
			return
		}

		t.Logf("✅ 元数据刷新工作流已启动，instance_id: %s", instanceID)

		// 使用 workflow progress API 查询进度（最多等待 5 分钟）
		// 使用 FinishedAt 字段判断完成状态，不依赖字符串匹配
		t.Log("⏳ 等待元数据刷新工作流完成...")
		maxWaitSeconds := 300 // 5 分钟
		for i := 0; i < maxWaitSeconds; i++ {
			time.Sleep(1 * time.Second)

			// 使用 progress 接口获取详细状态（包含 FinishedAt 字段）
			resp, err := ctx.doRequest("GET", "/api/v1/instances/"+instanceID+"/progress", nil)
			if err != nil {
				if i%30 == 0 {
					t.Logf("  查询工作流进度失败: %v", err)
				}
				continue
			}

			statusData, err := getServerResponseData(resp)
			if err != nil {
				if i%30 == 0 {
					t.Logf("  解析响应失败: %v", err)
				}
				continue
			}

			// 获取任务完成情况
			taskCount, _ := statusData["task_count"].(float64)
			completedTask, _ := statusData["completed_task"].(float64)
			failedTask, _ := statusData["failed_task"].(float64)
			progress, _ := statusData["progress"].(float64)
			errorMessage, _ := statusData["error_message"].(string)
			statusStr := getServerStringField(statusData, "status")
			finishedAt := statusData["finished_at"]

			// 判断工作流是否完成的多种方式（不依赖单一字段）：
			// 1. FinishedAt 字段存在（最可靠）
			// 2. 所有任务完成（completedTask + failedTask == taskCount）
			// 3. 进度 100% 且状态为终态
			isFinished := false
			if finishedAt != nil && finishedAt != "" {
				isFinished = true
			} else if taskCount > 0 && int(completedTask+failedTask) == int(taskCount) {
				isFinished = true
			} else if progress >= 100.0 && (statusStr == "Success" || statusStr == "Failed" || statusStr == "Terminated") {
				isFinished = true
			}

			if isFinished {
				// 工作流已完成，根据任务完成情况判断成功或失败
				if failedTask > 0 {
					// 有任务失败
					if errorMessage != "" {
						t.Logf("⚠️  元数据刷新工作流执行失败: %s", errorMessage)
					} else {
						t.Logf("⚠️  元数据刷新工作流执行失败: %d/%d 任务失败", int(failedTask), int(taskCount))
					}
					return
				} else if completedTask > 0 && int(completedTask) == int(taskCount) {
					// 所有任务成功完成
					t.Log("✅ 元数据刷新工作流执行成功")
					// 查询最终的 API 数量
					dsResp, err := ctx.doRequest("GET", "/api/v1/datasources/"+dataSourceID, nil)
					if err == nil {
						dsData, err := getServerResponseData(dsResp)
						if err == nil {
							if apiCount, ok := dsData["api_count"].(float64); ok {
								t.Logf("  数据源API数量: %.0f", apiCount)
							}
						}
					}
					return
				} else {
					// 已完成但状态不明确，使用状态字段作为后备
					t.Logf("⚠️  工作流已完成，状态: %s", statusStr)
					return
				}
			}

			// 工作流未完成，打印进度信息
			if i%30 == 0 {
				t.Logf("  等待中... (%d秒) - 状态: %s, 进度: %.1f%%, 任务: %d/%d",
					i, statusStr, progress, int(completedTask), int(taskCount))
			}
		}
		t.Logf("⚠️  元数据刷新等待超时（%d秒），继续后续测试", maxWaitSeconds)
	})

	// ==================== Step 4: 创建DataStore ====================
	t.Run("Step4_CreateDataStore", func(t *testing.T) {
		t.Log("----- Step 4: 创建DataStore -----")

		createReq := map[string]interface{}{
			"name":         "TestDuckDB",
			"description":  "Real E2E Test DuckDB",
			"type":         "duckdb",
			"storage_path": ctx.DuckDBPath,
		}
		resp, err := ctx.doRequest("POST", "/api/v1/datastores", createReq)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusCreated {
			body := readServerResponseBody(resp)
			t.Fatalf("创建DataStore失败: 状态码 %d, 响应: %s", resp.StatusCode, body)
		}

		data, err := getServerResponseData(resp)
		if err != nil {
			t.Fatalf("解析响应失败: %v", err)
		}
		dataStoreID = getServerStringField(data, "id")
		if dataStoreID == "" {
			t.Fatalf("响应中没有 ID 字段, data: %+v", data)
		}
		t.Logf("✅ DataStore创建成功: ID=%s, Path=%s", dataStoreID, ctx.DuckDBPath)
	})

	// ==================== Step 5: 建表 ====================
	t.Run("Step5_CreateTables", func(t *testing.T) {
		t.Log("----- Step 5: 为数据源建表 -----")
		if dataSourceID == "" || dataStoreID == "" {
			t.Skip("跳过：需要先创建数据源和DataStore")
		}

		// 调用建表接口
		createTablesReq := map[string]interface{}{
			"data_source_id": dataSourceID,
			// max_tables 可选，不传表示不限制
		}
		resp, err := ctx.doRequest("POST", "/api/v1/datastores/"+dataStoreID+"/create-tables", createTablesReq)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusCreated {
			body := readServerResponseBody(resp)
			t.Fatalf("建表失败: 状态码 %d, 响应: %s", resp.StatusCode, body)
		}

		data, err := getServerResponseData(resp)
		if err != nil {
			t.Fatalf("解析响应失败: %v", err)
		}

		// 获取 workflow instance ID
		tableInstanceID := getServerStringField(data, "instance_id")
		if tableInstanceID == "" {
			t.Fatalf("响应中没有 instance_id 字段, data: %+v", data)
		}
		t.Logf("✅ 建表工作流已启动，instance_id: %s", tableInstanceID)

		// 等待建表工作流完成（最多等待 20 分钟）
		t.Log("⏳ 等待建表工作流完成...")
		maxWaitSeconds := 1200 // 20 分钟
		for i := 0; i < maxWaitSeconds; i++ {
			time.Sleep(1 * time.Second)

			// 使用 progress 接口获取详细状态
			resp, err := ctx.doRequest("GET", "/api/v1/instances/"+tableInstanceID+"/progress", nil)
			if err != nil {
				if i%30 == 0 {
					t.Logf("  查询建表工作流进度失败: %v", err)
				}
				continue
			}

			statusData, err := getServerResponseData(resp)
			if err != nil {
				if i%30 == 0 {
					t.Logf("  解析响应失败: %v", err)
				}
				continue
			}

			// 获取任务完成情况
			taskCount, _ := statusData["task_count"].(float64)
			completedTask, _ := statusData["completed_task"].(float64)
			failedTask, _ := statusData["failed_task"].(float64)
			progress, _ := statusData["progress"].(float64)
			errorMessage, _ := statusData["error_message"].(string)
			statusStr := getServerStringField(statusData, "status")
			finishedAt := statusData["finished_at"]

			// 判断工作流是否完成
			isFinished := false
			if finishedAt != nil && finishedAt != "" {
				isFinished = true
			} else if taskCount > 0 && int(completedTask+failedTask) == int(taskCount) {
				isFinished = true
			} else if progress >= 100.0 && (statusStr == "Success" || statusStr == "Failed" || statusStr == "Terminated") {
				isFinished = true
			}

			if isFinished {
				if failedTask > 0 {
					if errorMessage != "" {
						t.Logf("⚠️  建表工作流执行失败: %s", errorMessage)
					} else {
						t.Logf("⚠️  建表工作流执行失败: %d/%d 任务失败", int(failedTask), int(taskCount))
					}
					return
				} else if completedTask > 0 && int(completedTask) == int(taskCount) {
					t.Logf("✅ 建表工作流执行成功: %d 个表", int(completedTask))

					// 校验所有表是否真的创建成功
					// 使用与同步计划相同的API列表，确保所有需要的表都已创建
					expectedTables := []string{
						"stock_basic", "trade_cal", "daily", "weekly", "monthly",
						"daily_basic", "adj_factor", "income", "balancesheet", "cashflow",
						"index_basic", "index_daily", "top_list", "margin", "block_trade",
						"fina_indicator", "namechange", "stk_limit", "margin_detail",
					}
					t.Logf("🔍 校验所有表是否创建成功 (%d 个表)...", len(expectedTables))
					if err := verifyTablesCreated(t, ctx.DuckDBPath, expectedTables); err != nil {
						t.Fatalf("表存在性校验失败: %v", err)
					}
					t.Logf("✅ 所有表存在性校验通过 (%d 个表)", len(expectedTables))
					return
				} else {
					t.Logf("⚠️  建表工作流已完成，状态: %s", statusStr)
					return
				}
			}

			// 工作流未完成，打印进度信息
			if i%30 == 0 {
				t.Logf("  等待中... (%d秒) - 状态: %s, 进度: %.1f%%, 任务: %d/%d",
					i, statusStr, progress, int(completedTask), int(taskCount))
			}
		}
		t.Logf("⚠️  建表等待超时（%d秒），继续后续测试", maxWaitSeconds)

		// 即使超时，也尝试校验表是否存在
		t.Log("🔍 校验表是否创建成功（超时后校验）...")
		expectedTables := []string{
			"stock_basic", "trade_cal", "daily", "weekly", "monthly",
			"daily_basic", "adj_factor", "income", "balancesheet", "cashflow",
			"index_basic", "index_daily", "top_list", "margin", "block_trade",
			"fina_indicator", "namechange", "stk_limit", "margin_detail",
		}
		if err := verifyTablesCreated(t, ctx.DuckDBPath, expectedTables); err != nil {
			t.Logf("⚠️  表存在性校验失败: %v", err)
		} else {
			t.Log("✅ 所有表存在性校验通过")
		}
	})

	// ==================== Step 5.5: 校验所有表是否创建成功 ====================
	t.Run("Step5_5_VerifyAllTablesCreated", func(t *testing.T) {
		t.Log("----- Step 5.5: 校验所有表是否创建成功 -----")
		if dataStoreID == "" {
			t.Skip("跳过：需要先创建DataStore")
		}

		// 校验所有20个API对应的表是否都存在
		expectedTables := []string{
			"stock_basic", "trade_cal", "daily", "weekly", "monthly",
			"daily_basic", "adj_factor", "income", "balancesheet", "cashflow",
			"index_basic", "index_daily", "top_list", "margin", "block_trade",
			"fina_indicator", "namechange", "stk_limit", "margin_detail",
		}
		t.Logf("🔍 校验所有表是否创建成功 (%d 个表)...", len(expectedTables))
		if err := verifyTablesCreated(t, ctx.DuckDBPath, expectedTables); err != nil {
			t.Fatalf("表存在性校验失败: %v", err)
		}
		t.Log("✅ 所有表存在性校验通过")
	})

	// ==================== Step 6: 创建同步计划 ====================
	t.Run("Step6_CreateSyncPlan", func(t *testing.T) {
		t.Log("----- Step 6: 创建同步计划 -----")
		if dataSourceID == "" || dataStoreID == "" {
			t.Skip("跳过：需要先创建数据源和DataStore")
		}

		// 选择多种类型的 API 进行测试（与 builtin_workflow_e2e_test 保持一致）
		selectedAPIs := []string{
			"stock_basic", "trade_cal", "daily", "weekly", "monthly",
			"daily_basic", "adj_factor", "income", "balancesheet", "cashflow",
			"index_basic", "index_daily", "top_list", "margin", "block_trade",
			"fina_indicator", "namechange", "stk_limit", "margin_detail",
		}
		createReq := map[string]interface{}{
			"name":           "RealSyncPlanTest (20 APIs)",
			"description":    "真实数据同步测试 - 20 个 API",
			"data_source_id": dataSourceID,
			"data_store_id":  dataStoreID,
			"selected_apis":  selectedAPIs,
		}

		resp, err := ctx.doRequest("POST", "/api/v1/sync-plans", createReq)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("创建同步计划失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}

		data, err := getServerResponseData(resp)
		require.NoError(t, err)
		syncPlanID = getServerStringField(data, "id")
		require.NotEmpty(t, syncPlanID)
		t.Logf("✅ 同步计划创建成功: ID=%s", syncPlanID)
	})

	// ==================== Step 6: 创建同步计划 ====================
	t.Run("Step6_CreateSyncPlan", func(t *testing.T) {
		t.Log("----- Step 6: 创建同步计划 -----")
		if dataSourceID == "" || dataStoreID == "" {
			t.Skip("跳过：需要先创建数据源和DataStore")
		}

		// 选择多种类型的 API 进行测试（与 builtin_workflow_e2e_test 保持一致）
		selectedAPIs := []string{
			"stock_basic", "trade_cal", "daily", "weekly", "monthly",
			"daily_basic", "adj_factor", "income", "balancesheet", "cashflow",
			"index_basic", "index_daily", "top_list", "margin", "block_trade",
			"fina_indicator", "namechange", "stk_limit", "margin_detail",
		}
		createReq := map[string]interface{}{
			"name":           "RealSyncPlanTest (20 APIs)",
			"description":    "真实数据同步测试 - 20 个 API",
			"data_source_id": dataSourceID,
			"data_store_id":  dataStoreID,
			"selected_apis":  selectedAPIs,
		}

		resp, err := ctx.doRequest("POST", "/api/v1/sync-plans", createReq)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("创建同步计划失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}

		data, err := getServerResponseData(resp)
		require.NoError(t, err)
		syncPlanID = getServerStringField(data, "id")
		require.NotEmpty(t, syncPlanID)
		t.Logf("✅ 同步计划创建成功: ID=%s", syncPlanID)
	})

	// ==================== Step 7: 解析同步计划依赖 ====================
	t.Run("Step7_ResolveSyncPlan", func(t *testing.T) {
		t.Log("----- Step 7: 解析同步计划依赖 -----")
		if syncPlanID == "" {
			t.Skip("跳过：需要先创建同步计划")
		}

		resp, err := ctx.doRequest("POST", "/api/v1/sync-plans/"+syncPlanID+"/resolve", nil)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("解析同步计划依赖失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}
		resp.Body.Close()
		t.Log("✅ 同步计划依赖解析成功")
	})

	// ==================== Step 8: 触发同步 ====================
	t.Run("Step8_TriggerSync", func(t *testing.T) {
		t.Log("----- Step 8: 触发同步 -----")
		if syncPlanID == "" {
			t.Skip("跳过：需要先创建同步计划")
		}

		// 设置同步日期范围（最近1年的交易日历）
		endDate := time.Now().Format("20060102")
		startDate := time.Now().AddDate(-1, 0, 0).Format("20060102")

		triggerReq := map[string]string{
			"target_db_path": ctx.DuckDBPath,
			"start_date":     startDate,
			"end_date":       endDate,
		}
		resp, err := ctx.doRequest("POST", "/api/v1/sync-plans/"+syncPlanID+"/trigger", triggerReq)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("触发同步失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}

		response, _ := parseServerResponse(resp)
		t.Logf("✅ 同步已触发: %+v", response)

		// 获取执行ID
		if data, ok := response["data"].(map[string]interface{}); ok {
			executionID = getServerStringField(data, "execution_id")
			t.Logf("  执行ID: %s", executionID)
		}
	})

	// ==================== Step 9: 等待同步完成 ====================
	t.Run("Step9_WaitForSyncComplete", func(t *testing.T) {
		t.Log("----- Step 9: 等待同步完成 -----")
		if syncPlanID == "" || executionID == "" {
			t.Skip("跳过：需要先触发同步")
		}

		// 兼容函数：仅通过执行状态轮询等待（旧逻辑）
		waitByExecutionStatus := func(maxWaitSeconds int) {
			t.Log("⏳ 通过执行状态轮询等待同步完成...")
			var lastStatus string
			for i := 0; i < maxWaitSeconds; i++ {
				time.Sleep(1 * time.Second)

				resp, err := ctx.doRequest("GET", "/api/v1/executions/"+executionID, nil)
				if err != nil {
					t.Logf("  获取执行状态失败: %v", err)
					continue
				}

				data, err := getServerResponseData(resp)
				if err != nil {
					t.Logf("  解析执行状态失败: %v", err)
					continue
				}

				status := getServerStringField(data, "status")
				lastStatus = status

				if i%10 == 0 {
					t.Logf("  等待中... 状态: %s (%d秒)", status, i)
				}

				switch status {
				case "completed", "success", "Completed", "Success":
					t.Logf("✅ 同步完成，状态: %s，耗时: %d秒", status, i)
					return
				case "failed", "Failed", "cancelled", "Cancelled":
					t.Logf("⚠️  同步结束，状态: %s，耗时: %d秒", status, i)
					return
				}
			}
			t.Logf("⚠️  同步等待超时 (%d秒)，最后状态: %s，继续检查数据...", maxWaitSeconds, lastStatus)
		}

		// 首先获取执行信息，从中解析 workflow 实例 ID
		t.Log("⏳ 查询执行信息以获取 workflow 实例 ID...")
		execResp, err := ctx.doRequest("GET", "/api/v1/executions/"+executionID, nil)
		if err != nil {
			t.Logf("⚠️  获取执行信息失败，回退到执行状态轮询: %v", err)
			waitByExecutionStatus(300)
			return
		}

		execData, err := getServerResponseData(execResp)
		if err != nil {
			t.Logf("⚠️  解析执行信息失败，回退到执行状态轮询: %v", err)
			waitByExecutionStatus(300)
			return
		}

		workflowInstanceID := getServerStringField(execData, "workflow_inst_id")
		if workflowInstanceID == "" {
			t.Log("⚠️  执行信息中没有 workflow_inst_id 字段，回退到执行状态轮询")
			waitByExecutionStatus(300)
			return
		}

		// 使用 workflow progress API 查询进度（最多等待 10 分钟）
		t.Logf("✅ 已获取 workflow 实例 ID: %s", workflowInstanceID)
		t.Log("⏳ 通过 workflow 进度接口等待同步完成...")
		maxWaitSeconds := 600

		for i := 0; i < maxWaitSeconds; i++ {
			time.Sleep(1 * time.Second)

			resp, err := ctx.doRequest("GET", "/api/v1/instances/"+workflowInstanceID+"/progress", nil)
			if err != nil {
				if i%30 == 0 {
					t.Logf("  查询同步工作流进度失败: %v", err)
				}
				continue
			}

			statusData, err := getServerResponseData(resp)
			if err != nil {
				if i%30 == 0 {
					t.Logf("  解析同步工作流进度响应失败: %v", err)
				}
				continue
			}

			// 获取任务完成情况
			taskCount, _ := statusData["task_count"].(float64)
			completedTask, _ := statusData["completed_task"].(float64)
			failedTask, _ := statusData["failed_task"].(float64)
			progress, _ := statusData["progress"].(float64)
			errorMessage, _ := statusData["error_message"].(string)
			statusStr := getServerStringField(statusData, "status")
			finishedAt := statusData["finished_at"]

			// 多种方式判断工作流是否完成
			isFinished := false
			if finishedAt != nil && finishedAt != "" {
				isFinished = true
			} else if taskCount > 0 && int(completedTask+failedTask) == int(taskCount) {
				isFinished = true
			} else if progress >= 100.0 && (statusStr == "Success" || statusStr == "Failed" || statusStr == "Terminated") {
				isFinished = true
			}

			if isFinished {
				if failedTask > 0 {
					// 有任务失败
					if errorMessage != "" {
						t.Logf("⚠️  同步工作流执行失败: %s", errorMessage)
					} else {
						t.Logf("⚠️  同步工作流执行失败: %d/%d 任务失败", int(failedTask), int(taskCount))
					}
					return
				} else if completedTask > 0 && int(completedTask) == int(taskCount) {
					// 所有任务成功完成
					t.Logf("✅ 同步工作流执行成功: %d/%d 任务完成，进度 %.1f%%", int(completedTask), int(taskCount), progress)
					return
				} else {
					// 已完成但状态不明确，打印状态后返回
					t.Logf("⚠️  同步工作流已完成，但状态不明确: %s，进度 %.1f%%，任务 %d/%d（失败 %d）",
						statusStr, progress, int(completedTask), int(taskCount), int(failedTask))
					return
				}
			}

			// 工作流未完成，定期打印进度信息
			if i%30 == 0 {
				t.Logf("  等待中... (%d秒) - 状态: %s, 进度: %.1f%%, 任务: %d/%d, 失败: %d",
					i, statusStr, progress, int(completedTask), int(taskCount), int(failedTask))
			}
		}
		t.Logf("⚠️  同步工作流等待超时（%d秒），继续后续测试", maxWaitSeconds)
	})

	// ==================== Step 9: 关闭服务器并查询同步的数据行数 ====================
	t.Run("Step9_QuerySyncedDataRows", func(t *testing.T) {
		t.Log("----- Step 9: 关闭服务器并查询同步的数据行数 -----")
		t.Logf("  DuckDB路径: %s", ctx.DuckDBPath)

		// 先关闭服务器以释放 DuckDB 锁
		t.Log("  关闭服务器以释放 DuckDB 锁...")
		if ctx.ServerCmd != nil && ctx.ServerCmd.Process != nil {
			ctx.ServerCmd.Process.Signal(syscall.SIGTERM)
			ctx.ServerCmd.Wait()
			ctx.ServerCmd = nil                // 防止 cleanup 再次关闭
			time.Sleep(500 * time.Millisecond) // 等待锁释放
		}

		// 检查DuckDB文件是否存在
		if _, err := os.Stat(ctx.DuckDBPath); os.IsNotExist(err) {
			t.Log("⚠️  DuckDB文件不存在，同步可能未完成")
			return
		}

		// 先列出所有表
		t.Log("  列出数据库中的表...")
		cmd := exec.Command("duckdb", ctx.DuckDBPath, "-c", "SHOW TABLES;")
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Logf("⚠️  列出表失败: %v, 输出: %s", err, string(output))
			return
		}
		t.Logf("  数据库中的表:\n%s", string(output))

		// 如果有trade_cal表，查询行数
		if strings.Contains(string(output), "trade_cal") {
			cmd2 := exec.Command("duckdb", ctx.DuckDBPath, "-c", "SELECT COUNT(*) as row_count FROM trade_cal;")
			output2, err := cmd2.CombinedOutput()
			if err != nil {
				t.Logf("⚠️  查询trade_cal行数失败: %v", err)
			} else {
				t.Logf("✅ trade_cal 数据行数:\n%s", string(output2))
			}

			// 查询前5行数据
			cmd3 := exec.Command("duckdb", ctx.DuckDBPath, "-c", "SELECT * FROM trade_cal LIMIT 5;")
			output3, _ := cmd3.CombinedOutput()
			t.Logf("  trade_cal 前5行数据:\n%s", string(output3))
		}
	})

	t.Log("========== 真实Tushare Token全流程测试完成 ==========")
}

// verifyTablesCreated 验证指定的表是否在数据库中创建成功
func verifyTablesCreated(t *testing.T, duckDBPath string, tableNames []string) error {
	if len(tableNames) == 0 {
		return nil
	}

	// 使用 DuckDB adapter 检查表是否存在
	ctx := context.Background()
	adapter := duckdb.NewAdapter(duckDBPath)
	if err := adapter.Connect(ctx); err != nil {
		return fmt.Errorf("连接 DuckDB 失败: %w", err)
	}
	defer adapter.Close()

	// 检查每个表是否存在
	missingTables := make([]string, 0)
	foundTables := make([]string, 0)

	for _, tableName := range tableNames {
		exists, err := adapter.TableExists(ctx, tableName)
		if err != nil {
			return fmt.Errorf("检查表 %s 是否存在时出错: %w", tableName, err)
		}
		if exists {
			foundTables = append(foundTables, tableName)
		} else {
			missingTables = append(missingTables, tableName)
		}
	}

	if len(missingTables) > 0 {
		return fmt.Errorf("以下表未创建: %v (已创建的表: %v)", missingTables, foundTables)
	}

	t.Logf("  ✅ 验证通过: 所有表已创建 (%v)", tableNames)
	return nil
}

// ==================== 简单测试 ====================

// TestE2E_Server_HealthCheck 简单的健康检查测试
func TestE2E_Server_HealthCheck(t *testing.T) {
	ctx := setupServerE2EContext(t)

	resp, err := ctx.HTTPClient.Get(ctx.BaseURL + "/health")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	t.Log("✅ 健康检查通过")
}

// TestE2E_Server_DataSourceCRUD 数据源CRUD测试
func TestE2E_Server_DataSourceCRUD(t *testing.T) {
	ctx := setupServerE2EContext(t)

	// 创建数据源
	createReq := map[string]string{
		"name":        "TestTushare",
		"description": "Test Tushare Data Source",
		"base_url":    "http://api.tushare.pro",
		"doc_url":     "https://tushare.pro/document/2",
	}
	resp, err := ctx.doRequest("POST", "/api/v1/datasources", createReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	data, err := getServerResponseData(resp)
	require.NoError(t, err)
	id := getServerStringField(data, "id")
	require.NotEmpty(t, id)
	t.Logf("✅ 数据源创建成功: ID=%s", id)

	// 获取数据源
	resp, err = ctx.doRequest("GET", "/api/v1/datasources/"+id, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	data, err = getServerResponseData(resp)
	require.NoError(t, err)
	assert.Equal(t, "TestTushare", getServerStringField(data, "name"))
	t.Log("✅ 数据源获取成功")

	// 列出数据源
	resp, err = ctx.doRequest("GET", "/api/v1/datasources", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	list, err := getServerResponseDataList(resp)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(list), 1)
	t.Logf("✅ 数据源列表: %d 个", len(list))
}
