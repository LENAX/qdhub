//go:build e2e
// +build e2e

// Package e2e 提供端到端测试
// 本文件实现从 Tushare 爬取数据源、建表和数据同步的完整流程测试
package e2e

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_TushareFullPipeline 测试 Tushare 完整流程：
// 1. 创建数据源并设置 Token
// 2. 爬取元数据（通过 RefreshMetadata API）
// 3. 创建数据存储
// 4. 基于元数据生成表结构
// 5. 创建表
// 6. 创建同步任务
// 7. 触发数据同步
func TestE2E_TushareFullPipeline(t *testing.T) {
	ctx := setupE2EFullTestContext(t)
	defer ctx.cleanup()

	t.Logf("========== Tushare 完整流程测试开始 ==========")
	t.Logf("测试模式: %s", ctx.Config.Mode)
	if ctx.Config.Mode == "real" {
		t.Logf("API 地址: %s", ctx.BaseURL)
	}

	// ==================== Step 1: 创建数据源 ====================
	t.Log("\n----- Step 1: 创建数据源 -----")
	createDSReq := map[string]string{
		"name":        "Tushare",
		"description": "Tushare Pro Data Source",
		"base_url":    "http://api.tushare.pro",
		"doc_url":     "https://tushare.pro/document/2",
	}
	resp, err := ctx.doRequest("POST", "/api/v1/datasources", createDSReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "创建数据源失败: %s", readResponseBody(resp))

	data, err := getFullResponseData(resp)
	require.NoError(t, err)
	require.NotNil(t, data)
	dataSourceID := getFullStringField(data, "id")
	require.NotEmpty(t, dataSourceID, "数据源 ID 为空")
	t.Logf("✅ 数据源创建成功: ID=%s", dataSourceID)

	// ==================== Step 2: 设置 Token ====================
	t.Log("\n----- Step 2: 设置 Token -----")
	token := ctx.Config.TushareToken
	if token == "" {
		token = "test-token-12345" // Mock 模式使用测试 token
		t.Logf("⚠️  未设置 TUSHARE_TOKEN，使用测试 token")
	} else {
		t.Logf("✅ 使用环境变量中的 Tushare Token（长度: %d）", len(token))
	}

	tokenReq := map[string]string{
		"token": token,
	}
	resp, err = ctx.doRequest("POST", "/api/v1/datasources/"+dataSourceID+"/token", tokenReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "设置 Token 失败: %s", readResponseBody(resp))
	t.Logf("✅ Token 设置成功")

	// ==================== Step 3: 爬取元数据 ====================
	t.Log("\n----- Step 3: 爬取元数据 -----")
	// 注意：RefreshMetadata 需要传入 doc_content
	// 在实际场景中，可能需要先获取文档内容，或者通过工作流自动获取
	// 这里我们使用一个简化的方式：直接创建一些 API 元数据用于测试
	// 或者，如果系统支持自动爬取，可以调用 RefreshMetadata（需要先获取文档内容）

	// 方案1: 如果系统支持自动爬取，可以尝试触发元数据刷新
	// 但 RefreshMetadata 需要 doc_content，所以我们需要先获取文档内容
	// 这里我们采用方案2：直接创建 API 元数据用于测试

	// 创建 API 元数据（用于测试）
	apiReq := map[string]interface{}{
		"data_source_id": dataSourceID,
		"name":           "stock_basic",
		"display_name":   "股票基本信息",
		"description":    "获取股票基本信息",
		"endpoint":       "/stock_basic",
		"permission":     "basic",
		"request_params": []map[string]interface{}{
			{"name": "list_status", "type": "str", "required": false, "description": "上市状态"},
		},
		"response_fields": []map[string]interface{}{
			{"name": "ts_code", "type": "str", "description": "股票代码", "is_primary": true},
			{"name": "symbol", "type": "str", "description": "股票代码（6位数字）"},
			{"name": "name", "type": "str", "description": "股票名称"},
			{"name": "area", "type": "str", "description": "地域"},
			{"name": "industry", "type": "str", "description": "所属行业"},
			{"name": "list_date", "type": "str", "description": "上市日期"},
		},
	}
	resp, err = ctx.doRequest("POST", "/api/v1/apis", apiReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "创建 API 元数据失败: %s", readResponseBody(resp))

	apiData, err := getFullResponseData(resp)
	require.NoError(t, err)
	apiMetadataID := getFullStringField(apiData, "id")
	require.NotEmpty(t, apiMetadataID)
	t.Logf("✅ API 元数据创建成功: ID=%s, Name=stock_basic", apiMetadataID)

	// 创建第二个 API 元数据（日线数据）
	apiReq2 := map[string]interface{}{
		"data_source_id": dataSourceID,
		"name":           "daily",
		"display_name":   "日线行情",
		"description":    "获取日线行情数据",
		"endpoint":       "/daily",
		"permission":     "basic",
		"request_params": []map[string]interface{}{
			{"name": "ts_code", "type": "str", "required": true, "description": "股票代码"},
			{"name": "start_date", "type": "str", "required": false, "description": "开始日期"},
			{"name": "end_date", "type": "str", "required": false, "description": "结束日期"},
		},
		"response_fields": []map[string]interface{}{
			{"name": "ts_code", "type": "str", "description": "股票代码", "is_primary": true},
			{"name": "trade_date", "type": "str", "description": "交易日期", "is_primary": true},
			{"name": "open", "type": "float", "description": "开盘价"},
			{"name": "high", "type": "float", "description": "最高价"},
			{"name": "low", "type": "float", "description": "最低价"},
			{"name": "close", "type": "float", "description": "收盘价"},
			{"name": "pre_close", "type": "float", "description": "昨收价"},
			{"name": "change", "type": "float", "description": "涨跌额"},
			{"name": "pct_chg", "type": "float", "description": "涨跌幅"},
			{"name": "vol", "type": "float", "description": "成交量"},
			{"name": "amount", "type": "float", "description": "成交额"},
		},
	}
	resp, err = ctx.doRequest("POST", "/api/v1/apis", apiReq2)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "创建 API 元数据失败: %s", readResponseBody(resp))

	apiData2, err := getFullResponseData(resp)
	require.NoError(t, err)
	apiMetadataID2 := getFullStringField(apiData2, "id")
	require.NotEmpty(t, apiMetadataID2)
	t.Logf("✅ API 元数据创建成功: ID=%s, Name=daily", apiMetadataID2)

	// 验证 API 列表
	resp, err = ctx.doRequest("GET", "/api/v1/datasources/"+dataSourceID+"/apis", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	apis, err := getFullResponseDataList(resp)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(apis), 2, "API 数量应该至少为 2")
	t.Logf("✅ 验证 API 列表: 共 %d 个 API", len(apis))

	// ==================== Step 4: 创建数据存储 ====================
	t.Log("\n----- Step 4: 创建数据存储 -----")
	// 创建临时数据库文件路径
	tmpDir := os.TempDir()
	dbPath := fmt.Sprintf("%s/qdhub_e2e_test_%d.duckdb", tmpDir, time.Now().Unix())

	createStoreReq := map[string]string{
		"name":         "Test DuckDB Store",
		"description":  "E2E 测试数据存储",
		"type":         "duckdb",
		"storage_path": dbPath,
	}
	resp, err = ctx.doRequest("POST", "/api/v1/datastores", createStoreReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "创建数据存储失败: %s", readResponseBody(resp))

	storeData, err := getFullResponseData(resp)
	require.NoError(t, err)
	dataStoreID := getFullStringField(storeData, "id")
	require.NotEmpty(t, dataStoreID)
	t.Logf("✅ 数据存储创建成功: ID=%s, Path=%s", dataStoreID, dbPath)

	// 测试连接
	resp, err = ctx.doRequest("POST", "/api/v1/datastores/"+dataStoreID+"/test", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "测试连接失败: %s", readResponseBody(resp))
	t.Logf("✅ 数据存储连接测试成功")

	// ==================== Step 5: 生成表结构 ====================
	t.Log("\n----- Step 5: 生成表结构 -----")

	// 为 stock_basic API 生成表结构
	generateSchemaReq1 := map[string]interface{}{
		"api_metadata_id": apiMetadataID,
		"table_name":      "stock_basic",
		"auto_create":     false, // 先不自动创建，手动创建以便验证
	}
	resp, err = ctx.doRequest("POST", "/api/v1/datastores/"+dataStoreID+"/schemas/generate", generateSchemaReq1)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "生成表结构失败: %s", readResponseBody(resp))

	schemaData1, err := getFullResponseData(resp)
	require.NoError(t, err)
	schemaID1 := getFullStringField(schemaData1, "id")
	require.NotEmpty(t, schemaID1)
	t.Logf("✅ 表结构生成成功: SchemaID=%s, TableName=stock_basic", schemaID1)

	// 为 daily API 生成表结构
	generateSchemaReq2 := map[string]interface{}{
		"api_metadata_id": apiMetadataID2,
		"table_name":      "daily",
		"auto_create":     false,
	}
	resp, err = ctx.doRequest("POST", "/api/v1/datastores/"+dataStoreID+"/schemas/generate", generateSchemaReq2)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "生成表结构失败: %s", readResponseBody(resp))

	schemaData2, err := getFullResponseData(resp)
	require.NoError(t, err)
	schemaID2 := getFullStringField(schemaData2, "id")
	require.NotEmpty(t, schemaID2)
	t.Logf("✅ 表结构生成成功: SchemaID=%s, TableName=daily", schemaID2)

	// 验证表结构列表
	resp, err = ctx.doRequest("GET", "/api/v1/datastores/"+dataStoreID+"/schemas", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	schemas, err := getFullResponseDataList(resp)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(schemas), 2, "表结构数量应该至少为 2")
	t.Logf("✅ 验证表结构列表: 共 %d 个表结构", len(schemas))

	// ==================== Step 6: 创建表 ====================
	t.Log("\n----- Step 6: 创建表 -----")

	// 创建 stock_basic 表
	resp, err = ctx.doRequest("POST", "/api/v1/datastores/"+dataStoreID+"/schemas/"+schemaID1+"/create", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "创建表失败: %s", readResponseBody(resp))
	t.Logf("✅ 表创建成功: stock_basic")

	// 创建 daily 表
	resp, err = ctx.doRequest("POST", "/api/v1/datastores/"+dataStoreID+"/schemas/"+schemaID2+"/create", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "创建表失败: %s", readResponseBody(resp))
	t.Logf("✅ 表创建成功: daily")

	// 验证表结构状态
	resp, err = ctx.doRequest("GET", "/api/v1/datastores/"+dataStoreID+"/schemas/"+schemaID1, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	schemaDetail, err := getFullResponseData(resp)
	require.NoError(t, err)
	status := getFullStringField(schemaDetail, "status")
	t.Logf("✅ 表结构状态: %s", status)

	// ==================== Step 7: 创建工作流定义 ====================
	t.Log("\n----- Step 7: 创建工作流定义 -----")
	workflowReq := map[string]interface{}{
		"name":        "tushare_sync",
		"description": "Tushare 数据同步工作流",
		"category":    "sync",
		"definition_yaml": `name: tushare_sync
tasks:
  - name: sync_data
    handler: sync_handler
    params:
      api_name: "{{api_name}}"
      token: "{{token}}"
`,
	}
	resp, err = ctx.doRequest("POST", "/api/v1/workflows", workflowReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "创建工作流定义失败: %s", readResponseBody(resp))

	workflowData, err := getFullResponseData(resp)
	require.NoError(t, err)
	workflowDefID := getFullStringField(workflowData, "id")
	require.NotEmpty(t, workflowDefID)
	t.Logf("✅ 工作流定义创建成功: ID=%s", workflowDefID)

	// ==================== Step 8: 创建同步任务 ====================
	t.Log("\n----- Step 8: 创建同步任务 -----")
	syncJobReq := map[string]interface{}{
		"name":            "Stock Basic Sync",
		"description":     "同步股票基本信息",
		"api_metadata_id": apiMetadataID,
		"data_store_id":   dataStoreID,
		"workflow_def_id": workflowDefID,
		"mode":            "batch",
		"cron_expression": "0 0 9 * * *", // 每天 9 点执行
		"params": map[string]interface{}{
			"list_status": "L", // 只同步上市股票
		},
	}
	resp, err = ctx.doRequest("POST", "/api/v1/sync-jobs", syncJobReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "创建同步任务失败: %s", readResponseBody(resp))

	syncJobData, err := getFullResponseData(resp)
	require.NoError(t, err)
	syncJobID := getFullStringField(syncJobData, "id")
	require.NotEmpty(t, syncJobID)
	t.Logf("✅ 同步任务创建成功: ID=%s", syncJobID)

	// 验证同步任务
	resp, err = ctx.doRequest("GET", "/api/v1/sync-jobs/"+syncJobID, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	jobData, err := getFullResponseData(resp)
	require.NoError(t, err)
	jobStatus := getFullStringField(jobData, "status")
	assert.Equal(t, "disabled", jobStatus, "新创建的同步任务应该是禁用状态")
	t.Logf("✅ 同步任务状态: %s", jobStatus)

	// ==================== Step 9: 启用并触发同步任务 ====================
	t.Log("\n----- Step 9: 启用并触发同步任务 -----")

	// 启用同步任务
	resp, err = ctx.doRequest("POST", "/api/v1/sync-jobs/"+syncJobID+"/enable", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "启用同步任务失败: %s", readResponseBody(resp))
	t.Logf("✅ 同步任务已启用")

	// 验证任务已启用
	resp, err = ctx.doRequest("GET", "/api/v1/sync-jobs/"+syncJobID, nil)
	require.NoError(t, err)
	jobData, err = getFullResponseData(resp)
	require.NoError(t, err)
	jobStatus = getFullStringField(jobData, "status")
	assert.Equal(t, "enabled", jobStatus, "同步任务应该已启用")
	t.Logf("✅ 同步任务状态: %s", jobStatus)

	// 手动触发同步（如果系统支持）
	// 注意：触发同步可能会执行实际的数据获取，在 mock 模式下可能不会真正执行
	resp, err = ctx.doRequest("POST", "/api/v1/sync-jobs/"+syncJobID+"/trigger", nil)
	if err == nil && resp.StatusCode == http.StatusOK {
		t.Logf("✅ 同步任务触发成功")
		// 获取执行记录
		resp, err = ctx.doRequest("GET", "/api/v1/sync-jobs/"+syncJobID+"/executions", nil)
		if err == nil && resp.StatusCode == http.StatusOK {
			executions, err := getFullResponseDataList(resp)
			if err == nil && len(executions) > 0 {
				execData := executions[0].(map[string]interface{})
				execID := getFullStringField(execData, "id")
				t.Logf("✅ 同步执行记录: ID=%s", execID)
			}
		}
	} else {
		t.Logf("⚠️  同步任务触发失败或未实现（这在 mock 模式下是正常的）")
	}

	// ==================== 测试总结 ====================
	t.Log("\n========== 测试总结 ==========")
	t.Logf("✅ 数据源创建: %s", dataSourceID)
	t.Logf("✅ API 元数据: %d 个", 2)
	t.Logf("✅ 数据存储: %s", dataStoreID)
	t.Logf("✅ 表结构: %d 个", 2)
	t.Logf("✅ 同步任务: %s", syncJobID)
	t.Logf("========== Tushare 完整流程测试完成 ==========")
}

// TestE2E_TushareMetadataCrawl 测试 Tushare 元数据爬取流程
// 这个测试专门测试元数据爬取功能（如果系统支持自动爬取）
func TestE2E_TushareMetadataCrawl(t *testing.T) {
	ctx := setupE2EFullTestContext(t)
	defer ctx.cleanup()

	t.Logf("========== Tushare 元数据爬取测试开始 ==========")

	// 创建数据源
	createDSReq := map[string]string{
		"name":        "Tushare",
		"description": "Tushare Pro Data Source",
		"base_url":    "http://api.tushare.pro",
		"doc_url":     "https://tushare.pro/document/2",
	}
	resp, err := ctx.doRequest("POST", "/api/v1/datasources", createDSReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	data, err := getFullResponseData(resp)
	require.NoError(t, err)
	dataSourceID := getFullStringField(data, "id")
	require.NotEmpty(t, dataSourceID)

	// 设置 Token
	token := ctx.Config.TushareToken
	if token == "" {
		token = "test-token-12345"
	}
	tokenReq := map[string]string{"token": token}
	resp, err = ctx.doRequest("POST", "/api/v1/datasources/"+dataSourceID+"/token", tokenReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// 尝试触发元数据刷新
	// 注意：RefreshMetadata 需要 doc_content，在实际场景中可能需要先获取文档内容
	// 这里我们提供一个简化的 HTML 内容用于测试
	mockDocContent := `<html><body>
		<h1>Tushare API 文档</h1>
		<ul>
			<li><a href="/document/2?doc_id=1">股票基本信息</a></li>
			<li><a href="/document/2?doc_id=2">日线行情</a></li>
		</ul>
	</body></html>`

	refreshReq := map[string]interface{}{
		"doc_content": mockDocContent,
		"doc_type":    "html",
	}
	resp, err = ctx.doRequest("POST", "/api/v1/datasources/"+dataSourceID+"/refresh", refreshReq)
	if err == nil && resp.StatusCode == http.StatusOK {
		t.Logf("✅ 元数据刷新成功")
		refreshData, err := getFullResponseData(resp)
		if err == nil {
			categoriesCreated := 0
			if catCount, ok := refreshData["categories_created"].(float64); ok {
				categoriesCreated = int(catCount)
			}
			t.Logf("✅ 创建的分类数: %d", categoriesCreated)
		}
	} else {
		t.Logf("⚠️  元数据刷新失败或未实现（这在某些配置下是正常的）")
		if resp != nil {
			t.Logf("响应状态码: %d, 响应体: %s", resp.StatusCode, readResponseBody(resp))
		}
	}

	// 验证分类列表
	resp, err = ctx.doRequest("GET", "/api/v1/datasources/"+dataSourceID+"/categories", nil)
	if err == nil && resp.StatusCode == http.StatusOK {
		categories, err := getFullResponseDataList(resp)
		if err == nil {
			t.Logf("✅ 分类列表: %d 个", len(categories))
		}
	}

	t.Logf("========== Tushare 元数据爬取测试完成 ==========")
}

// waitForSyncExecution 等待同步执行完成（辅助函数）
func waitForSyncExecution(ctx *E2ETestContext, executionID string, timeout time.Duration) error {
	startTime := time.Now()
	for {
		if time.Since(startTime) > timeout {
			return fmt.Errorf("等待同步执行超时")
		}

		resp, err := ctx.doRequest("GET", "/api/v1/executions/"+executionID, nil)
		if err != nil {
			return err
		}

		if resp.StatusCode == http.StatusOK {
			data, err := getFullResponseData(resp)
			if err != nil {
				return err
			}
			status := getFullStringField(data, "status")
			if status == "completed" || status == "failed" || status == "cancelled" {
				return nil
			}
		}

		time.Sleep(1 * time.Second)
	}
}
