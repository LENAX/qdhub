//go:build e2e
// +build e2e

// Package e2e 提供端到端测试
// 本文件验证HTTP API层的完整功能
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/impl"
	"qdhub/internal/domain/auth"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	authinfra "qdhub/internal/infrastructure/auth"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/persistence/uow"
	"qdhub/internal/infrastructure/scheduler"
	"qdhub/internal/infrastructure/taskengine"
	httpapi "qdhub/internal/interfaces/http"
	"time"
)

// setupE2EHTTPServer 设置E2E测试所需的HTTP服务器
func setupE2EHTTPServer(t *testing.T, db *persistence.DB, taskEngine *engine.Engine) (*httptest.Server, func()) {
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
	metadataSvc := impl.NewMetadataApplicationService(dataSourceRepo, metadataRepo, nil, workflowExecutor, nil)
	dataStoreSvc := impl.NewDataStoreApplicationService(dataStoreRepo, dataSourceRepo, syncPlanRepo, workflowExecutor, nil)
	syncSvc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, planScheduler, dataSourceRepo, dataStoreRepo, workflowExecutor, dependencyResolver, taskEngineAdapter, uowImpl)
	workflowSvc := impl.NewWorkflowApplicationService(workflowRepo, taskEngineAdapter)

	// 创建认证相关组件
	userRepo := repository.NewUserRepository(db)
	passwordHasher := auth.NewBcryptPasswordHasher(0)
	jwtManager := authinfra.NewJWTManager("test_secret_key_123456789012345678901234567890", 1*time.Hour, 24*time.Hour)
	enforcer, err := authinfra.NewCasbinEnforcer(db.DB, persistence.DBTypeSQLite)
	require.NoError(t, err)
	err = authinfra.InitializeDefaultPolicies(enforcer)
	require.NoError(t, err)
	authSvc := impl.NewAuthApplicationService(userRepo, userRepo, passwordHasher, jwtManager)

	// 创建HTTP服务器
	config := httpapi.DefaultServerConfig()
	config.Mode = gin.TestMode
	server := httpapi.NewServer(config, authSvc, metadataSvc, dataStoreSvc, nil, syncSvc, workflowSvc, nil, jwtManager, enforcer, "")

	// 创建测试服务器
	ts := httptest.NewServer(server.Engine())

	cleanup := func() {
		ts.Close()
	}

	return ts, cleanup
}

// loadTushareToken 从环境变量加载Tushare Token
func loadTushareToken() string {
	token := os.Getenv("QDHUB_TUSHARE_TOKEN")
	if token == "" {
		token = os.Getenv("TUSHARE_TOKEN")
	}
	return strings.TrimSpace(token)
}

// TestE2E_HTTPAPI_Metadata 测试Metadata相关的HTTP API
func TestE2E_HTTPAPI_Metadata(t *testing.T) {
	// 设置测试环境
	db, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	taskEngine, cleanupEngine := setupTaskEngine(t, db)
	defer cleanupEngine()

	ts, cleanup := setupE2EHTTPServer(t, db, taskEngine)
	defer cleanup()

	// 从环境变量读取Tushare Token
	tushareToken := loadTushareToken()
	if tushareToken == "" {
		t.Skip("跳过：需要设置 QDHUB_TUSHARE_TOKEN 环境变量来运行此测试")
	}

	var tushareDataSourceID string

	t.Run("POST /api/v1/datasources - CreateDataSource (Tushare)", func(t *testing.T) {
		if tushareToken == "" {
			t.Skip("跳过：需要设置 QDHUB_TUSHARE_TOKEN 环境变量")
		}
		reqBody := map[string]interface{}{
			"name":        "Tushare",
			"description": "Tushare Pro Data Source",
			"base_url":    "http://api.tushare.pro",
			"doc_url":     "https://tushare.pro/document/2",
		}
		body, _ := json.Marshal(reqBody)

		resp, err := http.Post(ts.URL+"/api/v1/datasources", "application/json", bytes.NewBuffer(body))
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusCreated, resp.StatusCode)

		var result map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		require.NotNil(t, result["data"], "response data should not be nil")

		// data应该是DataSource对象（map）
		data, ok := result["data"].(map[string]interface{})
		require.True(t, ok, "data should be a map")

		// Go的JSON序列化使用字段名，所以是"ID"而不是"id"
		var dataSourceID string
		if id, exists := data["ID"]; exists && id != nil {
			dataSourceID, ok = id.(string)
			require.True(t, ok, "ID should be a string")
		} else if id, exists := data["id"]; exists && id != nil {
			// 也支持小写的"id"（如果将来有JSON标签）
			dataSourceID, ok = id.(string)
			require.True(t, ok, "id should be a string")
		} else {
			t.Fatalf("ID field not found in data, available keys: %v", getMapKeys(data))
		}
		require.NotEmpty(t, dataSourceID, "ID should not be empty")

		// 保存Token（handler期望字段名为"token"）
		tokenBody := map[string]interface{}{
			"token": tushareToken,
		}
		tokenReq, _ := json.Marshal(tokenBody)
		tokenResp, err := http.Post(ts.URL+"/api/v1/datasources/"+dataSourceID+"/token", "application/json", bytes.NewBuffer(tokenReq))
		require.NoError(t, err)
		defer tokenResp.Body.Close()
		assert.Equal(t, http.StatusOK, tokenResp.StatusCode)

		// 保存数据源ID供后续测试使用
		tushareDataSourceID = dataSourceID
	})

	t.Run("GET /api/v1/datasources - ListDataSources", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/api/v1/datasources")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var result map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.NotNil(t, result["data"])
	})

	t.Run("GET /api/v1/datasources/:id - GetDataSource", func(t *testing.T) {
		if tushareToken == "" {
			t.Skip("跳过：需要设置 QDHUB_TUSHARE_TOKEN 环境变量")
		}
		// 使用之前创建的Tushare数据源
		require.NotEmpty(t, tushareDataSourceID, "需要先创建数据源")

		// 获取数据源
		resp, err := http.Get(ts.URL + "/api/v1/datasources/" + tushareDataSourceID)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("POST /api/v1/datasources/:id/token - SaveToken", func(t *testing.T) {
		if tushareToken == "" {
			t.Skip("跳过：需要设置 QDHUB_TUSHARE_TOKEN 环境变量")
		}
		// 使用之前创建的Tushare数据源
		require.NotEmpty(t, tushareDataSourceID, "需要先创建数据源")

		// 保存Token（使用环境变量中的真实token，handler期望字段名为"token"）
		tokenBody := map[string]interface{}{
			"token": tushareToken,
		}
		tokenReq, _ := json.Marshal(tokenBody)

		resp, err := http.Post(ts.URL+"/api/v1/datasources/"+tushareDataSourceID+"/token", "application/json", bytes.NewBuffer(tokenReq))
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("GET /api/v1/datasources/:id/token - GetToken", func(t *testing.T) {
		if tushareToken == "" {
			t.Skip("跳过：需要设置 QDHUB_TUSHARE_TOKEN 环境变量")
		}
		// 使用之前创建的Tushare数据源
		require.NotEmpty(t, tushareDataSourceID, "需要先创建数据源")

		// 确保Token已保存（如果之前没有保存，handler期望字段名为"token"）
		tokenBody := map[string]interface{}{
			"token": tushareToken,
		}
		tokenReq, _ := json.Marshal(tokenBody)
		tokenSaveResp, err := http.Post(ts.URL+"/api/v1/datasources/"+tushareDataSourceID+"/token", "application/json", bytes.NewBuffer(tokenReq))
		require.NoError(t, err)
		tokenSaveResp.Body.Close()
		assert.Equal(t, http.StatusOK, tokenSaveResp.StatusCode)

		// 获取Token
		resp, err := http.Get(ts.URL + "/api/v1/datasources/" + tushareDataSourceID + "/token")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

// TestE2E_HTTPAPI_DataStore 测试DataStore相关的HTTP API
func TestE2E_HTTPAPI_DataStore(t *testing.T) {
	// 设置测试环境
	db, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	taskEngine, cleanupEngine := setupTaskEngine(t, db)
	defer cleanupEngine()

	ts, cleanup := setupE2EHTTPServer(t, db, taskEngine)
	defer cleanup()

	t.Run("POST /api/v1/datastores - CreateDataStore", func(t *testing.T) {
		reqBody := map[string]interface{}{
			"name":         "TestDataStore",
			"description":  "Test Data Store",
			"type":         "duckdb",
			"storage_path": "/tmp/test.duckdb",
		}
		body, _ := json.Marshal(reqBody)

		resp, err := http.Post(ts.URL+"/api/v1/datastores", "application/json", bytes.NewBuffer(body))
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusCreated, resp.StatusCode)
	})

	t.Run("GET /api/v1/datastores - ListDataStores", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/api/v1/datastores")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("GET /api/v1/datastores/:id - GetDataStore", func(t *testing.T) {
		// 先创建一个数据存储
		reqBody := map[string]interface{}{
			"name":         "TestDataStore2",
			"description":  "Test Data Store 2",
			"type":         "duckdb",
			"storage_path": "/tmp/test2.duckdb",
		}
		body, _ := json.Marshal(reqBody)

		createResp, err := http.Post(ts.URL+"/api/v1/datastores", "application/json", bytes.NewBuffer(body))
		require.NoError(t, err)
		defer createResp.Body.Close()

		var createResult map[string]interface{}
		err = json.NewDecoder(createResp.Body).Decode(&createResult)
		require.NoError(t, err)
		require.NotNil(t, createResult["data"])
		data, ok := createResult["data"].(map[string]interface{})
		require.True(t, ok, "data should be a map")

		// Go的JSON序列化使用字段名，所以是"ID"而不是"id"
		var id string
		if idVal, exists := data["ID"]; exists && idVal != nil {
			id, ok = idVal.(string)
			require.True(t, ok, "ID should be a string")
		} else if idVal, exists := data["id"]; exists && idVal != nil {
			id, ok = idVal.(string)
			require.True(t, ok, "id should be a string")
		} else {
			t.Fatalf("ID field not found in data, available keys: %v", getMapKeys(data))
		}
		require.NotEmpty(t, id, "ID should not be empty")

		// 获取数据存储
		resp, err := http.Get(ts.URL + "/api/v1/datastores/" + id)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

// TestE2E_HTTPAPI_SyncPlan 测试SyncPlan相关的HTTP API
func TestE2E_HTTPAPI_SyncPlan(t *testing.T) {
	// 设置测试环境
	db, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	taskEngine, cleanupEngine := setupTaskEngine(t, db)
	defer cleanupEngine()

	ts, cleanup := setupE2EHTTPServer(t, db, taskEngine)
	defer cleanup()

	// 从环境变量读取Tushare Token
	tushareToken := loadTushareToken()
	if tushareToken == "" {
		t.Skip("跳过：需要设置 QDHUB_TUSHARE_TOKEN 环境变量来运行此测试")
	}

	// 先创建Tushare数据源和数据存储
	dsReqBody := map[string]interface{}{
		"name":        "Tushare",
		"description": "Tushare Pro Data Source",
		"base_url":    "http://api.tushare.pro",
		"doc_url":     "https://tushare.pro/document/2",
	}
	dsBody, _ := json.Marshal(dsReqBody)
	dsResp, err := http.Post(ts.URL+"/api/v1/datasources", "application/json", bytes.NewBuffer(dsBody))
	require.NoError(t, err)
	var dsResult map[string]interface{}
	err = json.NewDecoder(dsResp.Body).Decode(&dsResult)
	require.NoError(t, err)
	dsResp.Body.Close()
	require.NotNil(t, dsResult["data"])
	dsData, ok := dsResult["data"].(map[string]interface{})
	require.True(t, ok, "data should be a map")

	// Go的JSON序列化使用字段名，所以是"ID"而不是"id"
	var dsID string
	if id, exists := dsData["ID"]; exists && id != nil {
		dsID, ok = id.(string)
		require.True(t, ok, "ID should be a string")
	} else if id, exists := dsData["id"]; exists && id != nil {
		dsID, ok = id.(string)
		require.True(t, ok, "id should be a string")
	} else {
		t.Fatalf("ID field not found in data, available keys: %v", getMapKeys(dsData))
	}
	require.NotEmpty(t, dsID, "ID should not be empty")

	dsStoreReqBody := map[string]interface{}{
		"name":         "TestDataStore",
		"description":  "Test Data Store",
		"type":         "duckdb",
		"storage_path": "/tmp/test.duckdb",
	}
	dsStoreBody, _ := json.Marshal(dsStoreReqBody)
	dsStoreResp, err := http.Post(ts.URL+"/api/v1/datastores", "application/json", bytes.NewBuffer(dsStoreBody))
	require.NoError(t, err)
	var dsStoreResult map[string]interface{}
	err = json.NewDecoder(dsStoreResp.Body).Decode(&dsStoreResult)
	require.NoError(t, err)
	dsStoreResp.Body.Close()
	require.NotNil(t, dsStoreResult["data"])
	dsStoreData, ok := dsStoreResult["data"].(map[string]interface{})
	require.True(t, ok, "data should be a map")

	// Go的JSON序列化使用字段名，所以是"ID"而不是"id"
	var dsStoreID string
	if id, exists := dsStoreData["ID"]; exists && id != nil {
		dsStoreID, ok = id.(string)
		require.True(t, ok, "ID should be a string")
	} else if id, exists := dsStoreData["id"]; exists && id != nil {
		dsStoreID, ok = id.(string)
		require.True(t, ok, "id should be a string")
	} else {
		t.Fatalf("ID field not found in data, available keys: %v", getMapKeys(dsStoreData))
	}
	require.NotEmpty(t, dsStoreID, "ID should not be empty")

	// 保存Token到数据源（handler期望字段名为"token"）
	tokenBody := map[string]interface{}{
		"token": tushareToken,
	}
	tokenReq, _ := json.Marshal(tokenBody)
	tokenResp, err := http.Post(ts.URL+"/api/v1/datasources/"+dsID+"/token", "application/json", bytes.NewBuffer(tokenReq))
	require.NoError(t, err)
	tokenResp.Body.Close()
	require.Equal(t, http.StatusOK, tokenResp.StatusCode, "保存Token失败")

	t.Run("POST /api/v1/sync-plans - CreateSyncPlan", func(t *testing.T) {
		reqBody := map[string]interface{}{
			"name":           "Test Sync Plan",
			"description":    "Test sync plan",
			"data_source_id": dsID,
			"data_store_id":  dsStoreID,
			"selected_apis":  []string{"daily", "stock_basic"},
		}
		body, _ := json.Marshal(reqBody)

		resp, err := http.Post(ts.URL+"/api/v1/sync-plans", "application/json", bytes.NewBuffer(body))
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusCreated, resp.StatusCode)
	})

	t.Run("GET /api/v1/sync-plans - ListSyncPlans", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/api/v1/sync-plans")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("GET /api/v1/sync-plans/:id - GetSyncPlan", func(t *testing.T) {
		// 先创建一个计划
		reqBody := map[string]interface{}{
			"name":           "Test Sync Plan 2",
			"description":    "Test sync plan 2",
			"data_source_id": dsID,
			"data_store_id":  dsStoreID,
			"selected_apis":  []string{"daily"},
		}
		body, _ := json.Marshal(reqBody)

		createResp, err := http.Post(ts.URL+"/api/v1/sync-plans", "application/json", bytes.NewBuffer(body))
		require.NoError(t, err)
		defer createResp.Body.Close()

		var createResult map[string]interface{}
		err = json.NewDecoder(createResp.Body).Decode(&createResult)
		require.NoError(t, err)
		require.NotNil(t, createResult["data"])
		data, ok := createResult["data"].(map[string]interface{})
		require.True(t, ok, "data should be a map")

		// Go的JSON序列化使用字段名，所以是"ID"而不是"id"
		var id string
		if idVal, exists := data["ID"]; exists && idVal != nil {
			id, ok = idVal.(string)
			require.True(t, ok, "ID should be a string")
		} else if idVal, exists := data["id"]; exists && idVal != nil {
			id, ok = idVal.(string)
			require.True(t, ok, "id should be a string")
		} else {
			t.Fatalf("ID field not found in data, available keys: %v", getMapKeys(data))
		}
		require.NotEmpty(t, id, "ID should not be empty")

		// 获取计划
		resp, err := http.Get(ts.URL + "/api/v1/sync-plans/" + id)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

// TestE2E_HTTPAPI_Workflow 测试Workflow相关的HTTP API
func TestE2E_HTTPAPI_Workflow(t *testing.T) {
	// 设置测试环境
	db, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	taskEngine, cleanupEngine := setupTaskEngine(t, db)
	defer cleanupEngine()

	ts, cleanup := setupE2EHTTPServer(t, db, taskEngine)
	defer cleanup()

	t.Run("GET /api/v1/instances - ListInstances", func(t *testing.T) {
		// 需要workflow_id参数
		resp, err := http.Get(ts.URL + "/api/v1/instances?workflow_id=" + shared.NewID().String())
		require.NoError(t, err)
		defer resp.Body.Close()

		// 可能会返回400（如果workflow_id不存在）或200
		assert.Contains(t, []int{http.StatusOK, http.StatusBadRequest}, resp.StatusCode)
	})
}

// TestE2E_HTTPAPI_DeletedRoutes 测试已删除的路由返回404
func TestE2E_HTTPAPI_DeletedRoutes(t *testing.T) {
	// 设置测试环境
	db, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	taskEngine, cleanupEngine := setupTaskEngine(t, db)
	defer cleanupEngine()

	ts, cleanup := setupE2EHTTPServer(t, db, taskEngine)
	defer cleanup()

	deletedRoutes := []struct {
		method string
		path   string
		name   string
	}{
		{"PUT", "/api/v1/datasources/test-id", "UpdateDataSource"},
		{"DELETE", "/api/v1/datasources/test-id", "DeleteDataSource"},
		{"PUT", "/api/v1/datastores/test-id", "UpdateDataStore"},
		{"DELETE", "/api/v1/datastores/test-id", "DeleteDataStore"},
		{"POST", "/api/v1/datastores/test-id/test", "TestConnection"},
		{"POST", "/api/v1/workflows", "CreateWorkflow"},
		{"PUT", "/api/v1/workflows/test-id", "UpdateWorkflow"},
		{"DELETE", "/api/v1/workflows/test-id", "DeleteWorkflow"},
		{"POST", "/api/v1/workflows/test-id/execute", "ExecuteWorkflow"},
		{"POST", "/api/v1/instances/test-id/pause", "PauseInstance"},
		{"POST", "/api/v1/instances/test-id/resume", "ResumeInstance"},
	}

	for _, route := range deletedRoutes {
		t.Run(route.name+" should return 404", func(t *testing.T) {
			var resp *http.Response
			var err error

			switch route.method {
			case "GET":
				resp, err = http.Get(ts.URL + route.path)
			case "POST":
				body, _ := json.Marshal(map[string]interface{}{})
				resp, err = http.Post(ts.URL+route.path, "application/json", bytes.NewBuffer(body))
			case "PUT":
				body, _ := json.Marshal(map[string]interface{}{})
				req, _ := http.NewRequest("PUT", ts.URL+route.path, bytes.NewBuffer(body))
				req.Header.Set("Content-Type", "application/json")
				resp, err = http.DefaultClient.Do(req)
			case "DELETE":
				req, _ := http.NewRequest("DELETE", ts.URL+route.path, nil)
				resp, err = http.DefaultClient.Do(req)
			}

			require.NoError(t, err)
			defer resp.Body.Close()

			// 已删除的路由应该返回404
			assert.Equal(t, http.StatusNotFound, resp.StatusCode, "Route %s %s should return 404", route.method, route.path)
		})
	}
}

// getMapKeys 获取map的所有键，用于调试
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
