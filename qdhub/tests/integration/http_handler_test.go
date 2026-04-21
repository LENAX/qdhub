//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/auth"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/domain/workflow"
	authinfra "qdhub/internal/infrastructure/auth"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/persistence/uow"
	"qdhub/internal/infrastructure/scheduler"
	httphandler "qdhub/internal/interfaces/http"
)

// ==================== Mock Implementations ====================

// MockHTTPWorkflowExecutor implements workflow.WorkflowExecutor for HTTP integration tests.
type MockHTTPWorkflowExecutor struct{}

func (m *MockHTTPWorkflowExecutor) ExecuteBuiltInWorkflow(ctx context.Context, name string, params map[string]interface{}) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockHTTPWorkflowExecutor) ExecuteMetadataCrawl(ctx context.Context, req workflow.MetadataCrawlRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockHTTPWorkflowExecutor) ExecuteCreateTables(ctx context.Context, req workflow.CreateTablesRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockHTTPWorkflowExecutor) ExecuteBatchDataSync(ctx context.Context, req workflow.BatchDataSyncRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockHTTPWorkflowExecutor) ExecuteRealtimeDataSync(ctx context.Context, req workflow.RealtimeDataSyncRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockHTTPWorkflowExecutor) ExecuteFromExecutionGraph(ctx context.Context, req workflow.ExecutionGraphSyncRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

// MockHTTPTaskEngineAdapter implements workflow.TaskEngineAdapter for HTTP integration tests.
type MockHTTPTaskEngineAdapter struct{}

func (m *MockHTTPTaskEngineAdapter) RegisterWorkflow(ctx context.Context, def *workflow.WorkflowDefinition) error {
	return nil
}

func (m *MockHTTPTaskEngineAdapter) UnregisterWorkflow(ctx context.Context, defID string) error {
	return nil
}

func (m *MockHTTPTaskEngineAdapter) SubmitWorkflow(ctx context.Context, def *workflow.WorkflowDefinition, params map[string]interface{}) (string, error) {
	return shared.NewID().String(), nil
}

func (m *MockHTTPTaskEngineAdapter) PauseInstance(ctx context.Context, instanceID string) error {
	return nil
}

func (m *MockHTTPTaskEngineAdapter) ResumeInstance(ctx context.Context, instanceID string) error {
	return nil
}

func (m *MockHTTPTaskEngineAdapter) CancelInstance(ctx context.Context, instanceID string) error {
	return nil
}

func (m *MockHTTPTaskEngineAdapter) GetInstanceStatus(ctx context.Context, instanceID string) (*workflow.WorkflowStatus, error) {
	return &workflow.WorkflowStatus{
		InstanceID: instanceID,
		Status:     "Running",
		Progress:   50.0,
	}, nil
}

func (m *MockHTTPTaskEngineAdapter) GetTaskInstances(ctx context.Context, engineInstanceID string) ([]*workflow.TaskInstance, error) {
	return []*workflow.TaskInstance{}, nil
}

func (m *MockHTTPTaskEngineAdapter) RetryTask(ctx context.Context, taskInstanceID string) error {
	return nil
}

func (m *MockHTTPTaskEngineAdapter) SubmitDynamicWorkflow(ctx context.Context, wf *workflow.Workflow) (string, error) {
	return shared.NewID().String(), nil
}

func (m *MockHTTPTaskEngineAdapter) GetFunctionRegistry() interface{} {
	return nil
}

// MockHTTPDependencyResolver is a mock dependency resolver for HTTP testing.
type MockHTTPDependencyResolver struct{}

func (m *MockHTTPDependencyResolver) Resolve(selectedAPIs []string, allAPIDependencies map[string][]sync.ParamDependency) (*sync.ExecutionGraph, []string, error) {
	graph := &sync.ExecutionGraph{
		Levels:      [][]string{selectedAPIs},
		TaskConfigs: make(map[string]*sync.TaskConfig),
	}
	for _, api := range selectedAPIs {
		graph.TaskConfigs[api] = &sync.TaskConfig{
			APIName:  api,
			SyncMode: sync.TaskSyncModeDirect,
		}
	}
	return graph, selectedAPIs, nil
}

// ==================== Test Setup ====================

// httpTestContext holds all components for HTTP integration tests.
type httpTestContext struct {
	db             *persistence.DB
	router         *gin.Engine
	metadataSvc    contracts.MetadataApplicationService
	dataStoreSvc   contracts.DataStoreApplicationService
	syncSvc        contracts.SyncApplicationService
	workflowSvc    contracts.WorkflowApplicationService
	dataSourceRepo *repository.DataSourceRepositoryImpl
	dsRepo         *repository.QuantDataStoreRepositoryImpl
	syncPlanRepo   *repository.SyncPlanRepositoryImpl
	workflowRepo   *repository.WorkflowDefinitionRepositoryImpl
	accessToken    string
}

// setAuth sets the JWT Authorization header on req for protected API calls.
func (c *httpTestContext) setAuth(req *http.Request) {
	if c.accessToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.accessToken)
	}
}

func setupHTTPTestContext(t *testing.T) (*httpTestContext, func()) {
	gin.SetMode(gin.TestMode)

	db, cleanup := setupIntegrationDB(t)

	// Auth: run migration and create auth components for protected routes
	runAuthMigration(t, db)
	userRepo := repository.NewUserRepository(db)
	passwordHasher := auth.NewBcryptPasswordHasher(0)
	jwtManager := authinfra.NewJWTManager("test_secret_key_123456789012345678901234567890", 1*time.Hour, 24*time.Hour)
	enforcer, err := authinfra.NewCasbinEnforcer(db.DB, persistence.DBTypeSQLite)
	if err != nil {
		cleanup()
		t.Fatalf("Failed to create casbin enforcer: %v", err)
	}
	err = authinfra.InitializeDefaultPolicies(enforcer)
	if err != nil {
		cleanup()
		t.Fatalf("Failed to initialize default policies: %v", err)
	}
	authSvc := impl.NewAuthApplicationService(userRepo, userRepo, passwordHasher, jwtManager)

	// Register test user, assign admin role for full API access, then login to get token
	ctx := context.Background()
	registerResp, err := authSvc.Register(ctx, contracts.RegisterRequest{
		Username: "integration_test",
		Email:    "integration@test.com",
		Password: "password123",
	})
	if err != nil {
		cleanup()
		t.Fatalf("Failed to register test user: %v", err)
	}
	if err := userRepo.AssignRole(ctx, shared.ID(registerResp.UserID), "admin"); err != nil {
		cleanup()
		t.Fatalf("Failed to assign admin role to test user: %v", err)
	}
	loginResp, err := authSvc.Login(ctx, contracts.LoginRequest{
		Username: "integration_test",
		Password: "password123",
	})
	if err != nil {
		cleanup()
		t.Fatalf("Failed to login test user: %v", err)
	}

	// Create repositories
	dataSourceRepo := repository.NewDataSourceRepository(db)
	dsRepo := repository.NewQuantDataStoreRepository(db)
	metadataRepo := repository.NewMetadataRepository(db)
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	workflowRepo, err := repository.NewWorkflowDefinitionRepository(db)
	if err != nil {
		cleanup()
		t.Fatalf("Failed to create workflow repository: %v", err)
	}

	// Create mock adapters
	workflowExecutor := &MockHTTPWorkflowExecutor{}
	taskEngineAdapter := &MockHTTPTaskEngineAdapter{}
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	dependencyResolver := &MockHTTPDependencyResolver{}
	uowImpl := uow.NewUnitOfWork(db)

	// Create application services
	metadataSvc := impl.NewMetadataApplicationService(dataSourceRepo, metadataRepo, nil, workflowExecutor, nil)
	dataStoreSvc := impl.NewDataStoreApplicationService(dsRepo, dataSourceRepo, syncPlanRepo, workflowExecutor, nil)
	syncSvc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, nil, dataSourceRepo, dsRepo, workflowExecutor, dependencyResolver, taskEngineAdapter, uowImpl, metadataRepo, nil, nil, "", nil)
	workflowSvc := impl.NewWorkflowApplicationService(workflowRepo, taskEngineAdapter)

	// Create HTTP server with auth
	config := httphandler.ServerConfig{
		Host: "127.0.0.1",
		Port: 0,
		Mode: gin.TestMode,
	}
	server := httphandler.NewServer(config, authSvc, metadataSvc, dataStoreSvc, nil, syncSvc, workflowSvc, nil, nil, nil, nil, nil, nil, jwtManager, enforcer, "")

	return &httpTestContext{
		db:             db,
		router:         server.Engine(),
		metadataSvc:    metadataSvc,
		dataStoreSvc:   dataStoreSvc,
		syncSvc:        syncSvc,
		workflowSvc:    workflowSvc,
		dataSourceRepo: dataSourceRepo,
		dsRepo:         dsRepo,
		syncPlanRepo:   syncPlanRepo,
		workflowRepo:   workflowRepo,
		accessToken:    loginResp.AccessToken,
	}, cleanup
}

// ==================== Health Check Tests ====================

func TestHTTP_HealthCheck(t *testing.T) {
	ctx, cleanup := setupHTTPTestContext(t)
	defer cleanup()

	req, _ := http.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	ctx.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, "healthy", response["status"])
}

// ==================== Data Source Handler Tests ====================

func TestHTTP_DataSource_CRUD(t *testing.T) {
	ctx, cleanup := setupHTTPTestContext(t)
	defer cleanup()

	// Create data source
	t.Run("CreateDataSource", func(t *testing.T) {
		body := map[string]interface{}{
			"name":        "Test DataSource",
			"description": "Test Description",
			"base_url":    "https://api.test.com",
			"doc_url":     "https://doc.test.com",
		}
		jsonBody, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", "/api/v1/datasources", bytes.NewBuffer(jsonBody))
		req.Header.Set("Content-Type", "application/json")
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusCreated, w.Code)

		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.NotEmpty(t, response["data"])
	})

	// List data sources
	t.Run("ListDataSources", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/datasources", nil)
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		require.NoError(t, err)
		data, ok := response["data"].([]interface{})
		require.True(t, ok)
		assert.Len(t, data, 1)
	})

	// Get data source by creating first
	t.Run("GetDataSource", func(t *testing.T) {
		// First create a new data source
		ds := metadata.NewDataSource("Get Test", "Desc", "https://api.get.com", "https://doc.get.com")
		err := ctx.dataSourceRepo.Create(ds)
		require.NoError(t, err)

		req, _ := http.NewRequest("GET", "/api/v1/datasources/"+ds.ID.String(), nil)
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})
}

// ==================== Token Handler Tests ====================

func TestHTTP_Token_CRUD(t *testing.T) {
	ctx, cleanup := setupHTTPTestContext(t)
	defer cleanup()

	// Create data source first
	ds := metadata.NewDataSource("Token Test DS", "Desc", "https://api.token.com", "https://doc.token.com")
	err := ctx.dataSourceRepo.Create(ds)
	require.NoError(t, err)

	// Save token
	t.Run("SaveToken", func(t *testing.T) {
		body := map[string]interface{}{
			"token": "test-token-value",
		}
		jsonBody, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", "/api/v1/datasources/"+ds.ID.String()+"/token", bytes.NewBuffer(jsonBody))
		req.Header.Set("Content-Type", "application/json")
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	// Get token
	t.Run("GetToken", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/datasources/"+ds.ID.String()+"/token", nil)
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})
}

// ==================== Data Store Handler Tests ====================

func TestHTTP_DataStore_CRUD(t *testing.T) {
	ctx, cleanup := setupHTTPTestContext(t)
	defer cleanup()

	var dataStoreID string

	// Create data store
	t.Run("CreateDataStore", func(t *testing.T) {
		body := map[string]interface{}{
			"name":         "Test DataStore",
			"description":  "Test Description",
			"type":         "duckdb",
			"storage_path": "/tmp/test.duckdb",
		}
		jsonBody, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", "/api/v1/datastores", bytes.NewBuffer(jsonBody))
		req.Header.Set("Content-Type", "application/json")
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusCreated, w.Code)

		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		require.NoError(t, err)

		data, ok := response["data"].(map[string]interface{})
		require.True(t, ok, "response data should be a map, got: %v", response)

		id, ok := data["ID"]
		if !ok {
			id = data["id"]
		}
		dataStoreID = fmt.Sprintf("%v", id)
		assert.NotEmpty(t, dataStoreID)
	})

	// Get data store
	t.Run("GetDataStore", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/datastores/"+dataStoreID, nil)
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	// List data stores
	t.Run("ListDataStores", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/datastores", nil)
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		require.NoError(t, err)
		data := response["data"].([]interface{})
		assert.Len(t, data, 1)
	})

	// Create tables for datasource
	t.Run("CreateTablesForDatasource", func(t *testing.T) {
		// Create a data source first
		ds := metadata.NewDataSource("Tables Test DS", "Desc", "https://api.tables.com", "https://doc.tables.com")
		err := ctx.dataSourceRepo.Create(ds)
		require.NoError(t, err)

		body := map[string]interface{}{
			"data_source_id": ds.ID.String(),
		}
		jsonBody, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", "/api/v1/datastores/"+dataStoreID+"/create-tables", bytes.NewBuffer(jsonBody))
		req.Header.Set("Content-Type", "application/json")
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusCreated, w.Code)

		var response map[string]interface{}
		err = json.Unmarshal(w.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.NotEmpty(t, response["data"])
	})
}

// ==================== Sync Plan Handler Tests ====================

func TestHTTP_SyncPlan_CRUD(t *testing.T) {
	ctx, cleanup := setupHTTPTestContext(t)
	defer cleanup()

	// Create prerequisite entities
	ds := metadata.NewDataSource("Sync DS", "Desc", "https://api.sync.com", "https://doc.sync.com")
	err := ctx.dataSourceRepo.Create(ds)
	require.NoError(t, err)

	dataStore := datastore.NewQuantDataStore("Sync Store", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/sync.duckdb")
	err = ctx.dsRepo.Create(dataStore)
	require.NoError(t, err)

	var syncPlanID string

	// Create sync plan
	t.Run("CreateSyncPlan", func(t *testing.T) {
		body := map[string]interface{}{
			"name":           "Test Sync Plan",
			"description":    "Test Description",
			"data_source_id": ds.ID.String(),
			"data_store_id":  dataStore.ID.String(),
			"selected_apis":  []string{"daily", "stock_basic"},
		}
		jsonBody, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", "/api/v1/sync-plans", bytes.NewBuffer(jsonBody))
		req.Header.Set("Content-Type", "application/json")
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		if !assert.Equal(t, http.StatusCreated, w.Code) {
			t.Logf("Response: %s", w.Body.String())
			return
		}

		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		require.NoError(t, err)

		data, ok := response["data"].(map[string]interface{})
		if !ok {
			t.Fatalf("Expected data map, got: %v", response)
		}

		id, ok := data["ID"]
		if !ok {
			id = data["id"]
		}
		syncPlanID = fmt.Sprintf("%v", id)
		assert.NotEmpty(t, syncPlanID)
	})

	// Get sync plan
	t.Run("GetSyncPlan", func(t *testing.T) {
		if syncPlanID == "" {
			t.Skip("Skipping - no sync plan created")
		}
		req, _ := http.NewRequest("GET", "/api/v1/sync-plans/"+syncPlanID, nil)
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	// List sync plans
	t.Run("ListSyncPlans", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/sync-plans", nil)
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		if !assert.Equal(t, http.StatusOK, w.Code) {
			t.Logf("Response: %s", w.Body.String())
			return
		}

		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		require.NoError(t, err)
		data, ok := response["data"].([]interface{})
		if !ok {
			t.Fatalf("Expected data array, got: %v", response)
		}
		assert.Len(t, data, 1)
	})

	// Update sync plan
	t.Run("UpdateSyncPlan", func(t *testing.T) {
		body := map[string]interface{}{
			"name": "Updated Sync Plan",
		}
		jsonBody, _ := json.Marshal(body)
		req, _ := http.NewRequest("PUT", "/api/v1/sync-plans/"+syncPlanID, bytes.NewBuffer(jsonBody))
		req.Header.Set("Content-Type", "application/json")
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	// Resolve sync plan dependencies (required before enabling)
	t.Run("ResolveSyncPlan", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/api/v1/sync-plans/"+syncPlanID+"/resolve", nil)
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		if !assert.Equal(t, http.StatusOK, w.Code) {
			t.Logf("Response: %s", w.Body.String())
		}
	})

	// Enable sync plan
	t.Run("EnableSyncPlan", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/api/v1/sync-plans/"+syncPlanID+"/enable", nil)
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		if !assert.Equal(t, http.StatusOK, w.Code) {
			t.Logf("Response: %s", w.Body.String())
		}
	})

	// Disable sync plan
	t.Run("DisableSyncPlan", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/api/v1/sync-plans/"+syncPlanID+"/disable", nil)
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	// Delete sync plan
	t.Run("DeleteSyncPlan", func(t *testing.T) {
		req, _ := http.NewRequest("DELETE", "/api/v1/sync-plans/"+syncPlanID, nil)
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNoContent, w.Code)
	})
}

// ==================== Error Handling Tests ====================

func TestHTTP_ErrorHandling(t *testing.T) {
	ctx, cleanup := setupHTTPTestContext(t)
	defer cleanup()

	// Not found error
	t.Run("NotFound", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/datasources/nonexistent-id", nil)
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	// Invalid request body
	t.Run("InvalidRequestBody", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/api/v1/datasources", bytes.NewBuffer([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	// Missing required field
	t.Run("MissingRequiredField", func(t *testing.T) {
		body := map[string]interface{}{
			// Missing name field
			"description": "Test",
		}
		jsonBody, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", "/api/v1/datasources", bytes.NewBuffer(jsonBody))
		req.Header.Set("Content-Type", "application/json")
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)

		// Should fail validation or creation
		assert.True(t, w.Code == http.StatusBadRequest || w.Code == http.StatusInternalServerError)
	})
}

// ==================== End-to-End Flow Tests ====================

func TestHTTP_E2E_DataSyncFlow(t *testing.T) {
	ctx, cleanup := setupHTTPTestContext(t)
	defer cleanup()

	// Helper function to extract ID from response
	extractID := func(t *testing.T, body []byte) string {
		var response map[string]interface{}
		json.Unmarshal(body, &response)
		data, ok := response["data"].(map[string]interface{})
		if !ok {
			t.Fatalf("Expected data map, got: %v", response)
		}
		id, ok := data["ID"]
		if !ok {
			id = data["id"]
		}
		return fmt.Sprintf("%v", id)
	}

	// 1. Create a data source
	var dataSourceID string
	{
		body := map[string]interface{}{
			"name":        "E2E DataSource",
			"description": "E2E Test",
			"base_url":    "https://api.e2e.com",
			"doc_url":     "https://doc.e2e.com",
		}
		jsonBody, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", "/api/v1/datasources", bytes.NewBuffer(jsonBody))
		req.Header.Set("Content-Type", "application/json")
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)
		require.Equal(t, http.StatusCreated, w.Code)
		dataSourceID = extractID(t, w.Body.Bytes())
	}

	// 2. Create data store
	var dataStoreID string
	{
		body := map[string]interface{}{
			"name":         "E2E DataStore",
			"description":  "E2E Test",
			"type":         "duckdb",
			"storage_path": "/tmp/e2e.duckdb",
		}
		jsonBody, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", "/api/v1/datastores", bytes.NewBuffer(jsonBody))
		req.Header.Set("Content-Type", "application/json")
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)
		require.Equal(t, http.StatusCreated, w.Code)
		dataStoreID = extractID(t, w.Body.Bytes())
	}

	// 3. Create sync plan
	var syncPlanID string
	{
		body := map[string]interface{}{
			"name":           "E2E Sync Plan",
			"description":    "E2E Test",
			"data_source_id": dataSourceID,
			"data_store_id":  dataStoreID,
			"selected_apis":  []string{"daily"},
		}
		jsonBody, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", "/api/v1/sync-plans", bytes.NewBuffer(jsonBody))
		req.Header.Set("Content-Type", "application/json")
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)
		require.Equal(t, http.StatusCreated, w.Code)
		syncPlanID = extractID(t, w.Body.Bytes())
	}

	// Verify all entities exist
	{
		// Verify data source
		req, _ := http.NewRequest("GET", "/api/v1/datasources/"+dataSourceID, nil)
		ctx.setAuth(req)
		w := httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// Verify data store
		req, _ = http.NewRequest("GET", "/api/v1/datastores/"+dataStoreID, nil)
		ctx.setAuth(req)
		w = httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// Verify sync plan
		req, _ = http.NewRequest("GET", "/api/v1/sync-plans/"+syncPlanID, nil)
		ctx.setAuth(req)
		w = httptest.NewRecorder()
		ctx.router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	}
}

// ==================== Mock Job Scheduler (unused variable fix) ====================

// MockHTTPJobScheduler is a mock implementation of sync.JobScheduler.
type MockHTTPJobScheduler struct {
	scheduledJobs map[string]string
}

func NewMockHTTPJobScheduler() *MockHTTPJobScheduler {
	return &MockHTTPJobScheduler{
		scheduledJobs: make(map[string]string),
	}
}

func (m *MockHTTPJobScheduler) Start() {}

func (m *MockHTTPJobScheduler) Stop() context.Context {
	return context.Background()
}

func (m *MockHTTPJobScheduler) ScheduleJob(jobID string, cronExpr string) error {
	m.scheduledJobs[jobID] = cronExpr
	return nil
}

func (m *MockHTTPJobScheduler) UnscheduleJob(jobID string) {
	delete(m.scheduledJobs, jobID)
}

func (m *MockHTTPJobScheduler) IsScheduled(jobID string) bool {
	_, exists := m.scheduledJobs[jobID]
	return exists
}

func (m *MockHTTPJobScheduler) GetNextRunTime(jobID string) *time.Time {
	return nil
}
