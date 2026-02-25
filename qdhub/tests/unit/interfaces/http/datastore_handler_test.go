package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/shared"
	httpapi "qdhub/internal/interfaces/http"
)

// MockDataStoreService is a mock implementation of DataStoreApplicationService.
type MockDataStoreService struct {
	mock.Mock
}

func (m *MockDataStoreService) CreateDataStore(ctx context.Context, req contracts.CreateDataStoreRequest) (*datastore.QuantDataStore, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*datastore.QuantDataStore), args.Error(1)
}

func (m *MockDataStoreService) GetDataStore(ctx context.Context, id shared.ID) (*datastore.QuantDataStore, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*datastore.QuantDataStore), args.Error(1)
}

func (m *MockDataStoreService) ListDataStores(ctx context.Context) ([]*datastore.QuantDataStore, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*datastore.QuantDataStore), args.Error(1)
}

func (m *MockDataStoreService) UpdateDataStore(ctx context.Context, id shared.ID, req contracts.UpdateDataStoreRequest) (*datastore.QuantDataStore, error) {
	args := m.Called(ctx, id, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*datastore.QuantDataStore), args.Error(1)
}
func (m *MockDataStoreService) DeleteDataStore(ctx context.Context, id shared.ID) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}
func (m *MockDataStoreService) ValidateDataStore(ctx context.Context, id shared.ID) (*contracts.ValidateDataStoreResult, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*contracts.ValidateDataStoreResult), args.Error(1)
}
func (m *MockDataStoreService) CreateTablesForDatasource(ctx context.Context, req contracts.CreateTablesForDatasourceRequest) (shared.ID, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(shared.ID), args.Error(1)
}

func setupDataStoreRouter(mockSvc *MockDataStoreService) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := httpapi.NewDataStoreHandler(mockSvc)
	v1 := router.Group("/api/v1")
	handler.RegisterRoutes(v1)
	return router
}

func TestListDataStores(t *testing.T) {
	mockSvc := new(MockDataStoreService)
	router := setupDataStoreRouter(mockSvc)

	stores := []*datastore.QuantDataStore{
		datastore.NewQuantDataStore("test-store", "Test Store", datastore.DataStoreTypeDuckDB, "", "./data.duckdb"),
	}
	mockSvc.On("ListDataStores", mock.Anything).Return(stores, nil)

	req, _ := http.NewRequest("GET", "/api/v1/datastores", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestCreateDataStore(t *testing.T) {
	mockSvc := new(MockDataStoreService)
	router := setupDataStoreRouter(mockSvc)

	ds := datastore.NewQuantDataStore("test-store", "Test Store", datastore.DataStoreTypeDuckDB, "", "./data.duckdb")
	mockSvc.On("CreateDataStore", mock.Anything, mock.MatchedBy(func(req contracts.CreateDataStoreRequest) bool {
		return req.Name == "test-store"
	})).Return(ds, nil)

	body := map[string]interface{}{
		"name":         "test-store",
		"description":  "Test Store",
		"type":         "duckdb",
		"storage_path": "./data.duckdb",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/datastores", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetDataStore(t *testing.T) {
	mockSvc := new(MockDataStoreService)
	router := setupDataStoreRouter(mockSvc)

	ds := datastore.NewQuantDataStore("test-store", "Test Store", datastore.DataStoreTypeDuckDB, "", "./data.duckdb")
	mockSvc.On("GetDataStore", mock.Anything, shared.ID("test-id")).Return(ds, nil)

	req, _ := http.NewRequest("GET", "/api/v1/datastores/test-id", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

// Note: Tests for deleted methods (ListSchemas, GetSchema, UpdateSchema, etc.)
// have been removed as these methods are no longer part of the DataStoreApplicationService interface.

func TestGetDataStoreNotFound(t *testing.T) {
	mockSvc := new(MockDataStoreService)
	router := setupDataStoreRouter(mockSvc)

	mockSvc.On("GetDataStore", mock.Anything, shared.ID("not-found")).Return(nil, shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil))

	req, _ := http.NewRequest("GET", "/api/v1/datastores/not-found", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

// ==================== Error Scenario Tests ====================

func TestListDataStoresError(t *testing.T) {
	mockSvc := new(MockDataStoreService)
	router := setupDataStoreRouter(mockSvc)

	mockSvc.On("ListDataStores", mock.Anything).Return(nil, errors.New("database error"))

	req, _ := http.NewRequest("GET", "/api/v1/datastores", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestCreateDataStoreInvalidBody(t *testing.T) {
	mockSvc := new(MockDataStoreService)
	router := setupDataStoreRouter(mockSvc)

	// Missing required fields
	body := map[string]interface{}{
		"description": "Test Store",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/datastores", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCreateDataStoreServiceError(t *testing.T) {
	mockSvc := new(MockDataStoreService)
	router := setupDataStoreRouter(mockSvc)

	mockSvc.On("CreateDataStore", mock.Anything, mock.Anything).Return(nil, shared.NewDomainError(shared.ErrCodeConflict, "data store already exists", nil))

	body := map[string]interface{}{
		"name":         "test-store",
		"description":  "Test Store",
		"type":         "duckdb",
		"storage_path": "./data.duckdb",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/datastores", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
	mockSvc.AssertExpectations(t)
}
