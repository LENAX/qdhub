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
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	httpapi "qdhub/internal/interfaces/http"
)

// MockMetadataService is a mock implementation of MetadataApplicationService.
type MockMetadataService struct {
	mock.Mock
}

func (m *MockMetadataService) CreateDataSource(ctx context.Context, req contracts.CreateDataSourceRequest) (*metadata.DataSource, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*metadata.DataSource), args.Error(1)
}

func (m *MockMetadataService) GetDataSource(ctx context.Context, id shared.ID) (*metadata.DataSource, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*metadata.DataSource), args.Error(1)
}

func (m *MockMetadataService) ListDataSources(ctx context.Context) ([]*metadata.DataSource, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*metadata.DataSource), args.Error(1)
}

func (m *MockMetadataService) ParseAndImportMetadata(ctx context.Context, req contracts.ParseMetadataRequest) (*contracts.ParseMetadataResult, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*contracts.ParseMetadataResult), args.Error(1)
}

func (m *MockMetadataService) CreateAPISyncStrategy(ctx context.Context, req contracts.CreateAPISyncStrategyRequest) (*metadata.APISyncStrategy, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*metadata.APISyncStrategy), args.Error(1)
}

func (m *MockMetadataService) GetAPISyncStrategy(ctx context.Context, req contracts.GetAPISyncStrategyRequest) (*metadata.APISyncStrategy, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*metadata.APISyncStrategy), args.Error(1)
}

func (m *MockMetadataService) UpdateAPISyncStrategy(ctx context.Context, id shared.ID, req contracts.UpdateAPISyncStrategyRequest) error {
	args := m.Called(ctx, id, req)
	return args.Error(0)
}

func (m *MockMetadataService) DeleteAPISyncStrategy(ctx context.Context, id shared.ID) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockMetadataService) ListAPISyncStrategies(ctx context.Context, dataSourceID shared.ID) ([]*metadata.APISyncStrategy, error) {
	args := m.Called(ctx, dataSourceID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*metadata.APISyncStrategy), args.Error(1)
}

func (m *MockMetadataService) ListAPIMetadata(ctx context.Context, dataSourceID shared.ID, req contracts.ListAPIMetadataRequest) (*contracts.ListAPIMetadataResponse, error) {
	args := m.Called(ctx, dataSourceID, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*contracts.ListAPIMetadataResponse), args.Error(1)
}

func (m *MockMetadataService) DeleteDataSource(ctx context.Context, id shared.ID) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockMetadataService) DeleteAPIMetadata(ctx context.Context, id shared.ID) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockMetadataService) SaveToken(ctx context.Context, req contracts.SaveTokenRequest) error {
	args := m.Called(ctx, req)
	return args.Error(0)
}

func (m *MockMetadataService) GetToken(ctx context.Context, dataSourceID shared.ID) (*metadata.Token, error) {
	args := m.Called(ctx, dataSourceID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*metadata.Token), args.Error(1)
}

func (m *MockMetadataService) ValidateDataSourceToken(ctx context.Context, dataSourceID shared.ID) (hasToken bool, valid bool, message string, err error) {
	args := m.Called(ctx, dataSourceID)
	return args.Bool(0), args.Bool(1), args.String(2), args.Error(3)
}

func (m *MockMetadataService) GetDataSourceConfig(ctx context.Context, dataSourceID shared.ID) (apiURL string, token string, err error) {
	args := m.Called(ctx, dataSourceID)
	return args.String(0), args.String(1), args.Error(2)
}

func (m *MockMetadataService) ListAPICategories(ctx context.Context, dataSourceID shared.ID, hasAPIsOnly bool) ([]metadata.APICategory, error) {
	args := m.Called(ctx, dataSourceID, hasAPIsOnly)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]metadata.APICategory), args.Error(1)
}

func setupMetadataRouter(mockSvc *MockMetadataService) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := httpapi.NewMetadataHandler(mockSvc)
	v1 := router.Group("/api/v1")
	handler.RegisterRoutes(v1)
	return router
}

func TestListDataSources(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	// Setup expectations
	dataSources := []*metadata.DataSource{
		metadata.NewDataSource("tushare", "Tushare Data", "https://api.tushare.pro", "https://tushare.pro/document/2"),
	}
	mockSvc.On("ListDataSources", mock.Anything).Return(dataSources, nil)

	// Make request
	req, _ := http.NewRequest("GET", "/api/v1/datasources", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assertions
	assert.Equal(t, http.StatusOK, w.Code)

	var resp httpapi.Response
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Code)
	assert.Equal(t, "success", resp.Message)

	mockSvc.AssertExpectations(t)
}

func TestCreateDataSource(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	// Setup expectations
	ds := metadata.NewDataSource("tushare", "Tushare Data", "https://api.tushare.pro", "https://tushare.pro/document/2")
	mockSvc.On("CreateDataSource", mock.Anything, mock.MatchedBy(func(req contracts.CreateDataSourceRequest) bool {
		return req.Name == "tushare" && req.Description == "Tushare Data"
	})).Return(ds, nil)

	// Make request
	body := map[string]string{
		"name":        "tushare",
		"description": "Tushare Data",
		"base_url":    "https://api.tushare.pro",
		"doc_url":     "https://tushare.pro/document/2",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/datasources", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assertions
	assert.Equal(t, http.StatusCreated, w.Code)

	var resp httpapi.Response
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Code)
	assert.Equal(t, "created", resp.Message)

	mockSvc.AssertExpectations(t)
}

func TestGetDataSource(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	// Setup expectations
	ds := metadata.NewDataSource("tushare", "Tushare Data", "https://api.tushare.pro", "https://tushare.pro/document/2")
	mockSvc.On("GetDataSource", mock.Anything, shared.ID("test-id")).Return(ds, nil)

	// Make request
	req, _ := http.NewRequest("GET", "/api/v1/datasources/test-id", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assertions
	assert.Equal(t, http.StatusOK, w.Code)

	var resp httpapi.Response
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Code)

	mockSvc.AssertExpectations(t)
}

func TestGetDataSourceNotFound(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	// Setup expectations
	mockSvc.On("GetDataSource", mock.Anything, shared.ID("not-found")).Return(nil, shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil))

	// Make request
	req, _ := http.NewRequest("GET", "/api/v1/datasources/not-found", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assertions
	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp httpapi.Response
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 404, resp.Code)

	mockSvc.AssertExpectations(t)
}


func TestCreateDataSourceInvalidBody(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	// Make request with invalid body (missing required field)
	body := map[string]string{
		"description": "No name provided",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/datasources", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assertions
	assert.Equal(t, http.StatusBadRequest, w.Code)

	var resp httpapi.Response
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 400, resp.Code)
}


// Note: Tests for deleted routes (ListAPIsByDataSource, GetCategories, etc.)
// have been removed as these routes are no longer part of the MetadataHandler.





func TestSetToken(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("SaveToken", mock.Anything, mock.MatchedBy(func(req contracts.SaveTokenRequest) bool {
		return req.TokenValue == "test-token"
	})).Return(nil)

	body := map[string]interface{}{
		"token": "test-token",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/datasources/ds-1/token", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetToken(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	token := metadata.NewToken(shared.ID("ds-1"), "test-token", nil)
	mockSvc.On("GetToken", mock.Anything, shared.ID("ds-1")).Return(token, nil)

	req, _ := http.NewRequest("GET", "/api/v1/datasources/ds-1/token", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestListAPIMetadata(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	api1 := metadata.NewAPIMetadata(shared.ID("ds-1"), "daily", "日线", "daily data", "/daily")
	api2 := metadata.NewAPIMetadata(shared.ID("ds-1"), "adj_factor", "复权因子", "adj factor", "/adj_factor")
	items := []*metadata.APIMetadata{api1, api2}
	mockSvc.On("ListAPIMetadata", mock.Anything, shared.ID("ds-1"), mock.MatchedBy(func(req contracts.ListAPIMetadataRequest) bool {
		return req.Page == 1 && req.PageSize == 20
	})).Return(&contracts.ListAPIMetadataResponse{Items: items, Total: 2}, nil)

	req, _ := http.NewRequest("GET", "/api/v1/datasources/ds-1/api-metadata", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var paged httpapi.PagedResponse
	err := json.Unmarshal(w.Body.Bytes(), &paged)
	assert.NoError(t, err)
	assert.Equal(t, 0, paged.Code)
	assert.Equal(t, int64(2), paged.Total)
	assert.Equal(t, 1, paged.Page)
	assert.Equal(t, 20, paged.Size)
	assert.NotNil(t, paged.Data)
	dataSlice, ok := paged.Data.([]interface{})
	assert.True(t, ok)
	assert.Len(t, dataSlice, 2)
	mockSvc.AssertExpectations(t)
}

func TestListAPIMetadataWithQueryParams(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	api1 := metadata.NewAPIMetadata(shared.ID("ds-1"), "daily", "日线", "daily data", "/daily")
	mockSvc.On("ListAPIMetadata", mock.Anything, shared.ID("ds-1"), mock.MatchedBy(func(req contracts.ListAPIMetadataRequest) bool {
		return req.Page == 2 && req.PageSize == 10 && req.Name == "daily"
	})).Return(&contracts.ListAPIMetadataResponse{Items: []*metadata.APIMetadata{api1}, Total: 1}, nil)

	req, _ := http.NewRequest("GET", "/api/v1/datasources/ds-1/api-metadata?page=2&page_size=10&name=daily", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var paged httpapi.PagedResponse
	err := json.Unmarshal(w.Body.Bytes(), &paged)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), paged.Total)
	assert.Equal(t, 2, paged.Page)
	assert.Equal(t, 10, paged.Size)
	mockSvc.AssertExpectations(t)
}

func TestListAPIMetadataNotFound(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("ListAPIMetadata", mock.Anything, shared.ID("ds-missing"), mock.Anything).
		Return(nil, shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil))

	req, _ := http.NewRequest("GET", "/api/v1/datasources/ds-missing/api-metadata", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestRefreshMetadata(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	result := &contracts.ParseMetadataResult{
		CategoriesCreated: 5,
		APIsCreated:       20,
		APIsUpdated:       0,
	}
	mockSvc.On("ParseAndImportMetadata", mock.Anything, mock.Anything).Return(result, nil)

	body := map[string]interface{}{
		"doc_content": "<html>...</html>",
		"doc_type":    "html",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/datasources/ds-1/refresh", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

// ==================== Error Scenario Tests ====================

func TestListDataSourcesError(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("ListDataSources", mock.Anything).Return(nil, errors.New("database error"))

	req, _ := http.NewRequest("GET", "/api/v1/datasources", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestCreateDataSourceServiceError(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("CreateDataSource", mock.Anything, mock.Anything).Return(nil, shared.NewDomainError(shared.ErrCodeConflict, "data source already exists", nil))

	body := map[string]string{
		"name":        "tushare",
		"description": "Tushare Data",
		"base_url":    "https://api.tushare.pro",
		"doc_url":     "https://tushare.pro/document/2",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/datasources", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
	mockSvc.AssertExpectations(t)
}




func TestRefreshMetadataWithOptionalBody(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	// Request body is now optional - workflow fetches documentation from data source's DocURL
	// Even with partial body, the service should be called
	mockSvc.On("ParseAndImportMetadata", mock.Anything, mock.MatchedBy(func(req contracts.ParseMetadataRequest) bool {
		return req.DataSourceID == "ds-1"
	})).Return(&contracts.ParseMetadataResult{
		CategoriesCreated: 0,
		APIsCreated:       0,
		APIsUpdated:       0,
	}, nil)

	body := map[string]interface{}{
		"doc_type": "html",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/datasources/ds-1/refresh", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Now expects success since body is optional (workflow fetches from DocURL)
	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestRefreshMetadataError(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("ParseAndImportMetadata", mock.Anything, mock.Anything).Return(nil, errors.New("parse error"))

	body := map[string]interface{}{
		"doc_content": "<html>...</html>",
		"doc_type":    "html",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/datasources/ds-1/refresh", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	mockSvc.AssertExpectations(t)
}









func TestSetTokenInvalidBody(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	// Missing required field
	body := map[string]interface{}{
		"expires_at": "2025-12-31",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/datasources/ds-1/token", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestSetTokenServiceError(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("SaveToken", mock.Anything, mock.Anything).Return(shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil))

	body := map[string]interface{}{
		"token": "test-token",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/datasources/ds-1/token", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetTokenError(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("GetToken", mock.Anything, shared.ID("ds-1")).Return(nil, shared.NewDomainError(shared.ErrCodeNotFound, "token not found", nil))

	req, _ := http.NewRequest("GET", "/api/v1/datasources/ds-1/token", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

