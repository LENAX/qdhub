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

func (m *MockMetadataService) UpdateDataSource(ctx context.Context, id shared.ID, req contracts.UpdateDataSourceRequest) error {
	args := m.Called(ctx, id, req)
	return args.Error(0)
}

func (m *MockMetadataService) DeleteDataSource(ctx context.Context, id shared.ID) error {
	args := m.Called(ctx, id)
	return args.Error(0)
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

func (m *MockMetadataService) CreateAPIMetadata(ctx context.Context, req contracts.CreateAPIMetadataRequest) (*metadata.APIMetadata, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*metadata.APIMetadata), args.Error(1)
}

func (m *MockMetadataService) GetAPIMetadata(ctx context.Context, id shared.ID) (*metadata.APIMetadata, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*metadata.APIMetadata), args.Error(1)
}

func (m *MockMetadataService) UpdateAPIMetadata(ctx context.Context, id shared.ID, req contracts.UpdateAPIMetadataRequest) error {
	args := m.Called(ctx, id, req)
	return args.Error(0)
}

func (m *MockMetadataService) DeleteAPIMetadata(ctx context.Context, id shared.ID) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockMetadataService) ListAPIMetadataByDataSource(ctx context.Context, dataSourceID shared.ID) ([]*metadata.APIMetadata, error) {
	args := m.Called(ctx, dataSourceID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*metadata.APIMetadata), args.Error(1)
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

func (m *MockMetadataService) DeleteToken(ctx context.Context, dataSourceID shared.ID) error {
	args := m.Called(ctx, dataSourceID)
	return args.Error(0)
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

func TestDeleteDataSource(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	// Setup expectations
	mockSvc.On("DeleteDataSource", mock.Anything, shared.ID("test-id")).Return(nil)

	// Make request
	req, _ := http.NewRequest("DELETE", "/api/v1/datasources/test-id", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assertions
	assert.Equal(t, http.StatusNoContent, w.Code)

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

func TestUpdateDataSource(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("UpdateDataSource", mock.Anything, shared.ID("test-id"), mock.Anything).Return(nil)

	body := map[string]interface{}{
		"name":        "updated-name",
		"description": "Updated Description",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("PUT", "/api/v1/datasources/test-id", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestListAPIsByDataSource(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	apis := []*metadata.APIMetadata{
		metadata.NewAPIMetadata(shared.ID("ds-1"), "daily", "Daily Data", "Daily stock data", "/daily"),
	}
	mockSvc.On("ListAPIMetadataByDataSource", mock.Anything, shared.ID("ds-1")).Return(apis, nil)

	req, _ := http.NewRequest("GET", "/api/v1/datasources/ds-1/apis", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetCategories(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	apis := []*metadata.APIMetadata{}
	mockSvc.On("ListAPIMetadataByDataSource", mock.Anything, shared.ID("ds-1")).Return(apis, nil)

	req, _ := http.NewRequest("GET", "/api/v1/datasources/ds-1/categories", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetAPIMetadata(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	api := metadata.NewAPIMetadata(shared.ID("ds-1"), "daily", "Daily Data", "Daily stock data", "/daily")
	mockSvc.On("GetAPIMetadata", mock.Anything, shared.ID("api-1")).Return(api, nil)

	req, _ := http.NewRequest("GET", "/api/v1/apis/api-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestCreateAPIMetadata(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	api := metadata.NewAPIMetadata(shared.ID("ds-1"), "daily", "Daily Data", "Daily stock data", "/daily")
	mockSvc.On("CreateAPIMetadata", mock.Anything, mock.MatchedBy(func(req contracts.CreateAPIMetadataRequest) bool {
		return req.Name == "daily"
	})).Return(api, nil)

	body := map[string]interface{}{
		"data_source_id": "ds-1",
		"name":           "daily",
		"display_name":   "Daily Data",
		"description":    "Daily stock data",
		"endpoint":       "/daily",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/apis", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestUpdateAPIMetadata(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("UpdateAPIMetadata", mock.Anything, shared.ID("api-1"), mock.Anything).Return(nil)

	body := map[string]interface{}{
		"display_name": "Updated Daily Data",
		"description":  "Updated description",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("PUT", "/api/v1/apis/api-1", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestDeleteAPIMetadata(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("DeleteAPIMetadata", mock.Anything, shared.ID("api-1")).Return(nil)

	req, _ := http.NewRequest("DELETE", "/api/v1/apis/api-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
	mockSvc.AssertExpectations(t)
}

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

func TestDeleteToken(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("DeleteToken", mock.Anything, shared.ID("ds-1")).Return(nil)

	req, _ := http.NewRequest("DELETE", "/api/v1/datasources/ds-1/token", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
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

func TestUpdateDataSourceInvalidBody(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	req, _ := http.NewRequest("PUT", "/api/v1/datasources/test-id", bytes.NewBuffer([]byte("invalid json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestUpdateDataSourceServiceError(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("UpdateDataSource", mock.Anything, shared.ID("test-id"), mock.Anything).Return(shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil))

	body := map[string]interface{}{
		"name": "updated-name",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("PUT", "/api/v1/datasources/test-id", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestDeleteDataSourceError(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("DeleteDataSource", mock.Anything, shared.ID("test-id")).Return(shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil))

	req, _ := http.NewRequest("DELETE", "/api/v1/datasources/test-id", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestRefreshMetadataInvalidBody(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	// Missing required fields
	body := map[string]interface{}{
		"doc_type": "html",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/datasources/ds-1/refresh", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
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

func TestGetCategoriesError(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("ListAPIMetadataByDataSource", mock.Anything, shared.ID("ds-1")).Return(nil, shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil))

	req, _ := http.NewRequest("GET", "/api/v1/datasources/ds-1/categories", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetCategoriesWithCategoryID(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	catID := shared.ID("cat-1")
	apis := []*metadata.APIMetadata{
		{ID: "api-1", CategoryID: &catID, Name: "daily"},
	}
	mockSvc.On("ListAPIMetadataByDataSource", mock.Anything, shared.ID("ds-1")).Return(apis, nil)

	req, _ := http.NewRequest("GET", "/api/v1/datasources/ds-1/categories", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestListAPIsByDataSourceError(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("ListAPIMetadataByDataSource", mock.Anything, shared.ID("ds-1")).Return(nil, shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil))

	req, _ := http.NewRequest("GET", "/api/v1/datasources/ds-1/apis", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetAPIMetadataError(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("GetAPIMetadata", mock.Anything, shared.ID("api-1")).Return(nil, shared.NewDomainError(shared.ErrCodeNotFound, "api not found", nil))

	req, _ := http.NewRequest("GET", "/api/v1/apis/api-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestCreateAPIMetadataInvalidBody(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	// Missing required fields
	body := map[string]interface{}{
		"description": "Daily stock data",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/apis", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCreateAPIMetadataServiceError(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("CreateAPIMetadata", mock.Anything, mock.Anything).Return(nil, shared.NewDomainError(shared.ErrCodeConflict, "api already exists", nil))

	body := map[string]interface{}{
		"data_source_id": "ds-1",
		"name":           "daily",
		"display_name":   "Daily Data",
		"description":    "Daily stock data",
		"endpoint":       "/daily",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/apis", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestCreateAPIMetadataWithCategoryID(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	api := metadata.NewAPIMetadata(shared.ID("ds-1"), "daily", "Daily Data", "Daily stock data", "/daily")
	mockSvc.On("CreateAPIMetadata", mock.Anything, mock.MatchedBy(func(req contracts.CreateAPIMetadataRequest) bool {
		return req.CategoryID != nil && *req.CategoryID == shared.ID("cat-1")
	})).Return(api, nil)

	body := map[string]interface{}{
		"data_source_id": "ds-1",
		"category_id":    "cat-1",
		"name":           "daily",
		"display_name":   "Daily Data",
		"description":    "Daily stock data",
		"endpoint":       "/daily",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/apis", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestUpdateAPIMetadataInvalidBody(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	req, _ := http.NewRequest("PUT", "/api/v1/apis/api-1", bytes.NewBuffer([]byte("invalid json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestUpdateAPIMetadataServiceError(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("UpdateAPIMetadata", mock.Anything, shared.ID("api-1"), mock.Anything).Return(shared.NewDomainError(shared.ErrCodeNotFound, "api not found", nil))

	body := map[string]interface{}{
		"display_name": "Updated Daily Data",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("PUT", "/api/v1/apis/api-1", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestDeleteAPIMetadataError(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("DeleteAPIMetadata", mock.Anything, shared.ID("api-1")).Return(shared.NewDomainError(shared.ErrCodeNotFound, "api not found", nil))

	req, _ := http.NewRequest("DELETE", "/api/v1/apis/api-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
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

func TestDeleteTokenError(t *testing.T) {
	mockSvc := new(MockMetadataService)
	router := setupMetadataRouter(mockSvc)

	mockSvc.On("DeleteToken", mock.Anything, shared.ID("ds-1")).Return(shared.NewDomainError(shared.ErrCodeNotFound, "token not found", nil))

	req, _ := http.NewRequest("DELETE", "/api/v1/datasources/ds-1/token", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}
