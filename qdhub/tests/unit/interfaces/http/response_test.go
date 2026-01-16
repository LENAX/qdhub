package http_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"qdhub/internal/domain/shared"
	httpapi "qdhub/internal/interfaces/http"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func TestSuccess(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	data := map[string]string{"key": "value"}
	httpapi.Success(c, data)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp httpapi.Response
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Code)
	assert.Equal(t, "success", resp.Message)
	assert.NotNil(t, resp.Data)
}

func TestCreated(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	data := map[string]string{"id": "123"}
	httpapi.Created(c, data)

	assert.Equal(t, http.StatusCreated, w.Code)

	var resp httpapi.Response
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Code)
	assert.Equal(t, "created", resp.Message)
}

func TestNoContent(t *testing.T) {
	// Use a real router to properly test NoContent
	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		httpapi.NoContent(c)
	})

	req, _ := http.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Empty(t, w.Body.String())
}

func TestBadRequest(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	httpapi.BadRequest(c, "invalid input")

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var resp httpapi.Response
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 400, resp.Code)
	assert.Equal(t, "invalid input", resp.Message)
}

func TestNotFound(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	httpapi.NotFound(c, "resource not found")

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp httpapi.Response
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 404, resp.Code)
}

func TestInternalError(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	httpapi.InternalError(c, "something went wrong")

	assert.Equal(t, http.StatusInternalServerError, w.Code)

	var resp httpapi.Response
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 500, resp.Code)
}

func TestPaged(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	data := []string{"item1", "item2"}
	httpapi.Paged(c, data, 100, 1, 10)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp httpapi.PagedResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Code)
	assert.Equal(t, int64(100), resp.Total)
	assert.Equal(t, 1, resp.Page)
	assert.Equal(t, 10, resp.Size)
}

func TestHandleErrorNotFound(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	err := shared.NewDomainError(shared.ErrCodeNotFound, "not found", nil)
	httpapi.HandleError(c, err)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleErrorValidation(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	err := shared.NewDomainError(shared.ErrCodeValidation, "validation failed", nil)
	httpapi.HandleError(c, err)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleErrorConflict(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	err := shared.NewDomainError(shared.ErrCodeConflict, "conflict", nil)
	httpapi.HandleError(c, err)

	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestHandleErrorGeneric(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	err := errors.New("generic error")
	httpapi.HandleError(c, err)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleErrorNil(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	httpapi.HandleError(c, nil)

	// Should not write anything
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Body.String())
}
