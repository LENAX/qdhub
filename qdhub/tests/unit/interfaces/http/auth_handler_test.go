package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/shared"
	httpapi "qdhub/internal/interfaces/http"
)

// MockAuthService is a mock implementation of AuthApplicationService
type MockAuthService struct {
	mock.Mock
}

func (m *MockAuthService) Register(ctx context.Context, req contracts.RegisterRequest) (*contracts.RegisterResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*contracts.RegisterResponse), args.Error(1)
}

func (m *MockAuthService) Login(ctx context.Context, req contracts.LoginRequest) (*contracts.LoginResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*contracts.LoginResponse), args.Error(1)
}

func (m *MockAuthService) RefreshToken(ctx context.Context, refreshToken string) (*contracts.LoginResponse, error) {
	args := m.Called(ctx, refreshToken)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*contracts.LoginResponse), args.Error(1)
}

func (m *MockAuthService) GetCurrentUser(ctx context.Context, userID shared.ID) (*contracts.UserInfo, error) {
	args := m.Called(ctx, userID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*contracts.UserInfo), args.Error(1)
}

func (m *MockAuthService) UpdatePassword(ctx context.Context, userID shared.ID, req contracts.UpdatePasswordRequest) error {
	args := m.Called(ctx, userID, req)
	return args.Error(0)
}

func (m *MockAuthService) ListUsers(ctx context.Context, offset, limit int) ([]*contracts.UserInfo, int64, error) {
	args := m.Called(ctx, offset, limit)
	if args.Get(0) == nil {
		return nil, 0, args.Error(2)
	}
	return args.Get(0).([]*contracts.UserInfo), args.Get(1).(int64), args.Error(2)
}

func (m *MockAuthService) UpdateUserRoles(ctx context.Context, userID shared.ID, roles []string) error {
	args := m.Called(ctx, userID, roles)
	return args.Error(0)
}

func setupAuthRouter(mockSvc *MockAuthService) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(httpapi.Recovery())
	router.Use(httpapi.Logger())

	authHandler := httpapi.NewAuthHandler(mockSvc)
	v1 := router.Group("/api/v1")
	authHandler.RegisterRoutes(v1)

	return router
}

func TestAuthHandler_Register(t *testing.T) {
	mockSvc := new(MockAuthService)
	router := setupAuthRouter(mockSvc)

	t.Run("success", func(t *testing.T) {
		req := contracts.RegisterRequest{
			Username: "testuser",
			Email:    "test@example.com",
			Password: "password123",
		}

		resp := &contracts.RegisterResponse{
			UserID:   "user-id",
			Username: "testuser",
			Email:    "test@example.com",
		}

		mockSvc.On("Register", mock.Anything, req).Return(resp, nil)

		body, _ := json.Marshal(req)
		w := httptest.NewRecorder()
		httpReq, _ := http.NewRequest("POST", "/api/v1/auth/register", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, httpReq)

		assert.Equal(t, http.StatusCreated, w.Code)
		mockSvc.AssertExpectations(t)
	})

	t.Run("invalid request", func(t *testing.T) {
		body := []byte(`{"username": ""}`)
		w := httptest.NewRecorder()
		httpReq, _ := http.NewRequest("POST", "/api/v1/auth/register", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, httpReq)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestAuthHandler_Login(t *testing.T) {
	mockSvc := new(MockAuthService)
	router := setupAuthRouter(mockSvc)

	t.Run("success", func(t *testing.T) {
		req := contracts.LoginRequest{
			Username: "testuser",
			Password: "password123",
		}

		resp := &contracts.LoginResponse{
			AccessToken:  "access_token",
			RefreshToken: "refresh_token",
			User: contracts.UserInfo{
				ID:       "user-id",
				Username: "testuser",
				Email:    "test@example.com",
				Roles:    []string{"admin"},
			},
		}

		mockSvc.On("Login", mock.Anything, req).Return(resp, nil)

		body, _ := json.Marshal(req)
		w := httptest.NewRecorder()
		httpReq, _ := http.NewRequest("POST", "/api/v1/auth/login", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, httpReq)

		assert.Equal(t, http.StatusOK, w.Code)
		mockSvc.AssertExpectations(t)
	})

	t.Run("invalid credentials", func(t *testing.T) {
		req := contracts.LoginRequest{
			Username: "testuser",
			Password: "wrong_password",
		}

		mockSvc.On("Login", mock.Anything, req).Return(nil, assert.AnError)

		body, _ := json.Marshal(req)
		w := httptest.NewRecorder()
		httpReq, _ := http.NewRequest("POST", "/api/v1/auth/login", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, httpReq)

		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})
}
