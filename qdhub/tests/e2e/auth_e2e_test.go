//go:build e2e
// +build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AuthTestContext holds authentication test context
type AuthTestContext struct {
	AccessToken  string
	RefreshToken string
	UserID       string
	Username     string
	Roles        []string
}

// TestAuthE2E_RegisterAndLogin tests user registration and login flow
func TestAuthE2E_RegisterAndLogin(t *testing.T) {
	ctx := setupServerE2EContext(t)

	// Register user
	registerReq := map[string]interface{}{
		"username": "e2e_test_user",
		"email":    "e2e_test@example.com",
		"password": "password123",
	}

	registerBody, err := json.Marshal(registerReq)
	require.NoError(t, err)

	registerResp, err := ctx.HTTPClient.Post(
		fmt.Sprintf("%s/api/v1/auth/register", ctx.BaseURL),
		"application/json",
		bytes.NewBuffer(registerBody),
	)
	require.NoError(t, err)
	defer registerResp.Body.Close()

	assert.Equal(t, http.StatusCreated, registerResp.StatusCode)

	var registerResult map[string]interface{}
	err = json.NewDecoder(registerResp.Body).Decode(&registerResult)
	require.NoError(t, err)
	assert.Equal(t, float64(0), registerResult["code"])

	// Login
	loginReq := map[string]interface{}{
		"username": "e2e_test_user",
		"password": "password123",
	}

	loginBody, err := json.Marshal(loginReq)
	require.NoError(t, err)

	loginResp, err := ctx.HTTPClient.Post(
		fmt.Sprintf("%s/api/v1/auth/login", ctx.BaseURL),
		"application/json",
		bytes.NewBuffer(loginBody),
	)
	require.NoError(t, err)
	defer loginResp.Body.Close()

	assert.Equal(t, http.StatusOK, loginResp.StatusCode)

	var loginResult map[string]interface{}
	err = json.NewDecoder(loginResp.Body).Decode(&loginResult)
	require.NoError(t, err)
	assert.Equal(t, float64(0), loginResult["code"])

	data := loginResult["data"].(map[string]interface{})
	assert.NotEmpty(t, data["access_token"])
	assert.NotEmpty(t, data["refresh_token"])
}

// TestAuthE2E_ProtectedEndpoints tests that protected endpoints require authentication
func TestAuthE2E_ProtectedEndpoints(t *testing.T) {
	ctx := setupServerE2EContext(t)

	// Try to access protected endpoint without token
	resp, err := ctx.HTTPClient.Get(fmt.Sprintf("%s/api/v1/sync-plans", ctx.BaseURL))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// TestAuthE2E_GetCurrentUser tests getting current user info
func TestAuthE2E_GetCurrentUser(t *testing.T) {
	ctx := setupServerE2EContext(t)
	authCtx := registerAndLogin(t, ctx, "current_user_test", "current_user@example.com", "password123")

	// Get current user
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/auth/me", ctx.BaseURL), nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", authCtx.AccessToken))

	resp, err := ctx.HTTPClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, float64(0), result["code"])

	data := result["data"].(map[string]interface{})
	assert.Equal(t, authCtx.Username, data["username"])
	assert.NotEmpty(t, data["roles"])
}

// TestAuthE2E_RefreshToken tests token refresh flow
func TestAuthE2E_RefreshToken(t *testing.T) {
	ctx := setupServerE2EContext(t)
	authCtx := registerAndLogin(t, ctx, "refresh_test_user", "refresh_test@example.com", "password123")

	// Refresh token
	refreshReq := map[string]interface{}{
		"refresh_token": authCtx.RefreshToken,
	}

	refreshBody, err := json.Marshal(refreshReq)
	require.NoError(t, err)

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/auth/refresh", ctx.BaseURL), bytes.NewBuffer(refreshBody))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", authCtx.AccessToken))

	resp, err := ctx.HTTPClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, float64(0), result["code"])

	data := result["data"].(map[string]interface{})
	assert.NotEmpty(t, data["access_token"])
	assert.NotEmpty(t, data["refresh_token"])
	assert.NotEqual(t, authCtx.AccessToken, data["access_token"]) // New token
}

// TestAuthE2E_UpdatePassword tests password update flow
func TestAuthE2E_UpdatePassword(t *testing.T) {
	ctx := setupServerE2EContext(t)
	authCtx := registerAndLogin(t, ctx, "password_test_user", "password_test@example.com", "old_password")

	// Update password
	updateReq := map[string]interface{}{
		"old_password": "old_password",
		"new_password": "new_password",
	}

	updateBody, err := json.Marshal(updateReq)
	require.NoError(t, err)

	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/api/v1/auth/password", ctx.BaseURL), bytes.NewBuffer(updateBody))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", authCtx.AccessToken))

	resp, err := ctx.HTTPClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Try to login with old password (should fail)
	loginReq := map[string]interface{}{
		"username": "password_test_user",
		"password": "old_password",
	}

	loginBody, err := json.Marshal(loginReq)
	require.NoError(t, err)

	loginResp, err := ctx.HTTPClient.Post(
		fmt.Sprintf("%s/api/v1/auth/login", ctx.BaseURL),
		"application/json",
		bytes.NewBuffer(loginBody),
	)
	require.NoError(t, err)
	defer loginResp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, loginResp.StatusCode)

	// Login with new password (should succeed)
	loginReq["password"] = "new_password"
	loginBody, err = json.Marshal(loginReq)
	require.NoError(t, err)

	loginResp, err = ctx.HTTPClient.Post(
		fmt.Sprintf("%s/api/v1/auth/login", ctx.BaseURL),
		"application/json",
		bytes.NewBuffer(loginBody),
	)
	require.NoError(t, err)
	defer loginResp.Body.Close()

	assert.Equal(t, http.StatusOK, loginResp.StatusCode)
}

// TestAuthE2E_RBAC tests RBAC permission enforcement
func TestAuthE2E_RBAC(t *testing.T) {
	ctx := setupServerE2EContext(t)

	// Register and login as viewer (default role)
	authCtx := registerAndLogin(t, ctx, "rbac_viewer", "rbac_viewer@example.com", "password123")

	// Viewer should be able to read sync-plans
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/sync-plans", ctx.BaseURL), nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", authCtx.AccessToken))

	resp, err := ctx.HTTPClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Viewer should NOT be able to create sync-plans
	createReq := map[string]interface{}{
		"name":         "test_plan",
		"description":  "test",
		"data_source_id": "test-id",
		"data_store_id":  "test-id",
		"selected_apis":  []string{},
	}

	createBody, err := json.Marshal(createReq)
	require.NoError(t, err)

	req, err = http.NewRequest("POST", fmt.Sprintf("%s/api/v1/sync-plans", ctx.BaseURL), bytes.NewBuffer(createBody))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", authCtx.AccessToken))

	resp, err = ctx.HTTPClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Should be forbidden (403) because viewer doesn't have write permission
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

// registerAndLogin is a helper function to register and login a user
func registerAndLogin(t *testing.T, ctx *ServerE2EContext, username, email, password string) *AuthTestContext {
	// Register
	registerReq := map[string]interface{}{
		"username": username,
		"email":    email,
		"password": password,
	}

	registerBody, err := json.Marshal(registerReq)
	require.NoError(t, err)

	registerResp, err := ctx.HTTPClient.Post(
		fmt.Sprintf("%s/api/v1/auth/register", ctx.BaseURL),
		"application/json",
		bytes.NewBuffer(registerBody),
	)
	require.NoError(t, err)
	defer registerResp.Body.Close()

	require.Equal(t, http.StatusCreated, registerResp.StatusCode)

	// Login
	loginReq := map[string]interface{}{
		"username": username,
		"password": password,
	}

	loginBody, err := json.Marshal(loginReq)
	require.NoError(t, err)

	loginResp, err := ctx.HTTPClient.Post(
		fmt.Sprintf("%s/api/v1/auth/login", ctx.BaseURL),
		"application/json",
		bytes.NewBuffer(loginBody),
	)
	require.NoError(t, err)
	defer loginResp.Body.Close()

	require.Equal(t, http.StatusOK, loginResp.StatusCode)

	var loginResult map[string]interface{}
	err = json.NewDecoder(loginResp.Body).Decode(&loginResult)
	require.NoError(t, err)

	data := loginResult["data"].(map[string]interface{})
	user := data["user"].(map[string]interface{})

	roles := []string{}
	if rolesInterface, ok := user["roles"].([]interface{}); ok {
		for _, r := range rolesInterface {
			roles = append(roles, r.(string))
		}
	}

	return &AuthTestContext{
		AccessToken:  data["access_token"].(string),
		RefreshToken: data["refresh_token"].(string),
		UserID:       user["id"].(string),
		Username:     user["username"].(string),
		Roles:        roles,
	}
}
