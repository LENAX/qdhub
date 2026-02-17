package auth_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/domain/shared"
	authinfra "qdhub/internal/infrastructure/auth"
)

func TestJWTManager_GenerateAccessToken(t *testing.T) {
	secretKey := "test_secret_key_123456789012345678901234567890"
	manager := authinfra.NewJWTManager(secretKey, 1*time.Hour, 24*time.Hour)

	userID := shared.NewID()
	username := "testuser"
	roles := []string{"admin", "operator"}

	token, err := manager.GenerateAccessToken(userID, username, roles)
	require.NoError(t, err)
	assert.NotEmpty(t, token)
}

func TestJWTManager_ValidateToken(t *testing.T) {
	secretKey := "test_secret_key_123456789012345678901234567890"
	manager := authinfra.NewJWTManager(secretKey, 1*time.Hour, 24*time.Hour)

	userID := shared.NewID()
	username := "testuser"
	roles := []string{"admin"}

	token, err := manager.GenerateAccessToken(userID, username, roles)
	require.NoError(t, err)

	claims, err := manager.ValidateToken(token)
	require.NoError(t, err)
	assert.Equal(t, userID.String(), claims.UserID)
	assert.Equal(t, username, claims.Username)
	assert.Equal(t, roles, claims.Roles)
}

func TestJWTManager_ValidateToken_Invalid(t *testing.T) {
	secretKey := "test_secret_key_123456789012345678901234567890"
	manager := authinfra.NewJWTManager(secretKey, 1*time.Hour, 24*time.Hour)

	_, err := manager.ValidateToken("invalid_token")
	assert.Error(t, err)
}

func TestJWTManager_GenerateRefreshToken(t *testing.T) {
	secretKey := "test_secret_key_123456789012345678901234567890"
	manager := authinfra.NewJWTManager(secretKey, 1*time.Hour, 24*time.Hour)

	userID := shared.NewID()

	token, err := manager.GenerateRefreshToken(userID)
	require.NoError(t, err)
	assert.NotEmpty(t, token)
}

func TestJWTManager_ValidateRefreshToken(t *testing.T) {
	secretKey := "test_secret_key_123456789012345678901234567890"
	manager := authinfra.NewJWTManager(secretKey, 1*time.Hour, 24*time.Hour)

	userID := shared.NewID()

	token, err := manager.GenerateRefreshToken(userID)
	require.NoError(t, err)

	validatedID, err := manager.ValidateRefreshToken(token)
	require.NoError(t, err)
	assert.Equal(t, userID, validatedID)
}

func TestJWTManager_ValidateRefreshToken_Invalid(t *testing.T) {
	secretKey := "test_secret_key_123456789012345678901234567890"
	manager := authinfra.NewJWTManager(secretKey, 1*time.Hour, 24*time.Hour)

	_, err := manager.ValidateRefreshToken("invalid_token")
	assert.Error(t, err)
}

func TestJWTManager_TokenExpiration(t *testing.T) {
	secretKey := "test_secret_key_123456789012345678901234567890"
	manager := authinfra.NewJWTManager(secretKey, 1*time.Second, 1*time.Second)

	userID := shared.NewID()
	username := "testuser"
	roles := []string{"admin"}

	token, err := manager.GenerateAccessToken(userID, username, roles)
	require.NoError(t, err)

	// Wait for token to expire
	time.Sleep(2 * time.Second)

	_, err = manager.ValidateToken(token)
	assert.Error(t, err)
}
