//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/auth"
	"qdhub/internal/domain/shared"
	authinfra "qdhub/internal/infrastructure/auth"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
)

func setupAuthIntegrationTest(t *testing.T) (*impl.AuthApplicationServiceImpl, *persistence.DB, func()) {
	// Create in-memory SQLite database
	db, err := persistence.NewDB(":memory:")
	require.NoError(t, err)

	// Run auth migration
	migrationSQL := `
	CREATE TABLE IF NOT EXISTS users (
		id TEXT PRIMARY KEY,
		username TEXT UNIQUE NOT NULL,
		email TEXT UNIQUE NOT NULL,
		password_hash TEXT NOT NULL,
		status TEXT DEFAULT 'active',
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS user_roles (
		user_id TEXT NOT NULL,
		role TEXT NOT NULL,
		PRIMARY KEY (user_id, role),
		FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS casbin_rule (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		ptype TEXT,
		v0 TEXT,
		v1 TEXT,
		v2 TEXT,
		v3 TEXT,
		v4 TEXT,
		v5 TEXT
	);
	`
	_, err = db.Exec(migrationSQL)
	require.NoError(t, err)

	// Initialize repositories
	userRepo := repository.NewUserRepository(db)
	userRoleRepo := userRepo

	// Initialize Casbin
	enforcer, err := authinfra.NewCasbinEnforcer(db.DB, persistence.DBTypeSQLite)
	require.NoError(t, err)

	// Initialize default policies
	err = authinfra.InitializeDefaultPolicies(enforcer)
	require.NoError(t, err)

	// Initialize services
	passwordHasher := auth.NewBcryptPasswordHasher(0)
	jwtManager := authinfra.NewJWTManager("test_secret_key_123456789012345678901234567890", 1*time.Hour, 24*time.Hour)

	authSvc := impl.NewAuthApplicationService(
		userRepo,
		userRoleRepo,
		passwordHasher,
		jwtManager,
	).(*impl.AuthApplicationServiceImpl)

	cleanup := func() {
		db.Close()
	}

	return authSvc, db, cleanup
}

func TestAuthIntegration_RegisterAndLogin(t *testing.T) {
	authSvc, _, cleanup := setupAuthIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// Register user
	registerReq := contracts.RegisterRequest{
		Username: "testuser",
		Email:    "test@example.com",
		Password: "password123",
	}

	registerResp, err := authSvc.Register(ctx, registerReq)
	require.NoError(t, err)
	assert.NotEmpty(t, registerResp.UserID)
	assert.Equal(t, "testuser", registerResp.Username)

	// Login
	loginReq := contracts.LoginRequest{
		Username: "testuser",
		Password: "password123",
	}

	loginResp, err := authSvc.Login(ctx, loginReq)
	require.NoError(t, err)
	assert.NotEmpty(t, loginResp.AccessToken)
	assert.NotEmpty(t, loginResp.RefreshToken)
	assert.Equal(t, "testuser", loginResp.User.Username)
	assert.Contains(t, loginResp.User.Roles, "viewer") // Default role
}

func TestAuthIntegration_Register_DuplicateUsername(t *testing.T) {
	authSvc, _, cleanup := setupAuthIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	req := contracts.RegisterRequest{
		Username: "testuser",
		Email:    "test@example.com",
		Password: "password123",
	}

	_, err := authSvc.Register(ctx, req)
	require.NoError(t, err)

	// Try to register again with same username
	_, err = authSvc.Register(ctx, req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "username already exists")
}

func TestAuthIntegration_Login_InvalidPassword(t *testing.T) {
	authSvc, _, cleanup := setupAuthIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// Register user
	registerReq := contracts.RegisterRequest{
		Username: "testuser",
		Email:    "test@example.com",
		Password: "password123",
	}

	_, err := authSvc.Register(ctx, registerReq)
	require.NoError(t, err)

	// Try to login with wrong password
	loginReq := contracts.LoginRequest{
		Username: "testuser",
		Password: "wrong_password",
	}

	_, err = authSvc.Login(ctx, loginReq)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid")
}

func TestAuthIntegration_RefreshToken(t *testing.T) {
	authSvc, _, cleanup := setupAuthIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// Register and login
	registerReq := contracts.RegisterRequest{
		Username: "testuser",
		Email:    "test@example.com",
		Password: "password123",
	}

	_, err := authSvc.Register(ctx, registerReq)
	require.NoError(t, err)

	loginReq := contracts.LoginRequest{
		Username: "testuser",
		Password: "password123",
	}

	loginResp, err := authSvc.Login(ctx, loginReq)
	require.NoError(t, err)

	// Refresh token
	refreshResp, err := authSvc.RefreshToken(ctx, loginResp.RefreshToken)
	require.NoError(t, err)
	assert.NotEmpty(t, refreshResp.AccessToken)
	assert.NotEmpty(t, refreshResp.RefreshToken)
	// Refresh returns new tokens; access token may be same until expiry depending on impl
}

func TestAuthIntegration_UpdatePassword(t *testing.T) {
	authSvc, _, cleanup := setupAuthIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// Register user
	registerReq := contracts.RegisterRequest{
		Username: "testuser",
		Email:    "test@example.com",
		Password: "old_password",
	}

	registerResp, err := authSvc.Register(ctx, registerReq)
	require.NoError(t, err)

	// Update password
	updateReq := contracts.UpdatePasswordRequest{
		OldPassword: "old_password",
		NewPassword: "new_password",
	}

	err = authSvc.UpdatePassword(ctx, shared.ID(registerResp.UserID), updateReq)
	require.NoError(t, err)

	// Try to login with old password (should fail)
	loginReq := contracts.LoginRequest{
		Username: "testuser",
		Password: "old_password",
	}

	_, err = authSvc.Login(ctx, loginReq)
	assert.Error(t, err)

	// Login with new password (should succeed)
	loginReq.Password = "new_password"
	loginResp, err := authSvc.Login(ctx, loginReq)
	require.NoError(t, err)
	assert.NotEmpty(t, loginResp.AccessToken)
}

func TestAuthIntegration_UpdateUserRoles(t *testing.T) {
	authSvc, _, cleanup := setupAuthIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// Register user
	registerReq := contracts.RegisterRequest{
		Username: "testuser",
		Email:    "test@example.com",
		Password: "password123",
	}

	registerResp, err := authSvc.Register(ctx, registerReq)
	require.NoError(t, err)

	// Get current user (should have viewer role)
	userInfo, err := authSvc.GetCurrentUser(ctx, shared.ID(registerResp.UserID))
	require.NoError(t, err)
	assert.Contains(t, userInfo.Roles, "viewer")

	// Update roles to admin
	err = authSvc.UpdateUserRoles(ctx, shared.ID(registerResp.UserID), []string{"admin"})
	require.NoError(t, err)

	// Get user again (should have admin role)
	userInfo, err = authSvc.GetCurrentUser(ctx, shared.ID(registerResp.UserID))
	require.NoError(t, err)
	assert.Contains(t, userInfo.Roles, "admin")
	assert.NotContains(t, userInfo.Roles, "viewer")
}
