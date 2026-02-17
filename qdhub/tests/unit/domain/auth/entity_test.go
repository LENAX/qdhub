package auth_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"qdhub/internal/domain/auth"
)

func TestUserStatus_IsValid(t *testing.T) {
	tests := []struct {
		name     string
		status   auth.UserStatus
		expected bool
	}{
		{"active", auth.UserStatusActive, true},
		{"inactive", auth.UserStatusInactive, true},
		{"locked", auth.UserStatusLocked, true},
		{"invalid", auth.UserStatus("invalid"), false},
		{"empty", auth.UserStatus(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.status.IsValid())
		})
	}
}

func TestNewUser(t *testing.T) {
	username := "testuser"
	email := "test@example.com"
	passwordHash := "hashed_password"

	user := auth.NewUser(username, email, passwordHash)

	assert.NotEmpty(t, user.ID)
	assert.Equal(t, username, user.Username)
	assert.Equal(t, email, user.Email)
	assert.Equal(t, passwordHash, user.PasswordHash)
	assert.Equal(t, auth.UserStatusActive, user.Status)
	assert.False(t, user.CreatedAt.IsZero())
	assert.False(t, user.UpdatedAt.IsZero())
}

func TestUser_UpdatePassword(t *testing.T) {
	user := auth.NewUser("testuser", "test@example.com", "old_hash")
	oldUpdatedAt := user.UpdatedAt

	newHash := "new_hash"
	user.UpdatePassword(newHash)

	assert.Equal(t, newHash, user.PasswordHash)
	assert.True(t, user.UpdatedAt.ToTime().After(oldUpdatedAt.ToTime()))
}

func TestUser_UpdateStatus(t *testing.T) {
	user := auth.NewUser("testuser", "test@example.com", "hash")
	oldUpdatedAt := user.UpdatedAt

	user.UpdateStatus(auth.UserStatusInactive)
	assert.Equal(t, auth.UserStatusInactive, user.Status)
	assert.True(t, user.UpdatedAt.ToTime().After(oldUpdatedAt.ToTime()))

	// Invalid status should not update
	user.UpdateStatus(auth.UserStatus("invalid"))
	assert.Equal(t, auth.UserStatusInactive, user.Status)
}

func TestUser_IsActive(t *testing.T) {
	user := auth.NewUser("testuser", "test@example.com", "hash")
	assert.True(t, user.IsActive())

	user.UpdateStatus(auth.UserStatusInactive)
	assert.False(t, user.IsActive())

	user.UpdateStatus(auth.UserStatusLocked)
	assert.False(t, user.IsActive())
}
