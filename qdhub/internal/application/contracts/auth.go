// Package contracts defines application service interfaces.
package contracts

import (
	"context"

	"qdhub/internal/domain/auth"
	"qdhub/internal/domain/shared"
)

// AuthApplicationService defines the interface for authentication application services.
type AuthApplicationService interface {
	// Register registers a new user.
	Register(ctx context.Context, req RegisterRequest) (*RegisterResponse, error)

	// Login authenticates a user and returns access and refresh tokens.
	Login(ctx context.Context, req LoginRequest) (*LoginResponse, error)

	// RefreshToken refreshes an access token using a refresh token.
	RefreshToken(ctx context.Context, refreshToken string) (*LoginResponse, error)

	// GetCurrentUser retrieves the current authenticated user.
	GetCurrentUser(ctx context.Context, userID shared.ID) (*UserInfo, error)

	// UpdatePassword updates the current user's password.
	UpdatePassword(ctx context.Context, userID shared.ID, req UpdatePasswordRequest) error

	// ListUsers lists users (admin only).
	ListUsers(ctx context.Context, offset, limit int) ([]*UserInfo, int64, error)

	// UpdateUserRoles updates a user's roles (admin only).
	UpdateUserRoles(ctx context.Context, userID shared.ID, roles []string) error
}

// RegisterRequest represents a user registration request.
type RegisterRequest struct {
	Username string `json:"username" binding:"required,min=3,max=64"`
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required,min=8"`
}

// RegisterResponse represents a user registration response.
type RegisterResponse struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	Email    string `json:"email"`
}

// LoginRequest represents a login request.
type LoginRequest struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

// LoginResponse represents a login response.
type LoginResponse struct {
	AccessToken  string   `json:"access_token"`
	RefreshToken string   `json:"refresh_token"`
	User         UserInfo `json:"user"`
}

// UserInfo represents user information.
type UserInfo struct {
	ID       string   `json:"id"`
	Username string   `json:"username"`
	Email    string   `json:"email"`
	Roles    []string `json:"roles"`
	Status   string   `json:"status"`
}

// UpdatePasswordRequest represents a password update request.
type UpdatePasswordRequest struct {
	OldPassword string `json:"old_password" binding:"required"`
	NewPassword string `json:"new_password" binding:"required,min=8"`
}

// ToUserInfo converts a domain User to UserInfo.
func ToUserInfo(user *auth.User, roles []string) *UserInfo {
	return &UserInfo{
		ID:       user.ID.String(),
		Username: user.Username,
		Email:    user.Email,
		Roles:    roles,
		Status:   user.Status.String(),
	}
}
