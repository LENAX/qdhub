// Package impl contains authentication application service implementation.
package impl

import (
	"context"
	"errors"
	"fmt"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/auth"
	"qdhub/internal/domain/shared"
	authinfra "qdhub/internal/infrastructure/auth"
)

// AuthApplicationServiceImpl implements AuthApplicationService.
type AuthApplicationServiceImpl struct {
	userRepo       auth.UserRepository
	userRoleRepo   auth.UserRoleRepository
	authDomainSvc  auth.AuthDomainService
	passwordHasher auth.PasswordHasher
	jwtManager     *authinfra.JWTManager
}

// NewAuthApplicationService creates a new AuthApplicationService implementation.
func NewAuthApplicationService(
	userRepo auth.UserRepository,
	userRoleRepo auth.UserRoleRepository,
	passwordHasher auth.PasswordHasher,
	jwtManager *authinfra.JWTManager,
) contracts.AuthApplicationService {
	return &AuthApplicationServiceImpl{
		userRepo:       userRepo,
		userRoleRepo:   userRoleRepo,
		authDomainSvc:  auth.NewAuthDomainService(),
		passwordHasher: passwordHasher,
		jwtManager:     jwtManager,
	}
}

// Register registers a new user.
func (s *AuthApplicationServiceImpl) Register(ctx context.Context, req contracts.RegisterRequest) (*contracts.RegisterResponse, error) {
	// Check if username already exists
	existingUser, err := s.userRepo.GetByUsername(ctx, req.Username)
	if err != nil {
		return nil, fmt.Errorf("failed to check username: %w", err)
	}
	if existingUser != nil {
		return nil, errors.New("username already exists")
	}

	// Check if email already exists
	existingUser, err = s.userRepo.GetByEmail(ctx, req.Email)
	if err != nil {
		return nil, fmt.Errorf("failed to check email: %w", err)
	}
	if existingUser != nil {
		return nil, errors.New("email already exists")
	}

	// Hash password
	passwordHash, err := s.passwordHasher.HashPassword(req.Password)
	if err != nil {
		return nil, fmt.Errorf("failed to hash password: %w", err)
	}

	// Create user
	user := auth.NewUser(req.Username, req.Email, passwordHash)

	// Save user
	if err := s.userRepo.Create(ctx, user); err != nil {
		return nil, fmt.Errorf("failed to create user: %w", err)
	}

	// Assign default role (viewer)
	if err := s.userRoleRepo.AssignRole(ctx, user.ID, "viewer"); err != nil {
		return nil, fmt.Errorf("failed to assign default role: %w", err)
	}

	return &contracts.RegisterResponse{
		UserID:   user.ID.String(),
		Username: user.Username,
		Email:    user.Email,
	}, nil
}

// Login authenticates a user and returns access and refresh tokens.
func (s *AuthApplicationServiceImpl) Login(ctx context.Context, req contracts.LoginRequest) (*contracts.LoginResponse, error) {
	// Validate credentials
	user, err := s.authDomainSvc.ValidateCredentials(ctx, req.Username, req.Password, s.passwordHasher, s.userRepo)
	if err != nil {
		return nil, errors.New("invalid username or password")
	}

	// Get user roles
	roles, err := s.userRoleRepo.GetUserRoles(ctx, user.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user roles: %w", err)
	}

	// Generate tokens
	accessToken, err := s.jwtManager.GenerateAccessToken(user.ID, user.Username, roles)
	if err != nil {
		return nil, fmt.Errorf("failed to generate access token: %w", err)
	}

	refreshToken, err := s.jwtManager.GenerateRefreshToken(user.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to generate refresh token: %w", err)
	}

	return &contracts.LoginResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		User:         *contracts.ToUserInfo(user, roles),
	}, nil
}

// RefreshToken refreshes an access token using a refresh token.
func (s *AuthApplicationServiceImpl) RefreshToken(ctx context.Context, refreshToken string) (*contracts.LoginResponse, error) {
	// Validate refresh token
	userID, err := s.jwtManager.ValidateRefreshToken(refreshToken)
	if err != nil {
		return nil, errors.New("invalid refresh token")
	}

	// Get user
	user, err := s.userRepo.GetByID(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}
	if user == nil {
		return nil, errors.New("user not found")
	}

	if !user.IsActive() {
		return nil, errors.New("user is not active")
	}

	// Get user roles
	roles, err := s.userRoleRepo.GetUserRoles(ctx, user.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user roles: %w", err)
	}

	// Generate new tokens
	accessToken, err := s.jwtManager.GenerateAccessToken(user.ID, user.Username, roles)
	if err != nil {
		return nil, fmt.Errorf("failed to generate access token: %w", err)
	}

	newRefreshToken, err := s.jwtManager.GenerateRefreshToken(user.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to generate refresh token: %w", err)
	}

	return &contracts.LoginResponse{
		AccessToken:  accessToken,
		RefreshToken: newRefreshToken,
		User:         *contracts.ToUserInfo(user, roles),
	}, nil
}

// GetCurrentUser retrieves the current authenticated user.
func (s *AuthApplicationServiceImpl) GetCurrentUser(ctx context.Context, userID shared.ID) (*contracts.UserInfo, error) {
	user, err := s.userRepo.GetByID(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}
	if user == nil {
		return nil, errors.New("user not found")
	}

	roles, err := s.userRoleRepo.GetUserRoles(ctx, user.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user roles: %w", err)
	}

	return contracts.ToUserInfo(user, roles), nil
}

// UpdatePassword updates the current user's password.
func (s *AuthApplicationServiceImpl) UpdatePassword(ctx context.Context, userID shared.ID, req contracts.UpdatePasswordRequest) error {
	// Get user
	user, err := s.userRepo.GetByID(ctx, userID)
	if err != nil {
		return fmt.Errorf("failed to get user: %w", err)
	}
	if user == nil {
		return errors.New("user not found")
	}

	// Verify old password
	if !s.passwordHasher.VerifyPassword(user.PasswordHash, req.OldPassword) {
		return errors.New("invalid old password")
	}

	// Hash new password
	newPasswordHash, err := s.passwordHasher.HashPassword(req.NewPassword)
	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}

	// Update password
	user.UpdatePassword(newPasswordHash)
	if err := s.userRepo.Update(ctx, user); err != nil {
		return fmt.Errorf("failed to update password: %w", err)
	}

	return nil
}

// ListUsers lists users (admin only).
func (s *AuthApplicationServiceImpl) ListUsers(ctx context.Context, offset, limit int) ([]*contracts.UserInfo, int64, error) {
	users, total, err := s.userRepo.List(ctx, offset, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list users: %w", err)
	}

	userInfos := make([]*contracts.UserInfo, 0, len(users))
	for _, user := range users {
		roles, err := s.userRoleRepo.GetUserRoles(ctx, user.ID)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to get user roles: %w", err)
		}
		userInfos = append(userInfos, contracts.ToUserInfo(user, roles))
	}

	return userInfos, total, nil
}

// UpdateUserRoles updates a user's roles (admin only).
func (s *AuthApplicationServiceImpl) UpdateUserRoles(ctx context.Context, userID shared.ID, roles []string) error {
	// Get user
	user, err := s.userRepo.GetByID(ctx, userID)
	if err != nil {
		return fmt.Errorf("failed to get user: %w", err)
	}
	if user == nil {
		return errors.New("user not found")
	}

	// Remove all existing roles
	if err := s.userRoleRepo.RemoveAllRoles(ctx, userID); err != nil {
		return fmt.Errorf("failed to remove existing roles: %w", err)
	}

	// Assign new roles
	for _, role := range roles {
		if err := s.userRoleRepo.AssignRole(ctx, userID, role); err != nil {
			return fmt.Errorf("failed to assign role %s: %w", role, err)
		}
	}

	return nil
}
