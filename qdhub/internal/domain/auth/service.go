// Package auth provides authentication and authorization domain services.
package auth

import (
	"context"
	"errors"

	"golang.org/x/crypto/bcrypt"
)

// PasswordHasher defines the interface for password hashing operations.
type PasswordHasher interface {
	// HashPassword hashes a plaintext password.
	HashPassword(password string) (string, error)

	// VerifyPassword verifies a password against a hash.
	VerifyPassword(hashedPassword, password string) bool
}

// BcryptPasswordHasher implements PasswordHasher using bcrypt.
type BcryptPasswordHasher struct {
	cost int
}

// NewBcryptPasswordHasher creates a new bcrypt password hasher.
func NewBcryptPasswordHasher(cost int) *BcryptPasswordHasher {
	if cost == 0 {
		cost = bcrypt.DefaultCost
	}
	return &BcryptPasswordHasher{cost: cost}
}

// HashPassword hashes a plaintext password using bcrypt.
func (h *BcryptPasswordHasher) HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), h.cost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// VerifyPassword verifies a password against a bcrypt hash.
func (h *BcryptPasswordHasher) VerifyPassword(hashedPassword, password string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password))
	return err == nil
}

// AuthDomainService defines the interface for authentication domain services.
type AuthDomainService interface {
	// ValidateCredentials validates user credentials.
	ValidateCredentials(ctx context.Context, username, password string, hasher PasswordHasher, userRepo UserRepository) (*User, error)
}

// authDomainService implements AuthDomainService.
type authDomainService struct{}

// NewAuthDomainService creates a new authentication domain service.
func NewAuthDomainService() AuthDomainService {
	return &authDomainService{}
}

// ValidateCredentials validates user credentials.
func (s *authDomainService) ValidateCredentials(ctx context.Context, username, password string, hasher PasswordHasher, userRepo UserRepository) (*User, error) {
	// Get user by username
	user, err := userRepo.GetByUsername(ctx, username)
	if err != nil {
		return nil, err
	}

	if user == nil {
		return nil, errors.New("user not found")
	}

	// Check if user is active
	if !user.IsActive() {
		return nil, errors.New("user is not active")
	}

	// Verify password
	if !hasher.VerifyPassword(user.PasswordHash, password) {
		return nil, errors.New("invalid password")
	}

	return user, nil
}
