// Package auth provides authentication and authorization domain entities.
package auth

import (
	"qdhub/internal/domain/shared"
)

// UserStatus represents the status of a user.
type UserStatus string

const (
	UserStatusActive   UserStatus = "active"
	UserStatusInactive UserStatus = "inactive"
	UserStatusLocked   UserStatus = "locked"
)

// String returns the string representation of the user status.
func (s UserStatus) String() string {
	return string(s)
}

// IsValid checks if the user status is valid.
func (s UserStatus) IsValid() bool {
	switch s {
	case UserStatusActive, UserStatusInactive, UserStatusLocked:
		return true
	default:
		return false
	}
}

// User represents a user entity in the domain.
type User struct {
	ID           shared.ID
	Username     string
	Email        string
	PasswordHash string
	Status       UserStatus
	CreatedAt    shared.Timestamp
	UpdatedAt    shared.Timestamp
}

// NewUser creates a new user entity.
func NewUser(username, email, passwordHash string) *User {
	now := shared.Now()
	return &User{
		ID:           shared.NewID(),
		Username:     username,
		Email:        email,
		PasswordHash: passwordHash,
		Status:       UserStatusActive,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
}

// UpdatePassword updates the user's password hash.
func (u *User) UpdatePassword(passwordHash string) {
	u.PasswordHash = passwordHash
	u.UpdatedAt = shared.Now()
}

// UpdateStatus updates the user's status.
func (u *User) UpdateStatus(status UserStatus) {
	if status.IsValid() {
		u.Status = status
		u.UpdatedAt = shared.Now()
	}
}

// IsActive checks if the user is active.
func (u *User) IsActive() bool {
	return u.Status == UserStatusActive
}

// Permission represents a permission for a resource.
type Permission struct {
	Resource string // e.g., "sync-plans"
	Action   string // e.g., "read", "write", "delete", "execute"
}

// Role represents a role with permissions.
type Role struct {
	Name        string
	Description string
	Permissions []Permission
}
