// Package auth provides authentication and authorization domain repositories.
package auth

import (
	"context"

	"qdhub/internal/domain/shared"
)

// UserRepository defines the interface for user persistence operations.
type UserRepository interface {
	// Create creates a new user.
	Create(ctx context.Context, user *User) error

	// GetByID retrieves a user by ID.
	GetByID(ctx context.Context, id shared.ID) (*User, error)

	// GetByUsername retrieves a user by username.
	GetByUsername(ctx context.Context, username string) (*User, error)

	// GetByEmail retrieves a user by email.
	GetByEmail(ctx context.Context, email string) (*User, error)

	// Update updates an existing user.
	Update(ctx context.Context, user *User) error

	// Delete deletes a user by ID.
	Delete(ctx context.Context, id shared.ID) error

	// List lists users with pagination.
	List(ctx context.Context, offset, limit int) ([]*User, int64, error)
}

// UserRoleRepository defines the interface for user-role relationship operations.
type UserRoleRepository interface {
	// AssignRole assigns a role to a user.
	AssignRole(ctx context.Context, userID shared.ID, role string) error

	// RemoveRole removes a role from a user.
	RemoveRole(ctx context.Context, userID shared.ID, role string) error

	// GetUserRoles retrieves all roles for a user.
	GetUserRoles(ctx context.Context, userID shared.ID) ([]string, error)

	// GetUsersByRole retrieves all users with a specific role.
	GetUsersByRole(ctx context.Context, role string) ([]shared.ID, error)

	// RemoveAllRoles removes all roles from a user.
	RemoveAllRoles(ctx context.Context, userID shared.ID) error
}
