// Package repository provides user repository implementations.
package repository

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/auth"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/dao"
)

// UserRepositoryImpl implements auth.UserRepository and auth.UserRoleRepository.
type UserRepositoryImpl struct {
	db          *persistence.DB
	tx          *sqlx.Tx
	userDAO     *dao.UserDAO
	userRoleDAO *dao.UserRoleDAO
}

// NewUserRepository creates a new UserRepositoryImpl.
func NewUserRepository(db *persistence.DB) *UserRepositoryImpl {
	return &UserRepositoryImpl{
		db:          db,
		tx:          nil,
		userDAO:     dao.NewUserDAO(db.DB),
		userRoleDAO: dao.NewUserRoleDAO(db.DB),
	}
}

// NewUserRepositoryWithTx creates a new UserRepositoryImpl bound to an external transaction.
func NewUserRepositoryWithTx(db *persistence.DB, tx *sqlx.Tx) *UserRepositoryImpl {
	return &UserRepositoryImpl{
		db:          db,
		tx:          tx,
		userDAO:     dao.NewUserDAO(db.DB),
		userRoleDAO: dao.NewUserRoleDAO(db.DB),
	}
}

// Create creates a new user.
func (r *UserRepositoryImpl) Create(ctx context.Context, user *auth.User) error {
	if r.tx != nil {
		return r.userDAO.Create(r.tx, user)
	}
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		return r.userDAO.Create(tx, user)
	})
}

// GetByID retrieves a user by ID.
func (r *UserRepositoryImpl) GetByID(ctx context.Context, id shared.ID) (*auth.User, error) {
	return r.userDAO.GetByID(r.tx, id)
}

// GetByUsername retrieves a user by username.
func (r *UserRepositoryImpl) GetByUsername(ctx context.Context, username string) (*auth.User, error) {
	return r.userDAO.GetByUsername(r.tx, username)
}

// GetByEmail retrieves a user by email.
func (r *UserRepositoryImpl) GetByEmail(ctx context.Context, email string) (*auth.User, error) {
	return r.userDAO.GetByEmail(r.tx, email)
}

// Update updates an existing user.
func (r *UserRepositoryImpl) Update(ctx context.Context, user *auth.User) error {
	if r.tx != nil {
		return r.userDAO.Update(r.tx, user)
	}
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		return r.userDAO.Update(tx, user)
	})
}

// Delete deletes a user by ID.
func (r *UserRepositoryImpl) Delete(ctx context.Context, id shared.ID) error {
	if r.tx != nil {
		// Delete roles first
		if err := r.userRoleDAO.RemoveAllRoles(r.tx, id); err != nil {
			return fmt.Errorf("failed to remove user roles: %w", err)
		}
		return r.userDAO.DeleteByID(r.tx, id)
	}
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		// Delete roles first
		if err := r.userRoleDAO.RemoveAllRoles(tx, id); err != nil {
			return fmt.Errorf("failed to remove user roles: %w", err)
		}
		return r.userDAO.DeleteByID(tx, id)
	})
}

// List lists users with pagination.
func (r *UserRepositoryImpl) List(ctx context.Context, offset, limit int) ([]*auth.User, int64, error) {
	return r.userDAO.ListWithPagination(r.tx, offset, limit)
}

// AssignRole assigns a role to a user.
func (r *UserRepositoryImpl) AssignRole(ctx context.Context, userID shared.ID, role string) error {
	if r.tx != nil {
		return r.userRoleDAO.AssignRole(r.tx, userID, role)
	}
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		return r.userRoleDAO.AssignRole(tx, userID, role)
	})
}

// RemoveRole removes a role from a user.
func (r *UserRepositoryImpl) RemoveRole(ctx context.Context, userID shared.ID, role string) error {
	if r.tx != nil {
		return r.userRoleDAO.RemoveRole(r.tx, userID, role)
	}
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		return r.userRoleDAO.RemoveRole(tx, userID, role)
	})
}

// GetUserRoles retrieves all roles for a user.
func (r *UserRepositoryImpl) GetUserRoles(ctx context.Context, userID shared.ID) ([]string, error) {
	return r.userRoleDAO.GetUserRoles(r.tx, userID)
}

// GetUsersByRole retrieves all users with a specific role.
func (r *UserRepositoryImpl) GetUsersByRole(ctx context.Context, role string) ([]shared.ID, error) {
	return r.userRoleDAO.GetUsersByRole(r.tx, role)
}

// RemoveAllRoles removes all roles from a user.
func (r *UserRepositoryImpl) RemoveAllRoles(ctx context.Context, userID shared.ID) error {
	if r.tx != nil {
		return r.userRoleDAO.RemoveAllRoles(r.tx, userID)
	}
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		return r.userRoleDAO.RemoveAllRoles(tx, userID)
	})
}
