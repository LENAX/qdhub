package dao

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/auth"
	"qdhub/internal/domain/shared"
)

// UserRow represents users table row.
type UserRow struct {
	ID           string    `db:"id"`
	Username     string    `db:"username"`
	Email        string    `db:"email"`
	PasswordHash string    `db:"password_hash"`
	Status       string    `db:"status"`
	CreatedAt    time.Time `db:"created_at"`
	UpdatedAt    time.Time `db:"updated_at"`
}

// UserRoleRow represents user_roles table row.
type UserRoleRow struct {
	UserID string `db:"user_id"`
	Role   string `db:"role"`
}

// UserDAO provides data access operations for User.
type UserDAO struct {
	*SQLBaseDAO[UserRow]
}

// NewUserDAO creates a new UserDAO.
func NewUserDAO(db *sqlx.DB) *UserDAO {
	return &UserDAO{
		SQLBaseDAO: NewSQLBaseDAO[UserRow](db, "users", "id"),
	}
}

// Create inserts a new user record.
func (d *UserDAO) Create(tx *sqlx.Tx, entity *auth.User) error {
	query := `INSERT INTO users (id, username, email, password_hash, status, created_at, updated_at)
		VALUES (:id, :username, :email, :password_hash, :status, :created_at, :updated_at)`

	row := d.toRow(entity)
	var err error
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to create user: %w", err)
	}
	return nil
}

// GetByID retrieves a user by ID.
func (d *UserDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*auth.User, error) {
	row, err := d.Get(tx, id.String())
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return d.toEntity(row), nil
}

// GetByUsername retrieves a user by username.
func (d *UserDAO) GetByUsername(tx *sqlx.Tx, username string) (*auth.User, error) {
	query := d.DB().Rebind(`SELECT * FROM users WHERE username = ?`)
	var row UserRow

	var err error
	if tx != nil {
		err = tx.Get(&row, query, username)
	} else {
		err = d.DB().Get(&row, query, username)
	}

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get user by username: %w", err)
	}

	return d.toEntity(&row), nil
}

// GetByEmail retrieves a user by email.
func (d *UserDAO) GetByEmail(tx *sqlx.Tx, email string) (*auth.User, error) {
	query := d.DB().Rebind(`SELECT * FROM users WHERE email = ?`)
	var row UserRow

	var err error
	if tx != nil {
		err = tx.Get(&row, query, email)
	} else {
		err = d.DB().Get(&row, query, email)
	}

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get user by email: %w", err)
	}

	return d.toEntity(&row), nil
}

// Update updates an existing user record.
func (d *UserDAO) Update(tx *sqlx.Tx, entity *auth.User) error {
	query := `UPDATE users SET
		username = :username, email = :email, password_hash = :password_hash,
		status = :status, updated_at = :updated_at
		WHERE id = :id`

	row := d.toRow(entity)
	var err error
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to update user: %w", err)
	}
	return nil
}

// DeleteByID deletes a user by ID.
func (d *UserDAO) DeleteByID(tx *sqlx.Tx, id shared.ID) error {
	return d.Delete(tx, id.String())
}

// ListWithPagination retrieves users with pagination.
func (d *UserDAO) ListWithPagination(tx *sqlx.Tx, offset, limit int) ([]*auth.User, int64, error) {
	// Get total count
	countQuery := d.DB().Rebind(`SELECT COUNT(*) FROM users`)
	var total int64
	var err error
	if tx != nil {
		err = tx.Get(&total, countQuery)
	} else {
		err = d.DB().Get(&total, countQuery)
	}
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count users: %w", err)
	}

	// Get paginated results
	query := d.DB().Rebind(`SELECT * FROM users ORDER BY created_at DESC LIMIT ? OFFSET ?`)
	var rows []UserRow
	if tx != nil {
		err = tx.Select(&rows, query, limit, offset)
	} else {
		err = d.DB().Select(&rows, query, limit, offset)
	}
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list users: %w", err)
	}

	entities := make([]*auth.User, 0, len(rows))
	for _, row := range rows {
		entities = append(entities, d.toEntity(&row))
	}

	return entities, total, nil
}

// toRow converts domain entity to database row.
func (d *UserDAO) toRow(entity *auth.User) *UserRow {
	return &UserRow{
		ID:           entity.ID.String(),
		Username:     entity.Username,
		Email:        entity.Email,
		PasswordHash: entity.PasswordHash,
		Status:       entity.Status.String(),
		CreatedAt:    entity.CreatedAt.ToTime(),
		UpdatedAt:    entity.UpdatedAt.ToTime(),
	}
}

// toEntity converts database row to domain entity.
func (d *UserDAO) toEntity(row *UserRow) *auth.User {
	return &auth.User{
		ID:           shared.ID(row.ID),
		Username:     row.Username,
		Email:        row.Email,
		PasswordHash: row.PasswordHash,
		Status:       auth.UserStatus(row.Status),
		CreatedAt:    shared.Timestamp(row.CreatedAt),
		UpdatedAt:    shared.Timestamp(row.UpdatedAt),
	}
}

// UserRoleDAO provides data access operations for user roles.
type UserRoleDAO struct {
	db *sqlx.DB
}

// NewUserRoleDAO creates a new UserRoleDAO.
func NewUserRoleDAO(db *sqlx.DB) *UserRoleDAO {
	return &UserRoleDAO{db: db}
}

// AssignRole assigns a role to a user.
func (d *UserRoleDAO) AssignRole(tx *sqlx.Tx, userID shared.ID, role string) error {
	query := d.db.Rebind(`INSERT INTO user_roles (user_id, role) VALUES (?, ?)`)
	var err error
	if tx != nil {
		_, err = tx.Exec(query, userID.String(), role)
	} else {
		_, err = d.db.Exec(query, userID.String(), role)
	}

	if err != nil {
		return fmt.Errorf("failed to assign role: %w", err)
	}
	return nil
}

// RemoveRole removes a role from a user.
func (d *UserRoleDAO) RemoveRole(tx *sqlx.Tx, userID shared.ID, role string) error {
	query := d.db.Rebind(`DELETE FROM user_roles WHERE user_id = ? AND role = ?`)
	var err error
	if tx != nil {
		_, err = tx.Exec(query, userID.String(), role)
	} else {
		_, err = d.db.Exec(query, userID.String(), role)
	}

	if err != nil {
		return fmt.Errorf("failed to remove role: %w", err)
	}
	return nil
}

// GetUserRoles retrieves all roles for a user.
func (d *UserRoleDAO) GetUserRoles(tx *sqlx.Tx, userID shared.ID) ([]string, error) {
	query := d.db.Rebind(`SELECT role FROM user_roles WHERE user_id = ?`)
	var roles []string
	var err error
	if tx != nil {
		err = tx.Select(&roles, query, userID.String())
	} else {
		err = d.db.Select(&roles, query, userID.String())
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get user roles: %w", err)
	}
	return roles, nil
}

// GetUsersByRole retrieves all user IDs with a specific role.
func (d *UserRoleDAO) GetUsersByRole(tx *sqlx.Tx, role string) ([]shared.ID, error) {
	query := d.db.Rebind(`SELECT user_id FROM user_roles WHERE role = ?`)
	var userIDs []string
	var err error
	if tx != nil {
		err = tx.Select(&userIDs, query, role)
	} else {
		err = d.db.Select(&userIDs, query, role)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get users by role: %w", err)
	}

	ids := make([]shared.ID, 0, len(userIDs))
	for _, id := range userIDs {
		ids = append(ids, shared.ID(id))
	}
	return ids, nil
}

// RemoveAllRoles removes all roles from a user.
func (d *UserRoleDAO) RemoveAllRoles(tx *sqlx.Tx, userID shared.ID) error {
	query := d.db.Rebind(`DELETE FROM user_roles WHERE user_id = ?`)
	var err error
	if tx != nil {
		_, err = tx.Exec(query, userID.String())
	} else {
		_, err = d.db.Exec(query, userID.String())
	}

	if err != nil {
		return fmt.Errorf("failed to remove all roles: %w", err)
	}
	return nil
}
