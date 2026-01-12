// Package dao provides data access object implementations.
package dao

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// BaseDAO defines the base interface for data access operations.
type BaseDAO[T any] interface {
	// Create inserts a new record.
	Create(tx *sqlx.Tx, entity *T) error

	// Get retrieves a record by ID.
	Get(tx *sqlx.Tx, id string) (*T, error)

	// Update updates an existing record.
	Update(tx *sqlx.Tx, entity *T) error

	// Delete removes a record by ID.
	Delete(tx *sqlx.Tx, id string) error

	// List retrieves all records.
	List(tx *sqlx.Tx) ([]*T, error)
}

// SQLBaseDAO provides a generic SQL-based implementation of BaseDAO.
type SQLBaseDAO[T any] struct {
	db        *sqlx.DB
	tableName string
	idColumn  string
}

// NewSQLBaseDAO creates a new SQLBaseDAO instance.
func NewSQLBaseDAO[T any](db *sqlx.DB, tableName, idColumn string) *SQLBaseDAO[T] {
	return &SQLBaseDAO[T]{
		db:        db,
		tableName: tableName,
		idColumn:  idColumn,
	}
}

// getQuerier returns the appropriate querier (tx or db).
func (d *SQLBaseDAO[T]) getQuerier(tx *sqlx.Tx) sqlx.Queryer {
	if tx != nil {
		return tx
	}
	return d.db
}

// getExecer returns the appropriate execer (tx or db).
func (d *SQLBaseDAO[T]) getExecer(tx *sqlx.Tx) sqlx.Execer {
	if tx != nil {
		return tx
	}
	return d.db
}

// Get retrieves a record by ID using the default query.
func (d *SQLBaseDAO[T]) Get(tx *sqlx.Tx, id string) (*T, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", d.tableName, d.idColumn)
	var entity T

	var err error
	if tx != nil {
		err = tx.Get(&entity, query, id)
	} else {
		err = d.db.Get(&entity, query, id)
	}

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get %s: %w", d.tableName, err)
	}
	return &entity, nil
}

// Delete removes a record by ID.
func (d *SQLBaseDAO[T]) Delete(tx *sqlx.Tx, id string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", d.tableName, d.idColumn)

	var err error
	if tx != nil {
		_, err = tx.Exec(query, id)
	} else {
		_, err = d.db.Exec(query, id)
	}

	if err != nil {
		return fmt.Errorf("failed to delete %s: %w", d.tableName, err)
	}
	return nil
}

// List retrieves all records.
func (d *SQLBaseDAO[T]) List(tx *sqlx.Tx) ([]*T, error) {
	query := fmt.Sprintf("SELECT * FROM %s", d.tableName)
	var entities []*T

	var err error
	if tx != nil {
		err = tx.Select(&entities, query)
	} else {
		err = d.db.Select(&entities, query)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to list %s: %w", d.tableName, err)
	}
	return entities, nil
}

// DB returns the underlying database connection.
func (d *SQLBaseDAO[T]) DB() *sqlx.DB {
	return d.db
}

// TableName returns the table name.
func (d *SQLBaseDAO[T]) TableName() string {
	return d.tableName
}

// ExecWithTx executes a query with optional transaction support.
func ExecWithTx(db *sqlx.DB, tx *sqlx.Tx, query string, args ...interface{}) (sql.Result, error) {
	if tx != nil {
		return tx.Exec(query, args...)
	}
	return db.Exec(query, args...)
}

// GetWithTx executes a get query with optional transaction support.
func GetWithTx[T any](db *sqlx.DB, tx *sqlx.Tx, dest *T, query string, args ...interface{}) error {
	if tx != nil {
		return tx.Get(dest, query, args...)
	}
	return db.Get(dest, query, args...)
}

// SelectWithTx executes a select query with optional transaction support.
func SelectWithTx[T any](db *sqlx.DB, tx *sqlx.Tx, dest *[]*T, query string, args ...interface{}) error {
	if tx != nil {
		return tx.Select(dest, query, args...)
	}
	return db.Select(dest, query, args...)
}
