// Package persistence provides database persistence implementations.
package persistence

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

// DB wraps sqlx.DB and provides database connection management.
type DB struct {
	*sqlx.DB
	dsn string // Store DSN for Task Engine integration
}

// NewDB creates a new database connection.
func NewDB(dsn string) (*DB, error) {
	db, err := sqlx.Connect("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Enable foreign keys for SQLite
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return nil, fmt.Errorf("failed to enable foreign keys: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	return &DB{DB: db, dsn: dsn}, nil
}

// Close closes the database connection.
func (db *DB) Close() error {
	return db.DB.Close()
}

// BeginTx starts a new transaction.
func (db *DB) BeginTx() (*sqlx.Tx, error) {
	return db.DB.Beginx()
}

// ExecInTx executes a function within a transaction.
func (db *DB) ExecInTx(fn func(*sqlx.Tx) error) error {
	tx, err := db.BeginTx()
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

// DSN returns the database connection string.
func (db *DB) DSN() string {
	return db.dsn
}

// Querier is an interface for database query operations.
// Both *sqlx.DB and *sqlx.Tx implement this interface.
type Querier interface {
	Get(dest interface{}, query string, args ...interface{}) error
	Select(dest interface{}, query string, args ...interface{}) error
	Exec(query string, args ...interface{}) (interface{}, error)
	NamedExec(query string, arg interface{}) (interface{}, error)
}
