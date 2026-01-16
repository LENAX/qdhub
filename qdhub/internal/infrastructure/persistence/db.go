// Package persistence provides database persistence implementations.
package persistence

import (
	"fmt"

	"github.com/jmoiron/sqlx"

	// Database drivers - imported for side effects
	_ "github.com/go-sql-driver/mysql" // MySQL driver
	_ "github.com/lib/pq"              // PostgreSQL driver
	_ "github.com/mattn/go-sqlite3"    // SQLite driver
)

// DB wraps sqlx.DB and provides database connection management.
type DB struct {
	*sqlx.DB
	dsn     string  // Store DSN for Task Engine integration
	dialect Dialect // Database dialect for DB-specific behaviors
}

// NewDB creates a new SQLite database connection (for backward compatibility).
func NewDB(dsn string) (*DB, error) {
	return NewDBWithDialect(NewSQLiteDialect(), dsn)
}

// NewDBWithType creates a new database connection with the specified database type.
func NewDBWithType(dbType DBType, dsn string) (*DB, error) {
	return NewDBWithDialect(GetDialect(dbType), dsn)
}

// NewDBWithDialect creates a new database connection with the specified dialect.
func NewDBWithDialect(dialect Dialect, dsn string) (*DB, error) {
	db, err := sqlx.Connect(dialect.DriverName(), dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database (%s): %w", dialect.DriverName(), err)
	}

	// Execute dialect-specific initialization
	if err := dialect.OnConnect(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	return &DB{DB: db, dsn: dsn, dialect: dialect}, nil
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

// Dialect returns the database dialect.
func (db *DB) Dialect() Dialect {
	return db.dialect
}

// DriverName returns the database driver name.
func (db *DB) DriverName() string {
	if db.dialect != nil {
		return db.dialect.DriverName()
	}
	return "sqlite3" // default for backward compatibility
}

// Querier is an interface for database query operations.
// Both *sqlx.DB and *sqlx.Tx implement this interface.
type Querier interface {
	Get(dest interface{}, query string, args ...interface{}) error
	Select(dest interface{}, query string, args ...interface{}) error
	Exec(query string, args ...interface{}) (interface{}, error)
	NamedExec(query string, arg interface{}) (interface{}, error)
}
