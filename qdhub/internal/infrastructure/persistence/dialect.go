// Package persistence provides database persistence implementations.
package persistence

import (
	"github.com/jmoiron/sqlx"
)

// DBType represents supported database types.
type DBType string

const (
	DBTypeSQLite   DBType = "sqlite3"
	DBTypePostgres DBType = "postgres"
	DBTypeMySQL    DBType = "mysql"
)

// Dialect defines database-specific behaviors.
// Each database may have different initialization requirements and SQL syntax.
type Dialect interface {
	// DriverName returns the database driver name for sqlx.Connect.
	DriverName() string

	// OnConnect is called after database connection is established.
	// Use this for database-specific initialization (e.g., PRAGMA for SQLite).
	OnConnect(db *sqlx.DB) error
}

// ==================== SQLite Dialect ====================

// SQLiteDialect implements Dialect for SQLite database.
type SQLiteDialect struct{}

// NewSQLiteDialect creates a new SQLite dialect.
func NewSQLiteDialect() *SQLiteDialect {
	return &SQLiteDialect{}
}

// DriverName returns "sqlite3".
func (d *SQLiteDialect) DriverName() string {
	return "sqlite3"
}

// OnConnect enables foreign keys for SQLite.
func (d *SQLiteDialect) OnConnect(db *sqlx.DB) error {
	_, err := db.Exec("PRAGMA foreign_keys = ON")
	return err
}

// ==================== PostgreSQL Dialect ====================

// PostgresDialect implements Dialect for PostgreSQL database.
type PostgresDialect struct{}

// NewPostgresDialect creates a new PostgreSQL dialect.
func NewPostgresDialect() *PostgresDialect {
	return &PostgresDialect{}
}

// DriverName returns "postgres".
func (d *PostgresDialect) DriverName() string {
	return "postgres"
}

// OnConnect performs PostgreSQL-specific initialization.
// PostgreSQL doesn't require special initialization like SQLite's PRAGMA.
func (d *PostgresDialect) OnConnect(db *sqlx.DB) error {
	// PostgreSQL has foreign keys enabled by default
	return nil
}

// ==================== MySQL Dialect ====================

// MySQLDialect implements Dialect for MySQL database.
type MySQLDialect struct{}

// NewMySQLDialect creates a new MySQL dialect.
func NewMySQLDialect() *MySQLDialect {
	return &MySQLDialect{}
}

// DriverName returns "mysql".
func (d *MySQLDialect) DriverName() string {
	return "mysql"
}

// OnConnect performs MySQL-specific initialization.
func (d *MySQLDialect) OnConnect(db *sqlx.DB) error {
	// MySQL has foreign keys enabled by default (InnoDB engine)
	// Ensure we're using strict mode for better data integrity
	_, err := db.Exec("SET sql_mode = 'STRICT_TRANS_TABLES'")
	return err
}

// ==================== Dialect Factory ====================

// GetDialect returns the appropriate dialect for a given database type.
func GetDialect(dbType DBType) Dialect {
	switch dbType {
	case DBTypeSQLite:
		return NewSQLiteDialect()
	case DBTypePostgres:
		return NewPostgresDialect()
	case DBTypeMySQL:
		return NewMySQLDialect()
	default:
		// Default to SQLite for backward compatibility
		return NewSQLiteDialect()
	}
}
