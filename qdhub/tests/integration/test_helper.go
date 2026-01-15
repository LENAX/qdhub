//go:build integration
// +build integration

package integration

import (
	"os"
	"testing"

	"qdhub/internal/infrastructure/persistence"
)

// setupIntegrationDB creates a database with full migrations for integration testing
func setupIntegrationDB(t *testing.T) (*persistence.DB, func()) {
	tmpfile, err := os.CreateTemp("", "integration_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()

	dsn := tmpfile.Name()
	db, err := persistence.NewDB(dsn)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Read and execute the full migration
	migrationSQL, err := os.ReadFile("../../migrations/001_init_schema.up.sql")
	if err != nil {
		db.Close()
		t.Fatalf("Failed to read migration file: %v", err)
	}

	_, err = db.Exec(string(migrationSQL))
	if err != nil {
		db.Close()
		t.Fatalf("Failed to execute migration: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(dsn)
	}

	return db, cleanup
}
