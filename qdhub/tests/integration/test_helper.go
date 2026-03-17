//go:build integration
// +build integration

package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"qdhub/internal/infrastructure/persistence"
)

// defaultMigrationsDir 为集成测试的迁移目录，与 container 行为一致。
// 需在模块根目录（qdhub）下执行 go test，即 go test ./tests/integration/...
const defaultMigrationsDir = "migrations"

// setupIntegrationDB creates a database with full migrations for integration testing.
// Migrations are discovered by scanning *.up.sql under defaultMigrationsDir (same as container).
func setupIntegrationDB(t *testing.T) (*persistence.DB, func()) {
	t.Helper()
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

	if err := runMigrations(t, db, defaultMigrationsDir, "sqlite"); err != nil {
		db.Close()
		t.Fatalf("Failed to run migrations: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(dsn)
	}
	return db, cleanup
}

// runMigrations 扫描 migrationsDir 下所有 *.up.sql，按 driver 过滤后按文件名升序执行，与 container.runMigrations 逻辑一致。
func runMigrations(t *testing.T, db *persistence.DB, migrationsDir string, driver string) error {
	t.Helper()
	if err := ensureSchemaMigrationsTable(db, driver); err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}

	upFiles, err := filepath.Glob(filepath.Join(migrationsDir, "*.up.sql"))
	if err != nil {
		return fmt.Errorf("glob migrations: %w", err)
	}
	upFiles = filterMigrationsByDriver(upFiles, driver)
	if len(upFiles) == 0 {
		return nil
	}
	sort.Strings(upFiles)

	for _, file := range upFiles {
		version := migrationVersionFromFile(file)
		applied, err := isMigrationApplied(db, version)
		if err != nil {
			return fmt.Errorf("check migration %s: %w", version, err)
		}
		if applied {
			continue
		}

		migrationSQL, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read %s: %w", file, err)
		}
		if _, err := db.Exec(string(migrationSQL)); err != nil {
			msg := err.Error()
			if strings.Contains(msg, "already exists") || strings.Contains(msg, "Already exists") ||
				strings.Contains(msg, "duplicate column") || strings.Contains(msg, "Duplicate column") {
				_ = recordMigration(db, version)
				continue
			}
			return fmt.Errorf("apply %s: %w", file, err)
		}
		if err := recordMigration(db, version); err != nil {
			return fmt.Errorf("record %s: %w", version, err)
		}
	}
	return nil
}

func ensureSchemaMigrationsTable(db *persistence.DB, driver string) error {
	sql := `CREATE TABLE IF NOT EXISTS schema_migrations (
	version TEXT PRIMARY KEY,
	applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`
	_, err := db.Exec(sql)
	return err
}

func migrationVersionFromFile(path string) string {
	base := filepath.Base(path)
	return strings.TrimSuffix(base, ".up.sql")
}

func isMigrationApplied(db *persistence.DB, version string) (bool, error) {
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM schema_migrations WHERE version = '%s';", version)
	if err := db.Get(&count, query); err != nil {
		return false, err
	}
	return count > 0, nil
}

func recordMigration(db *persistence.DB, version string) error {
	sql := fmt.Sprintf("INSERT INTO schema_migrations (version) VALUES ('%s');", version)
	_, err := db.Exec(sql)
	if err != nil {
		msg := err.Error()
		if strings.Contains(msg, "UNIQUE constraint failed") ||
			strings.Contains(msg, "duplicate key") ||
			strings.Contains(msg, "Duplicate entry") {
			return nil
		}
		return err
	}
	return nil
}

func filterMigrationsByDriver(files []string, driver string) []string {
	if driver == "" {
		driver = "sqlite"
	}
	var out []string
	for _, f := range files {
		base := filepath.Base(f)
		if strings.Contains(base, ".sqlite.") {
			if driver == "sqlite" {
				out = append(out, f)
			}
			continue
		}
		if strings.Contains(base, ".postgres.") {
			if driver == "postgres" {
				out = append(out, f)
			}
			continue
		}
		if strings.Contains(base, ".mysql.") {
			if driver == "mysql" {
				out = append(out, f)
			}
			continue
		}
		out = append(out, f)
	}
	return out
}

// runAuthMigration runs the auth schema migration (002) on the given DB.
// Used by HTTP/watchlist integration tests that need JWT-protected routes.
// When setupIntegrationDB is used, 002 is already applied via runMigrations; this is for custom setups.
func runAuthMigration(t *testing.T, db *persistence.DB) {
	t.Helper()
	migrationSQL, err := os.ReadFile(filepath.Join(defaultMigrationsDir, "002_auth_schema.sqlite.up.sql"))
	if err != nil {
		t.Fatalf("Failed to read auth migration file: %v", err)
	}
	_, err = db.Exec(string(migrationSQL))
	if err != nil {
		t.Fatalf("Failed to execute auth migration: %v", err)
	}
}
