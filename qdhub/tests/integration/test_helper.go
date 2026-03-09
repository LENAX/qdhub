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

	// Read and execute sync_plan migration
	syncPlanMigrationSQL, err := os.ReadFile("../../migrations/003_sync_plan_migration.up.sql")
	if err != nil {
		db.Close()
		t.Fatalf("Failed to read sync_plan migration file: %v", err)
	}

	_, err = db.Exec(string(syncPlanMigrationSQL))
	if err != nil {
		db.Close()
		t.Fatalf("Failed to execute sync_plan migration: %v", err)
	}

	// Read and execute api_sync_strategy migration
	strategyMigrationSQL, err := os.ReadFile("../../migrations/004_api_sync_strategy.up.sql")
	if err != nil {
		db.Close()
		t.Fatalf("Failed to read api_sync_strategy migration file: %v", err)
	}

	_, err = db.Exec(string(strategyMigrationSQL))
	if err != nil {
		db.Close()
		t.Fatalf("Failed to execute api_sync_strategy migration: %v", err)
	}

	// Sync plan default execute params (for scheduled runs)
	defaultParamsMigrationSQL, err := os.ReadFile("../../migrations/005_sync_plan_default_params.up.sql")
	if err != nil {
		db.Close()
		t.Fatalf("Failed to read 005_sync_plan_default_params migration: %v", err)
	}
	_, err = db.Exec(string(defaultParamsMigrationSQL))
	if err != nil {
		db.Close()
		t.Fatalf("Failed to execute 005_sync_plan_default_params migration: %v", err)
	}

	// Incremental sync plan fields (mode, last_successful_end_date)
	incrementalModeSQL, err := os.ReadFile("../../migrations/010_sync_plan_incremental_mode.up.sql")
	if err != nil {
		db.Close()
		t.Fatalf("Failed to read 010_sync_plan_incremental_mode migration file: %v", err)
	}
	_, err = db.Exec(string(incrementalModeSQL))
	if err != nil {
		db.Close()
		t.Fatalf("Failed to execute 010_sync_plan_incremental_mode migration: %v", err)
	}

	// Sync plan incremental start date source columns
	incrementalStartSQL, err := os.ReadFile("../../migrations/015_sync_plan_incremental_start_date_source.up.sql")
	if err != nil {
		db.Close()
		t.Fatalf("Failed to read 015_sync_plan_incremental_start_date_source migration file: %v", err)
	}
	_, err = db.Exec(string(incrementalStartSQL))
	if err != nil {
		db.Close()
		t.Fatalf("Failed to execute 015_sync_plan_incremental_start_date_source migration: %v", err)
	}

	// DataSource common_data_apis column
	commonDataSQL, err := os.ReadFile("../../migrations/011_data_source_common_data_apis.up.sql")
	if err != nil {
		db.Close()
		t.Fatalf("Failed to read 011_data_source_common_data_apis migration file: %v", err)
	}
	_, err = db.Exec(string(commonDataSQL))
	if err != nil {
		db.Close()
		t.Fatalf("Failed to execute 011_data_source_common_data_apis migration: %v", err)
	}

	// API Sync Strategy fixed params columns
	fixedParamsSQL, err := os.ReadFile("../../migrations/016_api_sync_strategy_fixed_params.up.sql")
	if err != nil {
		db.Close()
		t.Fatalf("Failed to read 016_api_sync_strategy_fixed_params migration file: %v", err)
	}
	_, err = db.Exec(string(fixedParamsSQL))
	if err != nil {
		db.Close()
		t.Fatalf("Failed to execute 016_api_sync_strategy_fixed_params migration: %v", err)
	}

	// Sync plan mode (batch/realtime) and pull_interval / schedule window
	planModeSQL, err := os.ReadFile("../../migrations/006_sync_plan_mode_realtime.up.sql")
	if err != nil {
		db.Close()
		t.Fatalf("Failed to read 006_sync_plan_mode_realtime migration: %v", err)
	}
	_, err = db.Exec(string(planModeSQL))
	if err != nil {
		db.Close()
		t.Fatalf("Failed to execute 006_sync_plan_mode_realtime migration: %v", err)
	}
	pullIntervalSQL, err := os.ReadFile("../../migrations/020_sync_plan_pull_interval_and_schedule_window.up.sql")
	if err != nil {
		db.Close()
		t.Fatalf("Failed to read 020_sync_plan_pull_interval_and_schedule_window migration: %v", err)
	}
	_, err = db.Exec(string(pullIntervalSQL))
	if err != nil {
		db.Close()
		t.Fatalf("Failed to execute 020_sync_plan_pull_interval_and_schedule_window migration: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(dsn)
	}

	return db, cleanup
}

// runAuthMigration runs the auth schema migration (002) on the given DB.
// Used by HTTP integration tests that need JWT-protected routes.
func runAuthMigration(t *testing.T, db *persistence.DB) {
	t.Helper()
	migrationSQL, err := os.ReadFile("../../migrations/002_auth_schema.sqlite.up.sql")
	if err != nil {
		t.Fatalf("Failed to read auth migration file: %v", err)
	}
	_, err = db.Exec(string(migrationSQL))
	if err != nil {
		t.Fatalf("Failed to execute auth migration: %v", err)
	}
}
