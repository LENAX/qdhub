package repository_test

import (
	"os"
	"testing"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
)

// setupSyncRepoTestDB creates a temporary database for SyncPlanRepository testing
func setupSyncRepoTestDB(t *testing.T) (*persistence.DB, func()) {
	tmpfile, err := os.CreateTemp("", "test_sync_repo_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()

	dsn := tmpfile.Name()
	db, err := persistence.NewDB(dsn)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Create tables for SyncPlan aggregate
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS sync_plan (
			id VARCHAR(64) PRIMARY KEY,
			name VARCHAR(128) NOT NULL,
			description TEXT,
			data_source_id VARCHAR(64) NOT NULL,
			data_store_id VARCHAR(64),
			selected_apis TEXT NOT NULL,
			resolved_apis TEXT,
			execution_graph TEXT,
			cron_expression VARCHAR(128),
			default_execute_params TEXT,
			status VARCHAR(32) DEFAULT 'draft',
			last_executed_at TIMESTAMP,
			next_execute_at TIMESTAMP,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		CREATE TABLE IF NOT EXISTS sync_task (
			id VARCHAR(64) PRIMARY KEY,
			sync_plan_id VARCHAR(64) NOT NULL,
			api_name VARCHAR(128) NOT NULL,
			sync_mode VARCHAR(32) NOT NULL,
			params TEXT,
			param_mappings TEXT,
			dependencies TEXT,
			level INTEGER DEFAULT 0,
			sort_order INTEGER DEFAULT 0,
			sync_frequency INTEGER DEFAULT 0,
			last_synced_at TIMESTAMP,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (sync_plan_id) REFERENCES sync_plan(id) ON DELETE CASCADE
		);
		
		CREATE TABLE IF NOT EXISTS sync_execution (
			id VARCHAR(64) PRIMARY KEY,
			sync_plan_id VARCHAR(64) NOT NULL,
			workflow_inst_id VARCHAR(64) NOT NULL,
			status VARCHAR(32) DEFAULT 'pending',
			started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			finished_at TIMESTAMP,
			record_count INTEGER DEFAULT 0,
			error_message TEXT,
			execute_params TEXT,
			synced_apis TEXT,
			skipped_apis TEXT,
			FOREIGN KEY (sync_plan_id) REFERENCES sync_plan(id) ON DELETE CASCADE
		);
	`)
	if err != nil {
		db.Close()
		t.Fatalf("Failed to create test tables: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(dsn)
	}

	return db, cleanup
}

// TestSyncPlanRepository_WithTx_UseExternalTransaction tests that repository uses external transaction
func TestSyncPlanRepository_WithTx_UseExternalTransaction(t *testing.T) {
	db, cleanup := setupSyncRepoTestDB(t)
	defer cleanup()

	// Start a transaction
	tx, err := db.BeginTx()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Create repository with external transaction
	repo := repository.NewSyncPlanRepositoryWithTx(db, tx)

	// Create a test plan
	plan := sync.NewSyncPlan("Test Plan", "Test Description", shared.NewID(), []string{"api1"})

	// Create should use the external transaction
	err = repo.Create(plan)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	// Verify the plan was created in the transaction (but not committed yet)
	retrieved, err := repo.Get(plan.ID)
	if err != nil {
		t.Fatalf("Failed to get plan: %v", err)
	}
	if retrieved == nil {
		t.Fatal("Plan should be retrievable within transaction")
	}
	if retrieved.Name != plan.Name {
		t.Errorf("Expected name %s, got %s", plan.Name, retrieved.Name)
	}
}

// TestSyncPlanRepository_WithoutTx_UseInternalTransaction tests that repository creates internal transaction
func TestSyncPlanRepository_WithoutTx_UseInternalTransaction(t *testing.T) {
	db, cleanup := setupSyncRepoTestDB(t)
	defer cleanup()

	// Create repository without external transaction
	repo := repository.NewSyncPlanRepository(db)

	// Create a test plan
	plan := sync.NewSyncPlan("Test Plan", "Test Description", shared.NewID(), []string{"api1"})

	// Create should create its own transaction and commit
	err := repo.Create(plan)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	// Verify the plan was committed and is retrievable
	retrieved, err := repo.Get(plan.ID)
	if err != nil {
		t.Fatalf("Failed to get plan: %v", err)
	}
	if retrieved == nil {
		t.Fatal("Plan should be retrievable after commit")
	}
	if retrieved.Name != plan.Name {
		t.Errorf("Expected name %s, got %s", plan.Name, retrieved.Name)
	}
}

// TestSyncPlanRepository_WithTx_UpdateUsesExternalTransaction tests Update with external transaction
func TestSyncPlanRepository_WithTx_UpdateUsesExternalTransaction(t *testing.T) {
	db, cleanup := setupSyncRepoTestDB(t)
	defer cleanup()

	// First create a plan without transaction
	repo := repository.NewSyncPlanRepository(db)
	plan := sync.NewSyncPlan("Test Plan", "Test Description", shared.NewID(), []string{"api1"})
	if err := repo.Create(plan); err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	// Start a transaction
	tx, err := db.BeginTx()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Create repository with external transaction
	txRepo := repository.NewSyncPlanRepositoryWithTx(db, tx)

	// Update the plan
	plan.Name = "Updated Plan"
	err = txRepo.Update(plan)
	if err != nil {
		t.Fatalf("Failed to update plan: %v", err)
	}

	// Verify update is visible within transaction
	retrieved, err := txRepo.Get(plan.ID)
	if err != nil {
		t.Fatalf("Failed to get plan: %v", err)
	}
	if retrieved.Name != "Updated Plan" {
		t.Errorf("Expected name 'Updated Plan', got %s", retrieved.Name)
	}

	// Verify update is NOT visible outside transaction (not committed)
	retrieved2, err := repo.Get(plan.ID)
	if err != nil {
		t.Fatalf("Failed to get plan: %v", err)
	}
	if retrieved2.Name == "Updated Plan" {
		t.Error("Update should not be visible outside transaction before commit")
	}
}
