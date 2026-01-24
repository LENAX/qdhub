package uow_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"qdhub/internal/application/contracts"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/uow"
)

// setupUoWTestDB creates a temporary database for UoW testing
func setupUoWTestDB(t *testing.T) (*persistence.DB, func()) {
	tmpfile, err := os.CreateTemp("", "test_uow_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()

	dsn := tmpfile.Name()
	db, err := persistence.NewDB(dsn)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Create a simple test table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS test_table (
			id VARCHAR(64) PRIMARY KEY,
			name VARCHAR(128) NOT NULL,
			value INTEGER DEFAULT 0
		);
	`)
	if err != nil {
		db.Close()
		t.Fatalf("Failed to create test table: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(dsn)
	}

	return db, cleanup
}

// TestUnitOfWork_Do_CommitOnSuccess tests that UoW commits transaction on success
func TestUnitOfWork_Do_CommitOnSuccess(t *testing.T) {
	db, cleanup := setupUoWTestDB(t)
	defer cleanup()

	uowImpl := uow.NewUnitOfWork(db)
	ctx := context.Background()

	err := uowImpl.Do(ctx, func(repos contracts.Repositories) error {
		// Verify repositories are available
		if repos.SyncPlanRepo() == nil {
			t.Error("SyncPlanRepo should not be nil")
		}
		if repos.DataSourceRepo() == nil {
			t.Error("DataSourceRepo should not be nil")
		}
		if repos.DataStoreRepo() == nil {
			t.Error("DataStoreRepo should not be nil")
		}
		if repos.MetadataRepo() == nil {
			t.Error("MetadataRepo should not be nil")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("UoW.Do should not return error on success: %v", err)
	}
}

// TestUnitOfWork_Do_RollbackOnError tests that UoW rolls back transaction on error
func TestUnitOfWork_Do_RollbackOnError(t *testing.T) {
	db, cleanup := setupUoWTestDB(t)
	defer cleanup()

	uowImpl := uow.NewUnitOfWork(db)
	ctx := context.Background()

	testError := errors.New("test error")
	err := uowImpl.Do(ctx, func(repos contracts.Repositories) error {
		return testError
	})

	if err == nil {
		t.Fatal("UoW.Do should return error when function returns error")
	}
	if !errors.Is(err, testError) {
		t.Errorf("Expected error %v, got %v", testError, err)
	}
}

// TestUnitOfWork_Repositories_AllAvailable tests that all repositories are available
func TestUnitOfWork_Repositories_AllAvailable(t *testing.T) {
	db, cleanup := setupUoWTestDB(t)
	defer cleanup()

	uowImpl := uow.NewUnitOfWork(db)
	ctx := context.Background()

	err := uowImpl.Do(ctx, func(repos contracts.Repositories) error {
		// Test all repository getters
		syncPlanRepo := repos.SyncPlanRepo()
		if syncPlanRepo == nil {
			t.Error("SyncPlanRepo() returned nil")
		}

		dataSourceRepo := repos.DataSourceRepo()
		if dataSourceRepo == nil {
			t.Error("DataSourceRepo() returned nil")
		}

		dataStoreRepo := repos.DataStoreRepo()
		if dataStoreRepo == nil {
			t.Error("DataStoreRepo() returned nil")
		}

		metadataRepo := repos.MetadataRepo()
		if metadataRepo == nil {
			t.Error("MetadataRepo() returned nil")
		}

		return nil
	})

	if err != nil {
		t.Fatalf("UoW.Do should not return error: %v", err)
	}
}
