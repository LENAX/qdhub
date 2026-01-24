//go:build integration
// +build integration

package integration

import (
	"context"
	"errors"
	"testing"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/persistence/uow"
)

// TestUnitOfWork_Integration_CommitMultipleWrites tests that multiple writes in UoW are all committed
func TestUnitOfWork_Integration_CommitMultipleWrites(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()
	uowImpl := uow.NewUnitOfWork(db)

	// Create a data source first
	dataSourceRepo := repository.NewDataSourceRepository(db)
	dataSource := metadata.NewDataSource("Tushare", "Test", "https://api.tushare.pro", "https://doc.tushare.pro")
	if err := dataSourceRepo.Create(dataSource); err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	// Create sync plan and tasks in a single transaction
	var planID shared.ID
	err := uowImpl.Do(ctx, func(repos contracts.Repositories) error {
		// Create sync plan
		plan := sync.NewSyncPlan("Test Plan", "Test Description", dataSource.ID, []string{"api1", "api2"})
		if err := repos.SyncPlanRepo().Create(plan); err != nil {
			return err
		}
		planID = plan.ID

		// Create tasks
		task1 := sync.NewSyncTask("api1", sync.TaskSyncModeDirect, 0)
		task1.SyncPlanID = planID
		if err := repos.SyncPlanRepo().AddTask(task1); err != nil {
			return err
		}

		task2 := sync.NewSyncTask("api2", sync.TaskSyncModeDirect, 0)
		task2.SyncPlanID = planID
		if err := repos.SyncPlanRepo().AddTask(task2); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		t.Fatalf("UoW.Do should not return error: %v", err)
	}

	// Verify all writes were committed
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	plan, err := syncPlanRepo.Get(planID)
	if err != nil {
		t.Fatalf("Failed to get plan: %v", err)
	}
	if plan == nil {
		t.Fatal("Plan should exist after commit")
	}

	tasks, err := syncPlanRepo.GetTasksByPlan(planID)
	if err != nil {
		t.Fatalf("Failed to get tasks: %v", err)
	}
	if len(tasks) != 2 {
		t.Errorf("Expected 2 tasks, got %d", len(tasks))
	}
}

// TestUnitOfWork_Integration_RollbackOnError tests that all writes are rolled back on error
func TestUnitOfWork_Integration_RollbackOnError(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()
	uowImpl := uow.NewUnitOfWork(db)

	// Create a data source first
	dataSourceRepo := repository.NewDataSourceRepository(db)
	dataSource := metadata.NewDataSource("Tushare", "Test", "https://api.tushare.pro", "https://doc.tushare.pro")
	if err := dataSourceRepo.Create(dataSource); err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	// Try to create sync plan and then fail
	var planID shared.ID
	testError := errors.New("simulated error")
	err := uowImpl.Do(ctx, func(repos contracts.Repositories) error {
		// Create sync plan
		plan := sync.NewSyncPlan("Test Plan", "Test Description", dataSource.ID, []string{"api1"})
		if err := repos.SyncPlanRepo().Create(plan); err != nil {
			return err
		}
		planID = plan.ID

		// Create a task
		task := sync.NewSyncTask("api1", sync.TaskSyncModeDirect, 0)
		task.SyncPlanID = planID
		if err := repos.SyncPlanRepo().AddTask(task); err != nil {
			return err
		}

		// Return error to trigger rollback
		return testError
	})

	if err == nil {
		t.Fatal("UoW.Do should return error when function returns error")
	}
	if !errors.Is(err, testError) {
		t.Errorf("Expected error %v, got %v", testError, err)
	}

	// Verify all writes were rolled back
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	plan, err := syncPlanRepo.Get(planID)
	if err != nil {
		t.Fatalf("Failed to get plan: %v", err)
	}
	if plan != nil {
		t.Error("Plan should not exist after rollback")
	}

	tasks, err := syncPlanRepo.GetTasksByPlan(planID)
	if err != nil {
		t.Fatalf("Failed to get tasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Errorf("Expected 0 tasks after rollback, got %d", len(tasks))
	}
}
