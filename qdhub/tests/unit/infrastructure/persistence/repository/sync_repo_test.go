package repository_test

import (
	"os"
	"testing"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
)

// setupSyncTestDB creates a temporary database for Sync testing
func setupSyncTestDB(t *testing.T) (*persistence.DB, func()) {
	tmpfile, err := os.CreateTemp("", "test_sync_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()

	dsn := tmpfile.Name()
	db, err := persistence.NewDB(dsn)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Create tables for Sync aggregate
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS sync_jobs (
			id VARCHAR(64) PRIMARY KEY,
			name VARCHAR(128) NOT NULL,
			description TEXT,
			api_meta_id VARCHAR(64) NOT NULL,
			data_store_id VARCHAR(64) NOT NULL,
			workflow_def_id VARCHAR(64),
			mode VARCHAR(32) NOT NULL,
			cron_expression VARCHAR(128),
			params TEXT,
			param_rules TEXT,
			status VARCHAR(32) DEFAULT 'disabled',
			last_run_at TIMESTAMP,
			next_run_at TIMESTAMP,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		CREATE TABLE IF NOT EXISTS sync_executions (
			id VARCHAR(64) PRIMARY KEY,
			sync_job_id VARCHAR(64) NOT NULL,
			workflow_inst_id VARCHAR(64),
			status VARCHAR(32) NOT NULL,
			started_at TIMESTAMP NOT NULL,
			finished_at TIMESTAMP,
			record_count INTEGER DEFAULT 0,
			error_message TEXT,
			FOREIGN KEY (sync_job_id) REFERENCES sync_jobs(id) ON DELETE CASCADE
		);
	`)
	if err != nil {
		db.Close()
		t.Fatalf("Failed to create tables: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(dsn)
	}

	return db, cleanup
}

func TestSyncJobRepository_Create(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Test Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)

	err := repo.Create(job)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if job.ID.IsEmpty() {
		t.Error("SyncJob ID should be set after creation")
	}
}

func TestSyncJobRepository_Create_WithExecutions(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Test Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)

	// Add execution
	exec := sync.NewSyncExecution(job.ID, shared.NewID())
	job.Executions = []sync.SyncExecution{*exec}

	err := repo.Create(job)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Verify execution was created
	got, err := repo.Get(job.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if len(got.Executions) != 1 {
		t.Errorf("Executions count = %d, want 1", len(got.Executions))
	}
}

func TestSyncJobRepository_Get(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Test Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	err := repo.Create(job)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := repo.Get(job.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got == nil {
		t.Fatal("Get() returned nil")
	}

	if got.ID != job.ID {
		t.Errorf("Get() ID = %s, want %s", got.ID, job.ID)
	}

	if got.Name != "Test Job" {
		t.Errorf("Get() Name = %s, want Test Job", got.Name)
	}
}

func TestSyncJobRepository_Get_NotFound(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	got, err := repo.Get(shared.NewID())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got != nil {
		t.Error("Get() should return nil for non-existent ID")
	}
}

func TestSyncJobRepository_Update(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Original Name", "Original Desc", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	err := repo.Create(job)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Update job
	cronExpr := "0 0 * * *"
	job.SetCronExpression(cronExpr)
	err = repo.Update(job)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := repo.Get(job.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got.CronExpression == nil || *got.CronExpression != cronExpr {
		t.Errorf("Update() CronExpression = %v, want %s", got.CronExpression, cronExpr)
	}
}

func TestSyncJobRepository_Delete(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("To Delete", "Desc", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	err := repo.Create(job)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = repo.Delete(job.ID)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	got, err := repo.Get(job.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got != nil {
		t.Error("Delete() should remove the sync job")
	}
}

func TestSyncJobRepository_Delete_Cascade(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Test Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	exec := sync.NewSyncExecution(job.ID, shared.NewID())
	job.Executions = []sync.SyncExecution{*exec}

	err := repo.Create(job)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Delete should cascade to executions
	err = repo.Delete(job.ID)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Verify sync job is deleted
	got, err := repo.Get(job.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got != nil {
		t.Error("Delete() should remove the sync job and cascade to executions")
	}
}

func TestSyncJobRepository_List(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	apiMetaID1 := shared.NewID()
	dataStoreID1 := shared.NewID()
	workflowDefID1 := shared.NewID()
	job1 := sync.NewSyncJob("Job 1", "Desc 1", apiMetaID1, dataStoreID1, workflowDefID1, sync.SyncModeBatch)

	apiMetaID2 := shared.NewID()
	dataStoreID2 := shared.NewID()
	workflowDefID2 := shared.NewID()
	job2 := sync.NewSyncJob("Job 2", "Desc 2", apiMetaID2, dataStoreID2, workflowDefID2, sync.SyncModeBatch)

	err := repo.Create(job1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = repo.Create(job2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	list, err := repo.List()
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if len(list) < 2 {
		t.Errorf("List() returned %d sync jobs, want at least 2", len(list))
	}
}

// Note: SyncExecutionRepository has been integrated into SyncJobRepository
// following DDD aggregate patterns. Tests below use SyncJobRepository methods.

// ==================== Extended Query Method Tests ====================

func TestSyncJobRepository_FindBy(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	// Create jobs with different statuses
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()

	job1 := sync.NewSyncJob("Job 1", "Desc 1", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	job1.Enable()
	repo.Create(job1)

	job2 := sync.NewSyncJob("Job 2", "Desc 2", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	// job2 remains disabled
	repo.Create(job2)

	t.Run("FindBy status", func(t *testing.T) {
		jobs, err := repo.FindBy(shared.Eq("status", string(sync.JobStatusEnabled)))
		if err != nil {
			t.Fatalf("FindBy() error = %v", err)
		}
		if len(jobs) != 1 {
			t.Errorf("FindBy() returned %d jobs, want 1", len(jobs))
		}
		if len(jobs) > 0 && jobs[0].Status != sync.JobStatusEnabled {
			t.Errorf("FindBy() returned job with status %s, want enabled", jobs[0].Status)
		}
	})

	t.Run("FindBy mode", func(t *testing.T) {
		jobs, err := repo.FindBy(shared.Eq("mode", string(sync.SyncModeBatch)))
		if err != nil {
			t.Fatalf("FindBy() error = %v", err)
		}
		if len(jobs) != 2 {
			t.Errorf("FindBy() returned %d jobs, want 2", len(jobs))
		}
	})
}

func TestSyncJobRepository_Count(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	// Create jobs
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()

	for i := 0; i < 5; i++ {
		job := sync.NewSyncJob("Job", "Desc", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
		repo.Create(job)
	}

	t.Run("Count all", func(t *testing.T) {
		count, err := repo.Count()
		if err != nil {
			t.Fatalf("Count() error = %v", err)
		}
		if count != 5 {
			t.Errorf("Count() = %d, want 5", count)
		}
	})

	t.Run("Count with condition", func(t *testing.T) {
		count, err := repo.Count(shared.Eq("status", string(sync.JobStatusDisabled)))
		if err != nil {
			t.Fatalf("Count() error = %v", err)
		}
		if count != 5 {
			t.Errorf("Count() = %d, want 5", count)
		}
	})
}

func TestSyncJobRepository_Exists(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	t.Run("Exists false when empty", func(t *testing.T) {
		exists, err := repo.Exists()
		if err != nil {
			t.Fatalf("Exists() error = %v", err)
		}
		if exists {
			t.Error("Exists() = true, want false for empty repository")
		}
	})

	// Create a job
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Job", "Desc", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	repo.Create(job)

	t.Run("Exists true when data exists", func(t *testing.T) {
		exists, err := repo.Exists()
		if err != nil {
			t.Fatalf("Exists() error = %v", err)
		}
		if !exists {
			t.Error("Exists() = false, want true")
		}
	})

	t.Run("Exists with condition", func(t *testing.T) {
		exists, err := repo.Exists(shared.Eq("status", string(sync.JobStatusDisabled)))
		if err != nil {
			t.Fatalf("Exists() error = %v", err)
		}
		if !exists {
			t.Error("Exists() = false, want true for disabled job")
		}

		exists, err = repo.Exists(shared.Eq("status", string(sync.JobStatusEnabled)))
		if err != nil {
			t.Fatalf("Exists() error = %v", err)
		}
		if exists {
			t.Error("Exists() = true, want false for enabled job (none exist)")
		}
	})
}

func TestSyncJobRepository_ListWithPagination(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	// Create 10 jobs
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()

	for i := 0; i < 10; i++ {
		job := sync.NewSyncJob("Job", "Desc", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
		repo.Create(job)
	}

	t.Run("First page", func(t *testing.T) {
		pagination := shared.Pagination{Page: 1, PageSize: 3}
		result, err := repo.ListWithPagination(pagination)
		if err != nil {
			t.Fatalf("ListWithPagination() error = %v", err)
		}
		if result.Total != 10 {
			t.Errorf("Total = %d, want 10", result.Total)
		}
		if len(result.Items) != 3 {
			t.Errorf("Items count = %d, want 3", len(result.Items))
		}
		if result.TotalPages != 4 {
			t.Errorf("TotalPages = %d, want 4", result.TotalPages)
		}
	})

	t.Run("Last page", func(t *testing.T) {
		pagination := shared.Pagination{Page: 4, PageSize: 3}
		result, err := repo.ListWithPagination(pagination)
		if err != nil {
			t.Fatalf("ListWithPagination() error = %v", err)
		}
		if len(result.Items) != 1 {
			t.Errorf("Items count = %d, want 1 (last page)", len(result.Items))
		}
	})
}

func TestSyncJobRepository_FindByWithPagination(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	// Create jobs with different modes
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()

	for i := 0; i < 5; i++ {
		job := sync.NewSyncJob("Batch Job", "Desc", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
		repo.Create(job)
	}

	for i := 0; i < 3; i++ {
		job := sync.NewSyncJob("Realtime Job", "Desc", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeRealtime)
		repo.Create(job)
	}

	t.Run("FindBy with pagination", func(t *testing.T) {
		pagination := shared.Pagination{Page: 1, PageSize: 2}
		result, err := repo.FindByWithPagination(pagination, shared.Eq("mode", string(sync.SyncModeBatch)))
		if err != nil {
			t.Fatalf("FindByWithPagination() error = %v", err)
		}
		if result.Total != 5 {
			t.Errorf("Total = %d, want 5", result.Total)
		}
		if len(result.Items) != 2 {
			t.Errorf("Items count = %d, want 2", len(result.Items))
		}
	})
}

func TestSyncJobRepository_FindByWithOrder(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	// Create jobs
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()

	job1 := sync.NewSyncJob("A Job", "Desc", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	repo.Create(job1)

	job2 := sync.NewSyncJob("B Job", "Desc", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	repo.Create(job2)

	job3 := sync.NewSyncJob("C Job", "Desc", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	repo.Create(job3)

	t.Run("Order by name ascending", func(t *testing.T) {
		jobs, err := repo.FindByWithOrder([]shared.OrderBy{{Field: "name", Order: shared.SortAsc}})
		if err != nil {
			t.Fatalf("FindByWithOrder() error = %v", err)
		}
		if len(jobs) != 3 {
			t.Fatalf("FindByWithOrder() returned %d jobs, want 3", len(jobs))
		}
		if jobs[0].Name != "A Job" {
			t.Errorf("First job name = %s, want 'A Job'", jobs[0].Name)
		}
	})

	t.Run("Order by name descending", func(t *testing.T) {
		jobs, err := repo.FindByWithOrder([]shared.OrderBy{{Field: "name", Order: shared.SortDesc}})
		if err != nil {
			t.Fatalf("FindByWithOrder() error = %v", err)
		}
		if len(jobs) != 3 {
			t.Fatalf("FindByWithOrder() returned %d jobs, want 3", len(jobs))
		}
		if jobs[0].Name != "C Job" {
			t.Errorf("First job name = %s, want 'C Job'", jobs[0].Name)
		}
	})
}

// ==================== Child Entity Tests (AddExecution, GetExecution, etc.) ====================

func TestSyncJobRepository_AddExecution(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	// Create job
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Desc", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	repo.Create(job)

	// Add execution via aggregate repository
	exec := sync.NewSyncExecution(job.ID, shared.NewID())
	err := repo.AddExecution(exec)
	if err != nil {
		t.Fatalf("AddExecution() error = %v", err)
	}

	// Verify
	got, err := repo.GetExecution(exec.ID)
	if err != nil {
		t.Fatalf("GetExecution() error = %v", err)
	}
	if got == nil {
		t.Fatal("GetExecution() returned nil")
	}
	if got.ID != exec.ID {
		t.Errorf("GetExecution() ID = %s, want %s", got.ID, exec.ID)
	}
}

func TestSyncJobRepository_GetExecutionsByJob(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	// Create job
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Desc", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	repo.Create(job)

	// Add multiple executions
	for i := 0; i < 3; i++ {
		exec := sync.NewSyncExecution(job.ID, shared.NewID())
		repo.AddExecution(exec)
	}

	// Get executions
	execs, err := repo.GetExecutionsByJob(job.ID)
	if err != nil {
		t.Fatalf("GetExecutionsByJob() error = %v", err)
	}
	if len(execs) != 3 {
		t.Errorf("GetExecutionsByJob() returned %d executions, want 3", len(execs))
	}
}

func TestSyncJobRepository_UpdateExecution(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	// Create job and execution
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Desc", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	repo.Create(job)

	exec := sync.NewSyncExecution(job.ID, shared.NewID())
	repo.AddExecution(exec)

	// Update execution
	exec.MarkSuccess(500)
	err := repo.UpdateExecution(exec)
	if err != nil {
		t.Fatalf("UpdateExecution() error = %v", err)
	}

	// Verify
	got, _ := repo.GetExecution(exec.ID)
	if got.Status != sync.ExecStatusSuccess {
		t.Errorf("UpdateExecution() Status = %s, want success", got.Status)
	}
	if got.RecordCount != 500 {
		t.Errorf("UpdateExecution() RecordCount = %d, want 500", got.RecordCount)
	}
}
