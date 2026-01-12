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

func TestSyncExecutionRepository_Create(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	// First create a sync job
	jobRepo := repository.NewSyncJobRepository(db)
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Test Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	err := jobRepo.Create(job)
	if err != nil {
		t.Fatalf("Failed to create sync job: %v", err)
	}

	// Create execution repository
	execRepo := repository.NewSyncExecutionRepository(db)
	workflowInstID := shared.NewID()
	exec := sync.NewSyncExecution(job.ID, workflowInstID)

	err = execRepo.Create(exec)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if exec.ID.IsEmpty() {
		t.Error("SyncExecution ID should be set after creation")
	}
}

func TestSyncExecutionRepository_Get(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	jobRepo := repository.NewSyncJobRepository(db)
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Test Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	err := jobRepo.Create(job)
	if err != nil {
		t.Fatalf("Failed to create sync job: %v", err)
	}

	execRepo := repository.NewSyncExecutionRepository(db)
	workflowInstID := shared.NewID()
	exec := sync.NewSyncExecution(job.ID, workflowInstID)
	err = execRepo.Create(exec)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := execRepo.Get(exec.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got == nil {
		t.Fatal("Get() returned nil")
	}

	if got.ID != exec.ID {
		t.Errorf("Get() ID = %s, want %s", got.ID, exec.ID)
	}
}

func TestSyncExecutionRepository_GetBySyncJob(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	jobRepo := repository.NewSyncJobRepository(db)
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Test Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	err := jobRepo.Create(job)
	if err != nil {
		t.Fatalf("Failed to create sync job: %v", err)
	}

	execRepo := repository.NewSyncExecutionRepository(db)
	workflowInstID1 := shared.NewID()
	workflowInstID2 := shared.NewID()
	exec1 := sync.NewSyncExecution(job.ID, workflowInstID1)
	exec2 := sync.NewSyncExecution(job.ID, workflowInstID2)

	err = execRepo.Create(exec1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = execRepo.Create(exec2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	executions, err := execRepo.GetBySyncJob(job.ID)
	if err != nil {
		t.Fatalf("GetBySyncJob() error = %v", err)
	}

	if len(executions) < 2 {
		t.Errorf("GetBySyncJob() returned %d executions, want at least 2", len(executions))
	}
}

func TestSyncExecutionRepository_Update(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	jobRepo := repository.NewSyncJobRepository(db)
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Test Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	err := jobRepo.Create(job)
	if err != nil {
		t.Fatalf("Failed to create sync job: %v", err)
	}

	execRepo := repository.NewSyncExecutionRepository(db)
	workflowInstID := shared.NewID()
	exec := sync.NewSyncExecution(job.ID, workflowInstID)
	err = execRepo.Create(exec)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	exec.MarkSuccess(100)
	err = execRepo.Update(exec)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := execRepo.Get(exec.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got.Status != sync.ExecStatusSuccess {
		t.Errorf("Update() Status = %s, want success", got.Status)
	}

	if got.RecordCount != 100 {
		t.Errorf("Update() RecordCount = %d, want 100", got.RecordCount)
	}
}

func TestSyncExecutionRepository_Delete(t *testing.T) {
	db, cleanup := setupSyncTestDB(t)
	defer cleanup()

	jobRepo := repository.NewSyncJobRepository(db)
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Test Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	err := jobRepo.Create(job)
	if err != nil {
		t.Fatalf("Failed to create sync job: %v", err)
	}

	execRepo := repository.NewSyncExecutionRepository(db)
	workflowInstID := shared.NewID()
	exec := sync.NewSyncExecution(job.ID, workflowInstID)
	err = execRepo.Create(exec)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = execRepo.Delete(exec.ID)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	got, err := execRepo.Get(exec.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got != nil {
		t.Error("Delete() should remove the execution")
	}
}
