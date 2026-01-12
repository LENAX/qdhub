package dao_test

import (
	"testing"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/infrastructure/persistence/dao"
)

func TestSyncJobDAO_Create(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	// Add sync_jobs table
	_, err := db.Exec(`
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
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewSyncJobDAO(db.DB)

	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Test Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)

	err = dao.Create(nil, job)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if job.ID.IsEmpty() {
		t.Error("SyncJob ID should be set")
	}
}

func TestSyncJobDAO_GetByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewSyncJobDAO(db.DB)

	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Test Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	err = dao.Create(nil, job)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := dao.GetByID(nil, job.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got == nil {
		t.Fatal("GetByID() returned nil")
	}

	if got.ID != job.ID {
		t.Errorf("GetByID() ID = %s, want %s", got.ID, job.ID)
	}
}

func TestSyncJobDAO_Update(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewSyncJobDAO(db.DB)

	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Original", "Original Desc", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	err = dao.Create(nil, job)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	cronExpr := "0 0 * * *"
	job.SetCronExpression(cronExpr)
	err = dao.Update(nil, job)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := dao.GetByID(nil, job.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got.CronExpression == nil || *got.CronExpression != cronExpr {
		t.Errorf("Update() CronExpression = %v, want %s", got.CronExpression, cronExpr)
	}
}

func TestSyncJobDAO_DeleteByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewSyncJobDAO(db.DB)

	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("To Delete", "Desc", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	err = dao.Create(nil, job)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = dao.DeleteByID(nil, job.ID)
	if err != nil {
		t.Fatalf("DeleteByID() error = %v", err)
	}

	got, err := dao.GetByID(nil, job.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got != nil {
		t.Error("DeleteByID() should remove the sync job")
	}
}

func TestSyncJobDAO_ListAll(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewSyncJobDAO(db.DB)

	apiMetaID1 := shared.NewID()
	dataStoreID1 := shared.NewID()
	workflowDefID1 := shared.NewID()
	job1 := sync.NewSyncJob("Job 1", "Desc 1", apiMetaID1, dataStoreID1, workflowDefID1, sync.SyncModeBatch)

	apiMetaID2 := shared.NewID()
	dataStoreID2 := shared.NewID()
	workflowDefID2 := shared.NewID()
	job2 := sync.NewSyncJob("Job 2", "Desc 2", apiMetaID2, dataStoreID2, workflowDefID2, sync.SyncModeBatch)

	err = dao.Create(nil, job1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = dao.Create(nil, job2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	list, err := dao.ListAll(nil)
	if err != nil {
		t.Fatalf("ListAll() error = %v", err)
	}

	if len(list) < 2 {
		t.Errorf("ListAll() returned %d sync jobs, want at least 2", len(list))
	}
}

func TestSyncExecutionDAO_Create(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	// Add sync_executions table
	_, err := db.Exec(`
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
			FOREIGN KEY (sync_job_id) REFERENCES sync_jobs(id)
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	// Create a sync job first
	jobDAO := dao.NewSyncJobDAO(db.DB)
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Test Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	err = jobDAO.Create(nil, job)
	if err != nil {
		t.Fatalf("Failed to create sync job: %v", err)
	}

	execDAO := dao.NewSyncExecutionDAO(db.DB)
	workflowInstID := shared.NewID()
	exec := sync.NewSyncExecution(job.ID, workflowInstID)

	err = execDAO.Create(nil, exec)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if exec.ID.IsEmpty() {
		t.Error("SyncExecution ID should be set")
	}
}

func TestSyncExecutionDAO_GetByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
			FOREIGN KEY (sync_job_id) REFERENCES sync_jobs(id)
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	jobDAO := dao.NewSyncJobDAO(db.DB)
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Test Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	err = jobDAO.Create(nil, job)
	if err != nil {
		t.Fatalf("Failed to create sync job: %v", err)
	}

	execDAO := dao.NewSyncExecutionDAO(db.DB)
	workflowInstID := shared.NewID()
	exec := sync.NewSyncExecution(job.ID, workflowInstID)
	err = execDAO.Create(nil, exec)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := execDAO.GetByID(nil, exec.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got == nil {
		t.Fatal("GetByID() returned nil")
	}

	if got.ID != exec.ID {
		t.Errorf("GetByID() ID = %s, want %s", got.ID, exec.ID)
	}
}

func TestSyncExecutionDAO_GetBySyncJob(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
			FOREIGN KEY (sync_job_id) REFERENCES sync_jobs(id)
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	jobDAO := dao.NewSyncJobDAO(db.DB)
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Test Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	err = jobDAO.Create(nil, job)
	if err != nil {
		t.Fatalf("Failed to create sync job: %v", err)
	}

	execDAO := dao.NewSyncExecutionDAO(db.DB)
	workflowInstID1 := shared.NewID()
	workflowInstID2 := shared.NewID()
	exec1 := sync.NewSyncExecution(job.ID, workflowInstID1)
	exec2 := sync.NewSyncExecution(job.ID, workflowInstID2)

	err = execDAO.Create(nil, exec1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = execDAO.Create(nil, exec2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	executions, err := execDAO.GetBySyncJob(nil, job.ID)
	if err != nil {
		t.Fatalf("GetBySyncJob() error = %v", err)
	}

	if len(executions) < 2 {
		t.Errorf("GetBySyncJob() returned %d executions, want at least 2", len(executions))
	}
}

func TestSyncExecutionDAO_Update(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
			FOREIGN KEY (sync_job_id) REFERENCES sync_jobs(id)
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	jobDAO := dao.NewSyncJobDAO(db.DB)
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Test Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	err = jobDAO.Create(nil, job)
	if err != nil {
		t.Fatalf("Failed to create sync job: %v", err)
	}

	execDAO := dao.NewSyncExecutionDAO(db.DB)
	workflowInstID := shared.NewID()
	exec := sync.NewSyncExecution(job.ID, workflowInstID)
	err = execDAO.Create(nil, exec)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	exec.MarkSuccess(100)
	err = execDAO.Update(nil, exec)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := execDAO.GetByID(nil, exec.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got.Status != sync.ExecStatusSuccess {
		t.Errorf("Update() Status = %s, want success", got.Status)
	}

	if got.RecordCount != 100 {
		t.Errorf("Update() RecordCount = %d, want 100", got.RecordCount)
	}
}

func TestSyncExecutionDAO_DeleteByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
			FOREIGN KEY (sync_job_id) REFERENCES sync_jobs(id)
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	jobDAO := dao.NewSyncJobDAO(db.DB)
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Test Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	err = jobDAO.Create(nil, job)
	if err != nil {
		t.Fatalf("Failed to create sync job: %v", err)
	}

	execDAO := dao.NewSyncExecutionDAO(db.DB)
	workflowInstID := shared.NewID()
	exec := sync.NewSyncExecution(job.ID, workflowInstID)
	err = execDAO.Create(nil, exec)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = execDAO.DeleteByID(nil, exec.ID)
	if err != nil {
		t.Fatalf("DeleteByID() error = %v", err)
	}

	got, err := execDAO.GetByID(nil, exec.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got != nil {
		t.Error("DeleteByID() should remove the execution")
	}
}

func TestSyncExecutionDAO_DeleteBySyncJob(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
			FOREIGN KEY (sync_job_id) REFERENCES sync_jobs(id)
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	jobDAO := dao.NewSyncJobDAO(db.DB)
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Test Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	err = jobDAO.Create(nil, job)
	if err != nil {
		t.Fatalf("Failed to create sync job: %v", err)
	}

	execDAO := dao.NewSyncExecutionDAO(db.DB)
	workflowInstID1 := shared.NewID()
	workflowInstID2 := shared.NewID()
	exec1 := sync.NewSyncExecution(job.ID, workflowInstID1)
	exec2 := sync.NewSyncExecution(job.ID, workflowInstID2)

	err = execDAO.Create(nil, exec1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = execDAO.Create(nil, exec2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Delete all executions for the sync job
	err = execDAO.DeleteBySyncJob(nil, job.ID)
	if err != nil {
		t.Fatalf("DeleteBySyncJob() error = %v", err)
	}

	// Verify all executions are deleted
	executions, err := execDAO.GetBySyncJob(nil, job.ID)
	if err != nil {
		t.Fatalf("GetBySyncJob() error = %v", err)
	}

	if len(executions) != 0 {
		t.Errorf("DeleteBySyncJob() should remove all executions, got %d remaining", len(executions))
	}
}
