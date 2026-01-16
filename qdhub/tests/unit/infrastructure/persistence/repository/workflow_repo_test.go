package repository_test

import (
	"os"
	"testing"

	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
)

// setupTestDB creates a temporary database for testing
func setupTestDB(t *testing.T) (*persistence.DB, func()) {
	// Create a temporary file for the database
	tmpfile, err := os.CreateTemp("", "test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()

	dsn := tmpfile.Name()
	db, err := persistence.NewDB(dsn)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Run migrations (simplified - just create Task Engine tables)
	// In a real scenario, we would run the actual migrations
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS workflow_definition (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			description TEXT,
			params TEXT,
			dependencies TEXT,
			create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			status TEXT NOT NULL DEFAULT 'ENABLED',
			sub_task_error_tolerance REAL NOT NULL DEFAULT 0.0,
			transactional INTEGER NOT NULL DEFAULT 0,
			transaction_mode TEXT DEFAULT '',
			max_concurrent_task INTEGER NOT NULL DEFAULT 10,
			cron_expr TEXT DEFAULT '',
			cron_enabled INTEGER NOT NULL DEFAULT 0,
			category TEXT,
			definition_yaml TEXT,
			version INTEGER DEFAULT 1,
			is_system INTEGER DEFAULT 0,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		
		CREATE TABLE IF NOT EXISTS workflow_instance (
			id TEXT PRIMARY KEY,
			workflow_id TEXT NOT NULL,
			status TEXT NOT NULL,
			start_time DATETIME,
			end_time DATETIME,
			breakpoint TEXT,
			error_message TEXT,
			create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			engine_instance_id TEXT,
			trigger_type TEXT,
			trigger_params TEXT,
			progress REAL DEFAULT 0,
			FOREIGN KEY (workflow_id) REFERENCES workflow_definition(id)
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

func TestWorkflowDefinitionRepository_Create(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	repo, err := repository.NewWorkflowDefinitionRepository(db)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}

	wf := workflow.NewWorkflowDefinition("Test Workflow", "Test Description", workflow.WfCategoryCustom, "name: test", false)

	err = repo.Create(wf)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if wf.ID() == "" {
		t.Error("Workflow ID should be set after creation")
	}
}

func TestWorkflowDefinitionRepository_Get(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	repo, err := repository.NewWorkflowDefinitionRepository(db)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}

	// Create a workflow
	wf := workflow.NewWorkflowDefinition("Test Workflow", "Test Description", workflow.WfCategoryCustom, "name: test", false)
	err = repo.Create(wf)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Get the workflow
	got, err := repo.Get(wf.ID())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got == nil {
		t.Fatal("Get() returned nil")
	}

	if got.ID() != wf.ID() {
		t.Errorf("Get() ID = %s, want %s", got.ID(), wf.ID())
	}

	if got.Workflow.Name != wf.Workflow.Name {
		t.Errorf("Get() Name = %s, want %s", got.Workflow.Name, wf.Workflow.Name)
	}
}

func TestWorkflowDefinitionRepository_Get_NotFound(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	repo, err := repository.NewWorkflowDefinitionRepository(db)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}

	got, err := repo.Get("non-existent-id")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got != nil {
		t.Error("Get() should return nil for non-existent ID")
	}
}

func TestWorkflowDefinitionRepository_Update(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	repo, err := repository.NewWorkflowDefinitionRepository(db)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}

	// Create a workflow
	wf := workflow.NewWorkflowDefinition("Test Workflow", "Test Description", workflow.WfCategoryCustom, "name: test", false)
	err = repo.Create(wf)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Update the workflow
	wf.Workflow.Name = "Updated Workflow"
	wf.UpdateDefinition("name: updated")
	err = repo.Update(wf)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	// Verify the update
	got, err := repo.Get(wf.ID())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got.Workflow.Name != "Updated Workflow" {
		t.Errorf("Update() Name = %s, want Updated Workflow", got.Workflow.Name)
	}
}

func TestWorkflowDefinitionRepository_Delete(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	repo, err := repository.NewWorkflowDefinitionRepository(db)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}

	// Create a workflow
	wf := workflow.NewWorkflowDefinition("Test Workflow", "Test Description", workflow.WfCategoryCustom, "name: test", false)
	err = repo.Create(wf)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Delete the workflow
	err = repo.Delete(wf.ID())
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Verify deletion
	got, err := repo.Get(wf.ID())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got != nil {
		t.Error("Delete() should remove the workflow")
	}
}

func TestWorkflowDefinitionRepository_List(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	repo, err := repository.NewWorkflowDefinitionRepository(db)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}

	// Create multiple workflows
	wf1 := workflow.NewWorkflowDefinition("Workflow 1", "Description 1", workflow.WfCategoryCustom, "name: wf1", false)
	wf2 := workflow.NewWorkflowDefinition("Workflow 2", "Description 2", workflow.WfCategorySync, "name: wf2", false)

	err = repo.Create(wf1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = repo.Create(wf2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// List all workflows
	list, err := repo.List()
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if len(list) < 2 {
		t.Errorf("List() returned %d workflows, want at least 2", len(list))
	}
}

// Note: WorkflowInstanceRepository has been integrated into WorkflowDefinitionRepository
// following DDD aggregate patterns. Use WorkflowDefinitionRepository methods for WorkflowInstance operations.
