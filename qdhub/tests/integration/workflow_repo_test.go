// +build integration

package integration

import (
	"os"
	"testing"

	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
)

// setupIntegrationDB is defined in test_helper.go
func _setupIntegrationDB(t *testing.T) (*persistence.DB, func()) {
	// Create a temporary file for the database
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

func TestWorkflowDefinitionRepository_Integration(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	repo, err := repository.NewWorkflowDefinitionRepository(db)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}

	t.Run("Create and Get", func(t *testing.T) {
		wf := workflow.NewWorkflowDefinition("Integration Test", "Test Description", workflow.WfCategorySync, "name: test\ntasks: []", false)

		err := repo.Create(wf)
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}

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

		if got.Workflow.Name != "Integration Test" {
			t.Errorf("Get() Name = %s, want Integration Test", got.Workflow.Name)
		}
	})

	t.Run("Update", func(t *testing.T) {
		wf := workflow.NewWorkflowDefinition("Original Name", "Original Desc", workflow.WfCategoryCustom, "name: original", false)

		err := repo.Create(wf)
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		wf.Workflow.Name = "Updated Name"
		wf.UpdateDefinition("name: updated")

		err = repo.Update(wf)
		if err != nil {
			t.Fatalf("Update() error = %v", err)
		}

		got, err := repo.Get(wf.ID())
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}

		if got.Workflow.Name != "Updated Name" {
			t.Errorf("Update() Name = %s, want Updated Name", got.Workflow.Name)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		wf := workflow.NewWorkflowDefinition("To Delete", "Desc", workflow.WfCategoryCustom, "name: delete", false)

		err := repo.Create(wf)
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		err = repo.Delete(wf.ID())
		if err != nil {
			t.Fatalf("Delete() error = %v", err)
		}

		got, err := repo.Get(wf.ID())
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}

		if got != nil {
			t.Error("Delete() should remove the workflow")
		}
	})

	t.Run("List", func(t *testing.T) {
		// Create multiple workflows
		wf1 := workflow.NewWorkflowDefinition("List Test 1", "Desc 1", workflow.WfCategoryMetadata, "name: list1", false)
		wf2 := workflow.NewWorkflowDefinition("List Test 2", "Desc 2", workflow.WfCategorySync, "name: list2", false)

		err := repo.Create(wf1)
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		err = repo.Create(wf2)
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		list, err := repo.List()
		if err != nil {
			t.Fatalf("List() error = %v", err)
		}

		if len(list) < 2 {
			t.Errorf("List() returned %d workflows, want at least 2", len(list))
		}

		// Verify both workflows are in the list
		found1, found2 := false, false
		for _, wf := range list {
			if wf.ID() == wf1.ID() {
				found1 = true
			}
			if wf.ID() == wf2.ID() {
				found2 = true
			}
		}

		if !found1 {
			t.Error("List() should contain wf1")
		}
		if !found2 {
			t.Error("List() should contain wf2")
		}
	})
}

func TestWorkflowInstanceRepository_Integration(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	defRepo, err := repository.NewWorkflowDefinitionRepository(db)
	if err != nil {
		t.Fatalf("Failed to create definition repository: %v", err)
	}

	instRepo, err := repository.NewWorkflowInstanceRepository(db)
	if err != nil {
		t.Fatalf("Failed to create instance repository: %v", err)
	}

	// Create a workflow definition first
	wf := workflow.NewWorkflowDefinition("Instance Test", "Desc", workflow.WfCategoryCustom, "name: instance_test", false)
	err = defRepo.Create(wf)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	t.Run("Create and Get", func(t *testing.T) {
		inst := workflow.NewWorkflowInstance(wf.ID())

		err := instRepo.Create(inst)
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		if inst.ID == "" {
			t.Error("Instance ID should be set after creation")
		}

		got, err := instRepo.Get(inst.ID)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}

		if got == nil {
			t.Fatal("Get() returned nil")
		}

		if got.ID != inst.ID {
			t.Errorf("Get() ID = %s, want %s", got.ID, inst.ID)
		}

		if got.WorkflowID != wf.ID() {
			t.Errorf("Get() WorkflowID = %s, want %s", got.WorkflowID, wf.ID())
		}
	})

	t.Run("GetByWorkflowDef", func(t *testing.T) {
		// Create multiple instances
		inst1 := workflow.NewWorkflowInstance(wf.ID())
		inst2 := workflow.NewWorkflowInstance(wf.ID())

		err := instRepo.Create(inst1)
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		err = instRepo.Create(inst2)
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		instances, err := instRepo.GetByWorkflowDef(wf.ID())
		if err != nil {
			t.Fatalf("GetByWorkflowDef() error = %v", err)
		}

		if len(instances) < 2 {
			t.Errorf("GetByWorkflowDef() returned %d instances, want at least 2", len(instances))
		}

		// Verify both instances are in the list
		found1, found2 := false, false
		for _, inst := range instances {
			if inst.ID == inst1.ID {
				found1 = true
			}
			if inst.ID == inst2.ID {
				found2 = true
			}
		}

		if !found1 {
			t.Error("GetByWorkflowDef() should contain inst1")
		}
		if !found2 {
			t.Error("GetByWorkflowDef() should contain inst2")
		}
	})

	t.Run("Update", func(t *testing.T) {
		inst := workflow.NewWorkflowInstance(wf.ID())

		err := instRepo.Create(inst)
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		// Update the instance status
		inst.Status = "Running"
		err = instRepo.Update(inst)
		if err != nil {
			t.Fatalf("Update() error = %v", err)
		}

		// Verify the update
		got, err := instRepo.Get(inst.ID)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}

		if got.Status != "Running" {
			t.Errorf("Update() Status = %s, want Running", got.Status)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		inst := workflow.NewWorkflowInstance(wf.ID())

		err := instRepo.Create(inst)
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		err = instRepo.Delete(inst.ID)
		if err != nil {
			t.Fatalf("Delete() error = %v", err)
		}

		got, err := instRepo.Get(inst.ID)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}

		if got != nil {
			t.Error("Delete() should remove the instance")
		}
	})
}
