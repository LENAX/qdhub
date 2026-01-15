package application_test

import (
	"context"
	"errors"
	"testing"

	"qdhub/internal/application"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
)

// ==================== Test Cases for WorkflowApplicationService ====================

func TestWorkflowApplicationService_CreateWorkflowDefinition(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		req := application.CreateWorkflowDefinitionRequest{
			Name:           "Test Workflow",
			Description:    "A test workflow",
			Category:       workflow.WfCategorySync,
			DefinitionYAML: "name: test\ntasks: []",
			IsSystem:       false,
		}

		def, err := svc.CreateWorkflowDefinition(ctx, req)
		if err != nil {
			t.Fatalf("CreateWorkflowDefinition failed: %v", err)
		}
		if def == nil {
			t.Fatal("Expected definition to be non-nil")
		}
		if def.Workflow.Name != req.Name {
			t.Errorf("Expected name %s, got %s", req.Name, def.Workflow.Name)
		}
	})

	t.Run("Invalid YAML", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		req := application.CreateWorkflowDefinitionRequest{
			Name:           "Test Workflow",
			Description:    "A test workflow",
			Category:       workflow.WfCategorySync,
			DefinitionYAML: "", // Empty YAML
			IsSystem:       false,
		}

		_, err := svc.CreateWorkflowDefinition(ctx, req)
		if err == nil {
			t.Fatal("Expected error for empty YAML")
		}
	})
}

func TestWorkflowApplicationService_GetWorkflowDefinition(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		def := workflow.NewWorkflowDefinition("Test", "Desc", workflow.WfCategorySync, "yaml: test", false)
		wfDefRepo.Create(def)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		result, err := svc.GetWorkflowDefinition(ctx, shared.ID(def.ID()))
		if err != nil {
			t.Fatalf("GetWorkflowDefinition failed: %v", err)
		}
		if result.ID() != def.ID() {
			t.Errorf("Expected ID %s, got %s", def.ID(), result.ID())
		}
	})

	t.Run("Not found", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		_, err := svc.GetWorkflowDefinition(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent definition")
		}
	})
}

func TestWorkflowApplicationService_UpdateWorkflowDefinition(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		def := workflow.NewWorkflowDefinition("Test", "Desc", workflow.WfCategorySync, "yaml: test", false)
		wfDefRepo.Create(def)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		newName := "Updated Name"
		err := svc.UpdateWorkflowDefinition(ctx, shared.ID(def.ID()), application.UpdateWorkflowDefinitionRequest{
			Name: &newName,
		})
		if err != nil {
			t.Fatalf("UpdateWorkflowDefinition failed: %v", err)
		}

		updated, _ := wfDefRepo.Get(def.ID())
		if updated.Workflow.Name != newName {
			t.Errorf("Expected name %s, got %s", newName, updated.Workflow.Name)
		}
	})

	t.Run("Cannot update with running instances", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		def := workflow.NewWorkflowDefinition("Test", "Desc", workflow.WfCategorySync, "yaml: test", false)
		wfDefRepo.Create(def)

		// Create a running instance
		inst := workflow.NewWorkflowInstance(def.ID())
		inst.Status = "Running"
		wfInstRepo.Create(inst)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		newName := "Updated Name"
		err := svc.UpdateWorkflowDefinition(ctx, shared.ID(def.ID()), application.UpdateWorkflowDefinitionRequest{
			Name: &newName,
		})
		if err == nil {
			t.Fatal("Expected error when updating with running instances")
		}
	})
}

func TestWorkflowApplicationService_DeleteWorkflowDefinition(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		def := workflow.NewWorkflowDefinition("Test", "Desc", workflow.WfCategorySync, "yaml: test", false)
		wfDefRepo.Create(def)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		err := svc.DeleteWorkflowDefinition(ctx, shared.ID(def.ID()))
		if err != nil {
			t.Fatalf("DeleteWorkflowDefinition failed: %v", err)
		}

		deleted, _ := wfDefRepo.Get(def.ID())
		if deleted != nil {
			t.Error("Definition should be deleted")
		}
	})

	t.Run("Cannot delete with running instances", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		def := workflow.NewWorkflowDefinition("Test", "Desc", workflow.WfCategorySync, "yaml: test", false)
		wfDefRepo.Create(def)

		// Create a running instance
		inst := workflow.NewWorkflowInstance(def.ID())
		inst.Status = "Running"
		wfInstRepo.Create(inst)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		err := svc.DeleteWorkflowDefinition(ctx, shared.ID(def.ID()))
		if err == nil {
			t.Fatal("Expected error when deleting with running instances")
		}
	})
}

func TestWorkflowApplicationService_ListWorkflowDefinitions(t *testing.T) {
	ctx := context.Background()

	t.Run("List all", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		// Create multiple definitions
		for i := 0; i < 3; i++ {
			def := workflow.NewWorkflowDefinition("Test", "Desc", workflow.WfCategorySync, "yaml: test", false)
			wfDefRepo.Create(def)
		}

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		defs, err := svc.ListWorkflowDefinitions(ctx, nil)
		if err != nil {
			t.Fatalf("ListWorkflowDefinitions failed: %v", err)
		}
		if len(defs) != 3 {
			t.Errorf("Expected 3 definitions, got %d", len(defs))
		}
	})

	t.Run("Filter by category", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		// Create definitions with different categories
		syncDef := workflow.NewWorkflowDefinition("Sync", "Desc", workflow.WfCategorySync, "yaml: test", false)
		wfDefRepo.Create(syncDef)

		metaDef := workflow.NewWorkflowDefinition("Meta", "Desc", workflow.WfCategoryMetadata, "yaml: test", false)
		wfDefRepo.Create(metaDef)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		syncCategory := workflow.WfCategorySync
		defs, err := svc.ListWorkflowDefinitions(ctx, &syncCategory)
		if err != nil {
			t.Fatalf("ListWorkflowDefinitions failed: %v", err)
		}
		if len(defs) != 1 {
			t.Errorf("Expected 1 sync definition, got %d", len(defs))
		}
	})
}

func TestWorkflowApplicationService_EnableDisableWorkflow(t *testing.T) {
	ctx := context.Background()

	t.Run("Enable workflow", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		def := workflow.NewWorkflowDefinition("Test", "Desc", workflow.WfCategorySync, "yaml: test", false)
		wfDefRepo.Create(def)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		err := svc.EnableWorkflow(ctx, shared.ID(def.ID()))
		if err != nil {
			t.Fatalf("EnableWorkflow failed: %v", err)
		}

		updated, _ := wfDefRepo.Get(def.ID())
		if !updated.IsEnabled() {
			t.Error("Workflow should be enabled")
		}
	})

	t.Run("Disable workflow", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		def := workflow.NewWorkflowDefinition("Test", "Desc", workflow.WfCategorySync, "yaml: test", false)
		def.Enable()
		wfDefRepo.Create(def)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		err := svc.DisableWorkflow(ctx, shared.ID(def.ID()))
		if err != nil {
			t.Fatalf("DisableWorkflow failed: %v", err)
		}

		updated, _ := wfDefRepo.Get(def.ID())
		if updated.IsEnabled() {
			t.Error("Workflow should be disabled")
		}
	})

	t.Run("Cannot disable with running instances", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		def := workflow.NewWorkflowDefinition("Test", "Desc", workflow.WfCategorySync, "yaml: test", false)
		def.Enable()
		wfDefRepo.Create(def)

		// Create a running instance
		inst := workflow.NewWorkflowInstance(def.ID())
		inst.Status = "Running"
		wfInstRepo.Create(inst)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		err := svc.DisableWorkflow(ctx, shared.ID(def.ID()))
		if err == nil {
			t.Fatal("Expected error when disabling with running instances")
		}
	})
}

func TestWorkflowApplicationService_ExecuteWorkflow(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		def := workflow.NewWorkflowDefinition("Test", "Desc", workflow.WfCategorySync, "yaml: test", false)
		def.Enable()
		wfDefRepo.Create(def)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		instID, err := svc.ExecuteWorkflow(ctx, application.ExecuteWorkflowRequest{
			WorkflowDefID: shared.ID(def.ID()),
			TriggerType:   workflow.TriggerTypeManual,
			TriggerParams: map[string]interface{}{"key": "value"},
		})
		if err != nil {
			t.Fatalf("ExecuteWorkflow failed: %v", err)
		}
		if instID.IsEmpty() {
			t.Error("Expected non-empty instance ID")
		}
	})

	t.Run("Cannot execute disabled workflow", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		def := workflow.NewWorkflowDefinition("Test", "Desc", workflow.WfCategorySync, "yaml: test", false)
		// Explicitly disable the workflow
		def.Disable()
		wfDefRepo.Create(def)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		_, err := svc.ExecuteWorkflow(ctx, application.ExecuteWorkflowRequest{
			WorkflowDefID: shared.ID(def.ID()),
			TriggerType:   workflow.TriggerTypeManual,
		})
		if err == nil {
			t.Fatal("Expected error when executing disabled workflow")
		}
	})

	t.Run("Task engine submit error", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()
		adapter.submitErr = errors.New("task engine error")

		def := workflow.NewWorkflowDefinition("Test", "Desc", workflow.WfCategorySync, "yaml: test", false)
		def.Enable()
		wfDefRepo.Create(def)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		_, err := svc.ExecuteWorkflow(ctx, application.ExecuteWorkflowRequest{
			WorkflowDefID: shared.ID(def.ID()),
			TriggerType:   workflow.TriggerTypeManual,
		})
		if err == nil {
			t.Fatal("Expected error when task engine fails")
		}
	})
}

func TestWorkflowApplicationService_GetWorkflowInstance(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		inst := workflow.NewWorkflowInstance("wf-id")
		wfInstRepo.Create(inst)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		result, err := svc.GetWorkflowInstance(ctx, shared.ID(inst.ID))
		if err != nil {
			t.Fatalf("GetWorkflowInstance failed: %v", err)
		}
		if result.ID != inst.ID {
			t.Errorf("Expected ID %s, got %s", inst.ID, result.ID)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		_, err := svc.GetWorkflowInstance(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent instance")
		}
	})
}

func TestWorkflowApplicationService_ListWorkflowInstances(t *testing.T) {
	ctx := context.Background()

	t.Run("List all for workflow", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		wfID := "test-workflow-id"

		// Create multiple instances
		for i := 0; i < 3; i++ {
			inst := workflow.NewWorkflowInstance(wfID)
			wfInstRepo.Create(inst)
		}

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		instances, err := svc.ListWorkflowInstances(ctx, shared.ID(wfID), nil)
		if err != nil {
			t.Fatalf("ListWorkflowInstances failed: %v", err)
		}
		if len(instances) != 3 {
			t.Errorf("Expected 3 instances, got %d", len(instances))
		}
	})

	t.Run("Filter by status", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		wfID := "test-workflow-id"

		// Create instances with different statuses
		runningInst := workflow.NewWorkflowInstance(wfID)
		runningInst.Status = "Running"
		wfInstRepo.Create(runningInst)

		successInst := workflow.NewWorkflowInstance(wfID)
		successInst.Status = "Success"
		wfInstRepo.Create(successInst)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		runningStatus := workflow.WfInstStatusRunning
		instances, err := svc.ListWorkflowInstances(ctx, shared.ID(wfID), &runningStatus)
		if err != nil {
			t.Fatalf("ListWorkflowInstances failed: %v", err)
		}
		if len(instances) != 1 {
			t.Errorf("Expected 1 running instance, got %d", len(instances))
		}
	})
}

func TestWorkflowApplicationService_InstanceControl(t *testing.T) {
	ctx := context.Background()

	t.Run("Pause workflow", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		inst := workflow.NewWorkflowInstance("wf-id")
		inst.Status = "Running"
		wfInstRepo.Create(inst)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		err := svc.PauseWorkflow(ctx, shared.ID(inst.ID))
		if err != nil {
			t.Fatalf("PauseWorkflow failed: %v", err)
		}
	})

	t.Run("Cannot pause non-running instance", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		inst := workflow.NewWorkflowInstance("wf-id")
		inst.Status = "Success"
		wfInstRepo.Create(inst)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		err := svc.PauseWorkflow(ctx, shared.ID(inst.ID))
		if err == nil {
			t.Fatal("Expected error when pausing non-running instance")
		}
	})

	t.Run("Resume workflow", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		inst := workflow.NewWorkflowInstance("wf-id")
		inst.Status = "Paused"
		wfInstRepo.Create(inst)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		err := svc.ResumeWorkflow(ctx, shared.ID(inst.ID))
		if err != nil {
			t.Fatalf("ResumeWorkflow failed: %v", err)
		}
	})

	t.Run("Cannot resume non-paused instance", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		inst := workflow.NewWorkflowInstance("wf-id")
		inst.Status = "Running"
		wfInstRepo.Create(inst)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		err := svc.ResumeWorkflow(ctx, shared.ID(inst.ID))
		if err == nil {
			t.Fatal("Expected error when resuming non-paused instance")
		}
	})

	t.Run("Cancel workflow", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		inst := workflow.NewWorkflowInstance("wf-id")
		inst.Status = "Running"
		wfInstRepo.Create(inst)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		err := svc.CancelWorkflow(ctx, shared.ID(inst.ID))
		if err != nil {
			t.Fatalf("CancelWorkflow failed: %v", err)
		}
	})

	t.Run("Cannot cancel terminal instance", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()

		inst := workflow.NewWorkflowInstance("wf-id")
		inst.Status = "Success"
		wfInstRepo.Create(inst)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		err := svc.CancelWorkflow(ctx, shared.ID(inst.ID))
		if err == nil {
			t.Fatal("Expected error when cancelling terminal instance")
		}
	})
}

func TestWorkflowApplicationService_GetWorkflowStatus(t *testing.T) {
	ctx := context.Background()

	wfDefRepo := NewMockWorkflowDefinitionRepository()
	wfInstRepo := NewMockWorkflowInstanceRepository()
	adapter := NewMockTaskEngineAdapter()
	adapter.instanceStatus = &workflow.WorkflowStatus{
		InstanceID:    "test-id",
		Status:        "Running",
		Progress:      75.0,
		TaskCount:     4,
		CompletedTask: 3,
		FailedTask:    0,
	}

	svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

	status, err := svc.GetWorkflowStatus(ctx, shared.ID("test-id"))
	if err != nil {
		t.Fatalf("GetWorkflowStatus failed: %v", err)
	}
	if status.Progress != 75.0 {
		t.Errorf("Expected progress 75.0, got %f", status.Progress)
	}
}

func TestWorkflowApplicationService_SyncWithEngine(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		wfInstRepo := NewMockWorkflowInstanceRepository()
		adapter := NewMockTaskEngineAdapter()
		adapter.instanceStatus = &workflow.WorkflowStatus{
			InstanceID: "test-id",
			Status:     "Success",
		}

		inst := workflow.NewWorkflowInstance("wf-id")
		inst.ID = "test-id"
		inst.Status = "Running"
		wfInstRepo.Create(inst)

		svc := application.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

		err := svc.SyncWithEngine(ctx, shared.ID(inst.ID))
		if err != nil {
			t.Fatalf("SyncWithEngine failed: %v", err)
		}

		updated, _ := wfInstRepo.Get(inst.ID)
		if updated.Status != "Success" {
			t.Errorf("Expected status Success, got %s", updated.Status)
		}
	})
}
