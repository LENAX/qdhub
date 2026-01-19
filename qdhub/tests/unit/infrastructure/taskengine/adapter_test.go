package taskengine_test

import (
	"context"
	"testing"

	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/taskengine"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/storage/sqlite"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

// createTestEngine creates a test engine with in-memory storage.
func createTestEngine(t *testing.T) *engine.Engine {
	t.Helper()

	// Create in-memory SQLite database
	db, err := sqlx.Connect("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create in-memory database: %v", err)
	}

	// Create aggregate repository
	aggregateRepo, err := sqlite.NewWorkflowAggregateRepo(db)
	if err != nil {
		t.Fatalf("Failed to create aggregate repo: %v", err)
	}

	// Create engine with aggregate repo
	eng, err := engine.NewEngineWithAggregateRepo(5, 30, aggregateRepo)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	return eng
}

func TestTaskEngineAdapter_Creation(t *testing.T) {
	eng := createTestEngine(t)
	defer eng.Stop()

	adapter := taskengine.NewTaskEngineAdapter(eng)
	if adapter == nil {
		t.Fatal("Expected adapter to be non-nil")
	}
}

func TestTaskEngineAdapter_SubmitWorkflow(t *testing.T) {
	ctx := context.Background()
	eng := createTestEngine(t)
	defer eng.Stop()

	// Start the engine
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Failed to start engine: %v", err)
	}

	adapter := taskengine.NewTaskEngineAdapter(eng)

	t.Run("Nil definition", func(t *testing.T) {
		_, err := adapter.SubmitWorkflow(ctx, nil, nil)
		if err == nil {
			t.Fatal("Expected error for nil definition")
		}
	})

	t.Run("Definition with nil workflow", func(t *testing.T) {
		def := &workflow.WorkflowDefinition{
			Workflow: nil,
		}
		_, err := adapter.SubmitWorkflow(ctx, def, nil)
		if err == nil {
			t.Fatal("Expected error for nil workflow")
		}
	})

	t.Run("Valid workflow", func(t *testing.T) {
		def := workflow.NewWorkflowDefinition("TestWorkflow", "Test", workflow.WfCategorySync, "yaml: test", false)

		// Must register workflow before submitting
		err := adapter.RegisterWorkflow(ctx, def)
		if err != nil {
			t.Fatalf("RegisterWorkflow failed: %v", err)
		}

		instanceID, err := adapter.SubmitWorkflow(ctx, def, map[string]interface{}{
			"param1": "value1",
			"param2": 123,
		})
		if err != nil {
			t.Fatalf("SubmitWorkflow failed: %v", err)
		}
		if instanceID == "" {
			t.Error("Expected non-empty instance ID")
		}
	})
}

func TestTaskEngineAdapter_RegisterWorkflow(t *testing.T) {
	ctx := context.Background()
	eng := createTestEngine(t)
	defer eng.Stop()

	// Start the engine
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Failed to start engine: %v", err)
	}

	adapter := taskengine.NewTaskEngineAdapter(eng)

	t.Run("Nil definition", func(t *testing.T) {
		err := adapter.RegisterWorkflow(ctx, nil)
		if err == nil {
			t.Fatal("Expected error for nil definition")
		}
	})

	t.Run("Valid workflow", func(t *testing.T) {
		def := workflow.NewWorkflowDefinition("TestWorkflow", "Test", workflow.WfCategorySync, "yaml: test", false)

		err := adapter.RegisterWorkflow(ctx, def)
		if err != nil {
			t.Fatalf("RegisterWorkflow failed: %v", err)
		}
	})
}

func TestTaskEngineAdapter_UnregisterWorkflow(t *testing.T) {
	ctx := context.Background()
	eng := createTestEngine(t)
	defer eng.Stop()

	adapter := taskengine.NewTaskEngineAdapter(eng)

	// UnregisterWorkflow is a no-op, should not error
	err := adapter.UnregisterWorkflow(ctx, "test-id")
	if err != nil {
		t.Fatalf("UnregisterWorkflow failed: %v", err)
	}
}

func TestTaskEngineAdapter_InstanceControl(t *testing.T) {
	ctx := context.Background()
	eng := createTestEngine(t)
	defer eng.Stop()

	// Start the engine
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Failed to start engine: %v", err)
	}

	adapter := taskengine.NewTaskEngineAdapter(eng)

	// Create and register workflow first
	def := workflow.NewWorkflowDefinition("TestWorkflow", "Test", workflow.WfCategorySync, "yaml: test", false)
	if err := adapter.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("RegisterWorkflow failed: %v", err)
	}

	// Then submit the workflow
	instanceID, err := adapter.SubmitWorkflow(ctx, def, nil)
	if err != nil {
		t.Fatalf("SubmitWorkflow failed: %v", err)
	}

	t.Run("PauseInstance", func(t *testing.T) {
		err := adapter.PauseInstance(ctx, instanceID)
		// May fail if instance is not in a pausable state, but should not panic
		_ = err
	})

	t.Run("ResumeInstance on non-paused", func(t *testing.T) {
		err := adapter.ResumeInstance(ctx, instanceID)
		// May fail if instance is not paused
		_ = err
	})

	t.Run("CancelInstance", func(t *testing.T) {
		err := adapter.CancelInstance(ctx, instanceID)
		// Should work or return error for already terminated instance
		_ = err
	})
}

func TestTaskEngineAdapter_GetInstanceStatus(t *testing.T) {
	ctx := context.Background()
	eng := createTestEngine(t)
	defer eng.Stop()

	// Start the engine
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Failed to start engine: %v", err)
	}

	adapter := taskengine.NewTaskEngineAdapter(eng)

	// Create and register workflow first
	def := workflow.NewWorkflowDefinition("TestWorkflow", "Test", workflow.WfCategorySync, "yaml: test", false)
	if err := adapter.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("RegisterWorkflow failed: %v", err)
	}

	// Then submit the workflow
	instanceID, err := adapter.SubmitWorkflow(ctx, def, nil)
	if err != nil {
		t.Fatalf("SubmitWorkflow failed: %v", err)
	}

	t.Run("Get status of submitted workflow", func(t *testing.T) {
		status, err := adapter.GetInstanceStatus(ctx, instanceID)
		if err != nil {
			t.Fatalf("GetInstanceStatus failed: %v", err)
		}
		if status == nil {
			t.Fatal("Expected status to be non-nil")
		}
		if status.InstanceID != instanceID {
			t.Errorf("Expected instance ID %s, got %s", instanceID, status.InstanceID)
		}
	})

	t.Run("Get status of non-existent instance", func(t *testing.T) {
		_, err := adapter.GetInstanceStatus(ctx, "non-existent-id")
		if err == nil {
			t.Fatal("Expected error for non-existent instance")
		}
	})
}
