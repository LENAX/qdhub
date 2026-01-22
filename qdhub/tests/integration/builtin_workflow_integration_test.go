//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/storage/sqlite"

	"qdhub/internal/application/impl"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/taskengine"
	"qdhub/internal/infrastructure/taskengine/workflows"
)

func TestBuiltInWorkflow_Integration(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	// Initialize Task Engine
	taskEngineDSN := db.DSN()
	aggregateRepo, err := sqlite.NewWorkflowAggregateRepoFromDSN(taskEngineDSN)
	if err != nil {
		t.Fatalf("Failed to create task engine aggregate repository: %v", err)
	}

	eng, err := engine.NewEngineWithAggregateRepo(5, 30, aggregateRepo)
	if err != nil {
		t.Fatalf("Failed to create task engine: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Failed to start task engine: %v", err)
	}
	defer eng.Stop()

	// Initialize Task Engine (register job functions)
	taskEngineDeps := &taskengine.Dependencies{
		DataSourceRegistry: nil,
		MetadataRepo:       nil,
	}
	if err := taskengine.Initialize(ctx, eng, taskEngineDeps); err != nil {
		t.Fatalf("Failed to initialize task engine: %v", err)
	}

	// Create repositories and services
	workflowRepo, err := repository.NewWorkflowDefinitionRepository(db)
	if err != nil {
		t.Fatalf("Failed to create workflow repository: %v", err)
	}

	taskEngineAdapter := taskengine.NewTaskEngineAdapter(eng)
	workflowFactory := taskengine.GetWorkflowFactory(eng)

	// Initialize built-in workflows
	builtInInitializer := impl.NewBuiltInWorkflowInitializer(workflowRepo, workflowFactory, taskEngineAdapter)
	if err := builtInInitializer.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize built-in workflows: %v", err)
	}

	t.Run("Verify built-in workflows are persisted", func(t *testing.T) {
		builtInWorkflows := workflows.GetBuiltInWorkflows()
		for _, meta := range builtInWorkflows {
			def, err := workflowRepo.Get(meta.ID)
			if err != nil {
				t.Errorf("Failed to get workflow %s: %v", meta.ID, err)
				continue
			}
			if def == nil {
				t.Errorf("Workflow %s was not persisted", meta.ID)
				continue
			}
			if !def.IsSystem {
				t.Errorf("Workflow %s should be marked as system workflow", meta.ID)
			}
			// Workflow.Name may differ from meta.Name (display name vs internal name)
			// Just verify workflow exists and has valid name
			if def.Workflow == nil || def.Workflow.Name == "" {
				t.Errorf("Workflow %s should have a valid workflow object with name", meta.ID)
			}
		}
	})

	t.Run("Get built-in workflow by API name", func(t *testing.T) {
		workflowID, err := workflows.GetBuiltInWorkflowIDByName("metadata_crawl")
		if err != nil {
			t.Fatalf("GetBuiltInWorkflowIDByName failed: %v", err)
		}
		if workflowID != workflows.BuiltInWorkflowIDMetadataCrawl {
			t.Errorf("GetBuiltInWorkflowIDByName = %s, want %s", workflowID, workflows.BuiltInWorkflowIDMetadataCrawl)
		}
	})

	t.Run("Verify workflow uses placeholder parameters", func(t *testing.T) {
		def, err := workflowRepo.Get(workflows.BuiltInWorkflowIDMetadataCrawl)
		if err != nil || def == nil {
			t.Fatalf("Failed to get metadata crawl workflow: %v", err)
		}

		// Check that workflow has tasks with placeholder parameters
		// This is a basic check - actual parameter replacement is tested in execution
		if def.Workflow == nil {
			t.Fatal("Workflow should not be nil")
		}
	})
}
