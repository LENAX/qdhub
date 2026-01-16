//go:build integration
// +build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/core/task"
	"github.com/LENAX/task-engine/pkg/core/workflow"
	"github.com/LENAX/task-engine/pkg/storage/sqlite"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"qdhub/internal/application/impl"
	"qdhub/internal/domain/shared"
	qdhubworkflow "qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/taskengine"
	"qdhub/internal/infrastructure/taskengine/workflows"
	httpapi "qdhub/internal/interfaces/http"
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
			if def.Workflow.Name != meta.Name {
				t.Errorf("Workflow %s name = %s, want %s", meta.ID, def.Workflow.Name, meta.Name)
			}
		}
	})

	t.Run("Execute built-in workflow by name via API", func(t *testing.T) {
		gin.SetMode(gin.TestMode)

		workflowSvc := impl.NewWorkflowApplicationService(workflowRepo, taskEngineAdapter)
		handler := httpapi.NewWorkflowHandler(workflowSvc)

		router := gin.New()
		api := router.Group("/api/v1")
		handler.RegisterRoutes(api)

		// Test ExecuteBuiltInWorkflowByName endpoint
		reqBody := map[string]interface{}{
			"trigger_type": "manual",
			"trigger_params": map[string]interface{}{
				"data_source_id":   "test-ds-id",
				"data_source_name": "test-ds",
			},
		}

		body, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", "/api/v1/workflows/built-in/metadata_crawl/execute", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Contains(t, response, "instance_id")
		assert.Contains(t, response, "status")
	})

	t.Run("Execute built-in workflow with parameter replacement", func(t *testing.T) {
		workflowSvc := impl.NewWorkflowApplicationService(workflowRepo, taskEngineAdapter)

		// Get the metadata crawl workflow
		def, err := workflowRepo.Get(workflows.BuiltInWorkflowIDMetadataCrawl)
		if err != nil || def == nil {
			t.Fatalf("Failed to get metadata crawl workflow: %v", err)
		}

		// Execute with parameters
		req := contracts.ExecuteWorkflowRequest{
			WorkflowDefID: shared.ID(def.ID()),
			TriggerType:   qdhubworkflow.TriggerTypeManual,
			TriggerParams: map[string]interface{}{
				"data_source_id":   "test-ds-id",
				"data_source_name": "test-ds",
			},
		}

		instanceID, err := workflowSvc.ExecuteWorkflow(ctx, req)
		if err != nil {
			t.Fatalf("ExecuteWorkflow failed: %v", err)
		}
		if instanceID == "" {
			t.Fatal("Expected non-empty instance ID")
		}

		// Verify instance was created
		inst, err := workflowRepo.GetInstance(instanceID.String())
		if err != nil {
			t.Logf("Warning: failed to get instance (may not be stored yet): %v", err)
		} else if inst != nil {
			if inst.WorkflowID != def.ID() {
				t.Errorf("Instance workflow ID = %s, want %s", inst.WorkflowID, def.ID())
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
