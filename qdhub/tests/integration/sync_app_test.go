//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"testing"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/taskengine"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/storage/sqlite"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

// ==================== Integration Test Helpers ====================

type testContext struct {
	db             *persistence.DB
	engine         *engine.Engine
	syncJobRepo    *repository.SyncJobRepositoryImpl
	syncExecRepo   *repository.SyncExecutionRepositoryImpl
	wfDefRepo      *repository.WorkflowDefinitionRepositoryImpl
	wfInstRepo     *repository.WorkflowInstanceRepositoryImpl
	adapter        workflow.TaskEngineAdapter
	syncAppService contracts.SyncApplicationService
	wfAppService   contracts.WorkflowApplicationService
	cleanup        func()
}

func setupTestContext(t *testing.T) *testContext {
	t.Helper()

	// Create temp database file
	tmpfile, err := os.CreateTemp("", "sync_app_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()
	dbPath := tmpfile.Name()

	// Create database connection
	db, err := persistence.NewDB(dbPath)
	if err != nil {
		os.Remove(dbPath)
		t.Fatalf("Failed to create database: %v", err)
	}

	// Run migrations
	migrationSQL, err := os.ReadFile("../../migrations/001_init_schema.up.sql")
	if err != nil {
		db.Close()
		os.Remove(dbPath)
		t.Fatalf("Failed to read migration file: %v", err)
	}
	if _, err := db.Exec(string(migrationSQL)); err != nil {
		db.Close()
		os.Remove(dbPath)
		t.Fatalf("Failed to execute migration: %v", err)
	}

	// Create Task Engine
	sqlxDB, err := sqlx.Connect("sqlite3", dbPath)
	if err != nil {
		db.Close()
		os.Remove(dbPath)
		t.Fatalf("Failed to create sqlx connection: %v", err)
	}

	aggregateRepo, err := sqlite.NewWorkflowAggregateRepo(sqlxDB)
	if err != nil {
		db.Close()
		sqlxDB.Close()
		os.Remove(dbPath)
		t.Fatalf("Failed to create aggregate repo: %v", err)
	}

	eng, err := engine.NewEngineWithAggregateRepo(5, 30, aggregateRepo)
	if err != nil {
		db.Close()
		sqlxDB.Close()
		os.Remove(dbPath)
		t.Fatalf("Failed to create engine: %v", err)
	}

	// Start engine
	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		db.Close()
		sqlxDB.Close()
		os.Remove(dbPath)
		t.Fatalf("Failed to start engine: %v", err)
	}

	// Create repositories
	syncJobRepo := repository.NewSyncJobRepository(db)
	syncExecRepo := repository.NewSyncExecutionRepository(db)
	wfDefRepo, err := repository.NewWorkflowDefinitionRepository(db)
	if err != nil {
		eng.Stop()
		db.Close()
		sqlxDB.Close()
		os.Remove(dbPath)
		t.Fatalf("Failed to create workflow definition repository: %v", err)
	}
	wfInstRepo, err := repository.NewWorkflowInstanceRepository(db)
	if err != nil {
		eng.Stop()
		db.Close()
		sqlxDB.Close()
		os.Remove(dbPath)
		t.Fatalf("Failed to create workflow instance repository: %v", err)
	}

	// Create adapter
	adapter := taskengine.NewTaskEngineAdapter(eng)

	// Create services
	syncAppService := impl.NewSyncApplicationService(syncJobRepo, syncExecRepo, wfDefRepo, wfInstRepo, adapter)
	wfAppService := impl.NewWorkflowApplicationService(wfDefRepo, wfInstRepo, adapter)

	return &testContext{
		db:             db,
		engine:         eng,
		syncJobRepo:    syncJobRepo,
		syncExecRepo:   syncExecRepo,
		wfDefRepo:      wfDefRepo,
		wfInstRepo:     wfInstRepo,
		adapter:        adapter,
		syncAppService: syncAppService,
		wfAppService:   wfAppService,
		cleanup: func() {
			eng.Stop()
			db.Close()
			sqlxDB.Close()
			os.Remove(dbPath)
		},
	}
}

// ==================== Integration Tests ====================

// Note: SyncJob integration tests are skipped because they require complex
// foreign key dependencies (data_source, api_metadata, quant_data_store).
// The core SyncApplicationService logic is tested in unit tests with mocks.
// Workflow integration tests below test the actual Task Engine integration.

func TestWorkflowApplicationService_Integration_CreateAndGetWorkflowDefinition(t *testing.T) {
	tc := setupTestContext(t)
	defer tc.cleanup()

	ctx := context.Background()

	// Create workflow definition
	req := contracts.CreateWorkflowDefinitionRequest{
		Name:           "Integration Test Workflow",
		Description:    "A workflow for integration testing",
		Category:       workflow.WfCategorySync,
		DefinitionYAML: "name: test\ntasks: []",
		IsSystem:       false,
	}

	def, err := tc.wfAppService.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("CreateWorkflowDefinition failed: %v", err)
	}
	if def == nil {
		t.Fatal("Expected definition to be non-nil")
	}

	// Get workflow definition
	retrieved, err := tc.wfAppService.GetWorkflowDefinition(ctx, shared.ID(def.ID()))
	if err != nil {
		t.Fatalf("GetWorkflowDefinition failed: %v", err)
	}
	if retrieved.Workflow.Name != req.Name {
		t.Errorf("Expected name %s, got %s", req.Name, retrieved.Workflow.Name)
	}
}

func TestWorkflowApplicationService_Integration_EnableDisableWorkflow(t *testing.T) {
	tc := setupTestContext(t)
	defer tc.cleanup()

	ctx := context.Background()

	// Create workflow definition
	def, _ := tc.wfAppService.CreateWorkflowDefinition(ctx, contracts.CreateWorkflowDefinitionRequest{
		Name:           "Test Workflow",
		Description:    "Test",
		Category:       workflow.WfCategorySync,
		DefinitionYAML: "name: test\ntasks: []",
		IsSystem:       false,
	})

	// Enable workflow
	if err := tc.wfAppService.EnableWorkflow(ctx, shared.ID(def.ID())); err != nil {
		t.Fatalf("EnableWorkflow failed: %v", err)
	}

	enabled, _ := tc.wfAppService.GetWorkflowDefinition(ctx, shared.ID(def.ID()))
	if !enabled.IsEnabled() {
		t.Error("Expected workflow to be enabled")
	}

	// Disable workflow
	if err := tc.wfAppService.DisableWorkflow(ctx, shared.ID(def.ID())); err != nil {
		t.Fatalf("DisableWorkflow failed: %v", err)
	}

	disabled, _ := tc.wfAppService.GetWorkflowDefinition(ctx, shared.ID(def.ID()))
	if disabled.IsEnabled() {
		t.Error("Expected workflow to be disabled")
	}
}

func TestWorkflowApplicationService_Integration_ListWorkflowDefinitions(t *testing.T) {
	tc := setupTestContext(t)
	defer tc.cleanup()

	ctx := context.Background()

	// Create multiple workflow definitions
	for i := 0; i < 3; i++ {
		tc.wfAppService.CreateWorkflowDefinition(ctx, contracts.CreateWorkflowDefinitionRequest{
			Name:           "Test Workflow",
			Description:    "Test",
			Category:       workflow.WfCategorySync,
			DefinitionYAML: "name: test\ntasks: []",
			IsSystem:       false,
		})
	}

	// List definitions
	defs, err := tc.wfAppService.ListWorkflowDefinitions(ctx, nil)
	if err != nil {
		t.Fatalf("ListWorkflowDefinitions failed: %v", err)
	}
	if len(defs) != 3 {
		t.Errorf("Expected 3 definitions, got %d", len(defs))
	}
}

func TestWorkflowApplicationService_Integration_ExecuteWorkflow(t *testing.T) {
	tc := setupTestContext(t)
	defer tc.cleanup()

	ctx := context.Background()

	// Create workflow definition
	def, _ := tc.wfAppService.CreateWorkflowDefinition(ctx, contracts.CreateWorkflowDefinitionRequest{
		Name:           "Test Workflow",
		Description:    "Test",
		Category:       workflow.WfCategorySync,
		DefinitionYAML: "name: test\ntasks: []",
		IsSystem:       false,
	})

	// Enable workflow
	tc.wfAppService.EnableWorkflow(ctx, shared.ID(def.ID()))

	// Execute workflow
	instID, err := tc.wfAppService.ExecuteWorkflow(ctx, contracts.ExecuteWorkflowRequest{
		WorkflowDefID: shared.ID(def.ID()),
		TriggerType:   workflow.TriggerTypeManual,
		TriggerParams: map[string]interface{}{"test": "value"},
	})
	if err != nil {
		t.Fatalf("ExecuteWorkflow failed: %v", err)
	}
	if instID.IsEmpty() {
		t.Error("Expected non-empty instance ID")
	}

	// Get workflow status
	status, err := tc.wfAppService.GetWorkflowStatus(ctx, instID)
	if err != nil {
		t.Fatalf("GetWorkflowStatus failed: %v", err)
	}
	if status == nil {
		t.Fatal("Expected status to be non-nil")
	}
}
