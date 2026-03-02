package dao_test

import (
	"testing"
	"time"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/persistence/dao"
)

func TestWorkflowDefinitionDAO_Create(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	// Add workflow_definitions table
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS workflow_definitions (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			description TEXT,
			category TEXT,
			definition_yaml TEXT,
			version INTEGER DEFAULT 1,
			status TEXT NOT NULL DEFAULT 'enabled',
			is_system INTEGER DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewWorkflowDefinitionDAO(db.DB)

	wf := workflow.NewWorkflowDefinition("Test Workflow", "Test Description", workflow.WfCategoryCustom, "name: test", false)

	err = dao.Create(nil, wf)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if wf.ID() == "" {
		t.Error("WorkflowDefinition ID should be set")
	}
}

func TestWorkflowDefinitionDAO_GetByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS workflow_definitions (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			description TEXT,
			category TEXT,
			definition_yaml TEXT,
			version INTEGER DEFAULT 1,
			status TEXT NOT NULL DEFAULT 'enabled',
			is_system INTEGER DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewWorkflowDefinitionDAO(db.DB)

	wf := workflow.NewWorkflowDefinition("Test Workflow", "Test Description", workflow.WfCategoryCustom, "name: test", false)
	err = dao.Create(nil, wf)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := dao.GetByID(nil, shared.ID(wf.ID()))
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got == nil {
		t.Fatal("GetByID() returned nil")
	}

	if got.ID() != wf.ID() {
		t.Errorf("GetByID() ID = %s, want %s", got.ID(), wf.ID())
	}
}

func TestWorkflowDefinitionDAO_Update(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS workflow_definitions (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			description TEXT,
			category TEXT,
			definition_yaml TEXT,
			version INTEGER DEFAULT 1,
			status TEXT NOT NULL DEFAULT 'enabled',
			is_system INTEGER DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewWorkflowDefinitionDAO(db.DB)

	wf := workflow.NewWorkflowDefinition("Original", "Original Desc", workflow.WfCategoryCustom, "name: original", false)
	err = dao.Create(nil, wf)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	wf.Workflow.Name = "Updated"
	wf.UpdateDefinition("name: updated")
	err = dao.Update(nil, wf)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := dao.GetByID(nil, shared.ID(wf.ID()))
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got.Workflow.Name != "Updated" {
		t.Errorf("Update() Name = %s, want Updated", got.Workflow.Name)
	}
}

func TestWorkflowDefinitionDAO_DeleteByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS workflow_definitions (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			description TEXT,
			category TEXT,
			definition_yaml TEXT,
			version INTEGER DEFAULT 1,
			status TEXT NOT NULL DEFAULT 'enabled',
			is_system INTEGER DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewWorkflowDefinitionDAO(db.DB)

	wf := workflow.NewWorkflowDefinition("To Delete", "Desc", workflow.WfCategoryCustom, "name: delete", false)
	err = dao.Create(nil, wf)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = dao.DeleteByID(nil, shared.ID(wf.ID()))
	if err != nil {
		t.Fatalf("DeleteByID() error = %v", err)
	}

	got, err := dao.GetByID(nil, shared.ID(wf.ID()))
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got != nil {
		t.Error("DeleteByID() should remove the workflow definition")
	}
}

func TestWorkflowDefinitionDAO_ListAll(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS workflow_definitions (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			description TEXT,
			category TEXT,
			definition_yaml TEXT,
			version INTEGER DEFAULT 1,
			status TEXT NOT NULL DEFAULT 'enabled',
			is_system INTEGER DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewWorkflowDefinitionDAO(db.DB)

	wf1 := workflow.NewWorkflowDefinition("Workflow 1", "Desc 1", workflow.WfCategoryCustom, "name: wf1", false)
	wf2 := workflow.NewWorkflowDefinition("Workflow 2", "Desc 2", workflow.WfCategorySync, "name: wf2", false)

	err = dao.Create(nil, wf1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = dao.Create(nil, wf2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	list, err := dao.ListAll(nil)
	if err != nil {
		t.Fatalf("ListAll() error = %v", err)
	}

	if len(list) < 2 {
		t.Errorf("ListAll() returned %d workflows, want at least 2", len(list))
	}
}

func TestWorkflowInstanceDAO_Create(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	// Add workflow_instances table
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS workflow_instances (
			id TEXT PRIMARY KEY,
			workflow_def_id TEXT NOT NULL,
			engine_instance_id TEXT,
			trigger_type TEXT,
			trigger_params TEXT,
			status TEXT NOT NULL,
			progress REAL DEFAULT 0,
			started_at TIMESTAMP,
			finished_at TIMESTAMP,
			error_message TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewWorkflowInstanceDAO(db.DB)

	workflowDefID := shared.NewID().String()
	inst := workflow.NewWorkflowInstance(workflowDefID)

	err = dao.Create(nil, inst)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if inst.ID == "" {
		t.Error("WorkflowInstance ID should be set")
	}
}

func TestWorkflowInstanceDAO_GetByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS workflow_instances (
			id TEXT PRIMARY KEY,
			workflow_def_id TEXT NOT NULL,
			engine_instance_id TEXT,
			trigger_type TEXT,
			trigger_params TEXT,
			status TEXT NOT NULL,
			progress REAL DEFAULT 0,
			started_at TIMESTAMP,
			finished_at TIMESTAMP,
			error_message TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewWorkflowInstanceDAO(db.DB)

	workflowDefID := shared.NewID().String()
	inst := workflow.NewWorkflowInstance(workflowDefID)
	err = dao.Create(nil, inst)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := dao.GetByID(nil, shared.ID(inst.ID))
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got == nil {
		t.Fatal("GetByID() returned nil")
	}

	if got.ID != inst.ID {
		t.Errorf("GetByID() ID = %s, want %s", got.ID, inst.ID)
	}
}

func TestWorkflowInstanceDAO_GetByWorkflowDef(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS workflow_instances (
			id TEXT PRIMARY KEY,
			workflow_def_id TEXT NOT NULL,
			engine_instance_id TEXT,
			trigger_type TEXT,
			trigger_params TEXT,
			status TEXT NOT NULL,
			progress REAL DEFAULT 0,
			started_at TIMESTAMP,
			finished_at TIMESTAMP,
			error_message TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewWorkflowInstanceDAO(db.DB)

	workflowDefID := shared.NewID().String()
	inst1 := workflow.NewWorkflowInstance(workflowDefID)
	inst2 := workflow.NewWorkflowInstance(workflowDefID)

	err = dao.Create(nil, inst1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = dao.Create(nil, inst2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	instances, err := dao.GetByWorkflowDef(nil, shared.ID(workflowDefID))
	if err != nil {
		t.Fatalf("GetByWorkflowDef() error = %v", err)
	}

	if len(instances) < 2 {
		t.Errorf("GetByWorkflowDef() returned %d instances, want at least 2", len(instances))
	}
}

func TestWorkflowInstanceDAO_Update(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS workflow_instances (
			id TEXT PRIMARY KEY,
			workflow_def_id TEXT NOT NULL,
			engine_instance_id TEXT,
			trigger_type TEXT,
			trigger_params TEXT,
			status TEXT NOT NULL,
			progress REAL DEFAULT 0,
			started_at TIMESTAMP,
			finished_at TIMESTAMP,
			error_message TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewWorkflowInstanceDAO(db.DB)

	workflowDefID := shared.NewID().String()
	inst := workflow.NewWorkflowInstance(workflowDefID)
	err = dao.Create(nil, inst)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	inst.Status = "Running"
	err = dao.Update(nil, inst)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := dao.GetByID(nil, shared.ID(inst.ID))
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got.Status != "Running" {
		t.Errorf("Update() Status = %s, want Running", got.Status)
	}
}

func TestWorkflowInstanceDAO_DeleteByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS workflow_instances (
			id TEXT PRIMARY KEY,
			workflow_def_id TEXT NOT NULL,
			engine_instance_id TEXT,
			trigger_type TEXT,
			trigger_params TEXT,
			status TEXT NOT NULL,
			progress REAL DEFAULT 0,
			started_at TIMESTAMP,
			finished_at TIMESTAMP,
			error_message TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewWorkflowInstanceDAO(db.DB)

	workflowDefID := shared.NewID().String()
	inst := workflow.NewWorkflowInstance(workflowDefID)
	err = dao.Create(nil, inst)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = dao.DeleteByID(nil, shared.ID(inst.ID))
	if err != nil {
		t.Fatalf("DeleteByID() error = %v", err)
	}

	got, err := dao.GetByID(nil, shared.ID(inst.ID))
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got != nil {
		t.Error("DeleteByID() should remove the workflow instance")
	}
}

func TestWorkflowInstanceDAO_DeleteByWorkflowDef(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS workflow_instances (
			id TEXT PRIMARY KEY,
			workflow_def_id TEXT NOT NULL,
			engine_instance_id TEXT,
			trigger_type TEXT,
			trigger_params TEXT,
			status TEXT NOT NULL,
			progress REAL DEFAULT 0,
			started_at TIMESTAMP,
			finished_at TIMESTAMP,
			error_message TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewWorkflowInstanceDAO(db.DB)

	workflowDefID := shared.NewID().String()
	inst1 := workflow.NewWorkflowInstance(workflowDefID)
	inst2 := workflow.NewWorkflowInstance(workflowDefID)

	err = dao.Create(nil, inst1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = dao.Create(nil, inst2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Delete all instances for the workflow definition
	err = dao.DeleteByWorkflowDef(nil, shared.ID(workflowDefID))
	if err != nil {
		t.Fatalf("DeleteByWorkflowDef() error = %v", err)
	}

	// Verify all instances are deleted
	instances, err := dao.GetByWorkflowDef(nil, shared.ID(workflowDefID))
	if err != nil {
		t.Fatalf("GetByWorkflowDef() error = %v", err)
	}

	if len(instances) != 0 {
		t.Errorf("DeleteByWorkflowDef() should remove all instances, got %d remaining", len(instances))
	}
}

func TestWorkflowInstanceDAO_toRow_WithOptionalFields(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS workflow_instances (
			id TEXT PRIMARY KEY,
			workflow_def_id TEXT NOT NULL,
			engine_instance_id TEXT,
			trigger_type TEXT,
			trigger_params TEXT,
			status TEXT NOT NULL,
			progress REAL DEFAULT 0,
			started_at TIMESTAMP,
			finished_at TIMESTAMP,
			error_message TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewWorkflowInstanceDAO(db.DB)

	// Test with instance that has EndTime and ErrorMessage set
	workflowDefID := shared.NewID().String()
	inst := workflow.NewWorkflowInstance(workflowDefID)
	inst.Status = "Failed"

	// Set optional fields
	endTime := time.Now()
	inst.EndTime = &endTime
	inst.ErrorMessage = "Test error"

	err = dao.Create(nil, inst)
	if err != nil {
		t.Fatalf("Create() with optional fields error = %v", err)
	}

	// Verify the optional fields were saved
	got, err := dao.GetByID(nil, shared.ID(inst.ID))
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got == nil {
		t.Fatal("GetByID() returned nil")
	}

	if got.ErrorMessage != "Test error" {
		t.Errorf("GetByID() ErrorMessage = %s, want Test error", got.ErrorMessage)
	}
}

func TestTaskInstanceDAO_Create(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	// Add task_instances table (matching DAO structure)
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS task_instances (
			id TEXT PRIMARY KEY,
			workflow_inst_id TEXT NOT NULL,
			task_name TEXT NOT NULL,
			status TEXT NOT NULL,
			started_at TIMESTAMP,
			finished_at TIMESTAMP,
			retry_count INTEGER DEFAULT 0,
			error_message TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewTaskInstanceDAO(db.DB)

	workflowInstID := shared.NewID().String()
	// TaskInstance is a type alias from Task Engine
	// We need to create it with the correct structure
	taskInst := &workflow.TaskInstance{
		ID:                 shared.NewID().String(),
		Name:               "test_task",
		WorkflowInstanceID: workflowInstID,
		Status:             "Pending",
		RetryCount:         0,
	}

	err = dao.Create(nil, taskInst)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if taskInst.ID == "" {
		t.Error("TaskInstance ID should be set")
	}
}

func TestTaskInstanceDAO_GetByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS task_instances (
			id TEXT PRIMARY KEY,
			workflow_inst_id TEXT NOT NULL,
			task_name TEXT NOT NULL,
			status TEXT NOT NULL,
			started_at TIMESTAMP,
			finished_at TIMESTAMP,
			retry_count INTEGER DEFAULT 0,
			error_message TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewTaskInstanceDAO(db.DB)

	workflowInstID := shared.NewID().String()
	taskInst := &workflow.TaskInstance{
		ID:                 shared.NewID().String(),
		Name:               "test_task",
		WorkflowInstanceID: workflowInstID,
		Status:             "Pending",
		RetryCount:         0,
	}
	err = dao.Create(nil, taskInst)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := dao.GetByID(nil, shared.ID(taskInst.ID))
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got == nil {
		t.Fatal("GetByID() returned nil")
	}

	if got.ID != taskInst.ID {
		t.Errorf("GetByID() ID = %s, want %s", got.ID, taskInst.ID)
	}
}

func TestTaskInstanceDAO_GetByWorkflowInstance(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS task_instances (
			id TEXT PRIMARY KEY,
			workflow_inst_id TEXT NOT NULL,
			task_name TEXT NOT NULL,
			status TEXT NOT NULL,
			started_at TIMESTAMP,
			finished_at TIMESTAMP,
			retry_count INTEGER DEFAULT 0,
			error_message TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewTaskInstanceDAO(db.DB)

	workflowInstID := shared.NewID().String()
	taskInst1 := &workflow.TaskInstance{
		ID:                 shared.NewID().String(),
		Name:               "task1",
		WorkflowInstanceID: workflowInstID,
		Status:             "Pending",
		RetryCount:         0,
	}
	taskInst2 := &workflow.TaskInstance{
		ID:                 shared.NewID().String(),
		Name:               "task2",
		WorkflowInstanceID: workflowInstID,
		Status:             "Pending",
		RetryCount:         0,
	}

	err = dao.Create(nil, taskInst1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = dao.Create(nil, taskInst2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	tasks, err := dao.GetByWorkflowInstance(nil, shared.ID(workflowInstID))
	if err != nil {
		t.Fatalf("GetByWorkflowInstance() error = %v", err)
	}

	if len(tasks) < 2 {
		t.Errorf("GetByWorkflowInstance() returned %d tasks, want at least 2", len(tasks))
	}
}

func TestTaskInstanceDAO_Update(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS task_instances (
			id TEXT PRIMARY KEY,
			workflow_inst_id TEXT NOT NULL,
			task_name TEXT NOT NULL,
			status TEXT NOT NULL,
			started_at TIMESTAMP,
			finished_at TIMESTAMP,
			retry_count INTEGER DEFAULT 0,
			error_message TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewTaskInstanceDAO(db.DB)

	workflowInstID := shared.NewID().String()
	taskInst := &workflow.TaskInstance{
		ID:                 shared.NewID().String(),
		Name:               "test_task",
		WorkflowInstanceID: workflowInstID,
		Status:             "Pending",
		RetryCount:         0,
	}
	err = dao.Create(nil, taskInst)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	taskInst.Status = "Running"
	err = dao.Update(nil, taskInst)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := dao.GetByID(nil, shared.ID(taskInst.ID))
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got.Status != "Running" {
		t.Errorf("Update() Status = %s, want Running", got.Status)
	}
}

func TestTaskInstanceDAO_DeleteByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS task_instances (
			id TEXT PRIMARY KEY,
			workflow_inst_id TEXT NOT NULL,
			task_name TEXT NOT NULL,
			status TEXT NOT NULL,
			started_at TIMESTAMP,
			finished_at TIMESTAMP,
			retry_count INTEGER DEFAULT 0,
			error_message TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewTaskInstanceDAO(db.DB)

	workflowInstID := shared.NewID().String()
	taskInst := &workflow.TaskInstance{
		ID:                 shared.NewID().String(),
		Name:               "test_task",
		WorkflowInstanceID: workflowInstID,
		Status:             "Pending",
		RetryCount:         0,
	}
	err = dao.Create(nil, taskInst)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = dao.DeleteByID(nil, shared.ID(taskInst.ID))
	if err != nil {
		t.Fatalf("DeleteByID() error = %v", err)
	}

	got, err := dao.GetByID(nil, shared.ID(taskInst.ID))
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got != nil {
		t.Error("DeleteByID() should remove the task instance")
	}
}

func TestTaskInstanceDAO_DeleteByWorkflowInstance(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS task_instances (
			id TEXT PRIMARY KEY,
			workflow_inst_id TEXT NOT NULL,
			task_name TEXT NOT NULL,
			status TEXT NOT NULL,
			started_at TIMESTAMP,
			finished_at TIMESTAMP,
			retry_count INTEGER DEFAULT 0,
			error_message TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewTaskInstanceDAO(db.DB)

	workflowInstID := shared.NewID().String()
	taskInst1 := &workflow.TaskInstance{
		ID:                 shared.NewID().String(),
		Name:               "task1",
		WorkflowInstanceID: workflowInstID,
		Status:             "Pending",
		RetryCount:         0,
	}
	taskInst2 := &workflow.TaskInstance{
		ID:                 shared.NewID().String(),
		Name:               "task2",
		WorkflowInstanceID: workflowInstID,
		Status:             "Pending",
		RetryCount:         0,
	}

	err = dao.Create(nil, taskInst1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = dao.Create(nil, taskInst2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Delete all tasks for the workflow instance
	err = dao.DeleteByWorkflowInstance(nil, shared.ID(workflowInstID))
	if err != nil {
		t.Fatalf("DeleteByWorkflowInstance() error = %v", err)
	}

	// Verify all tasks are deleted
	tasks, err := dao.GetByWorkflowInstance(nil, shared.ID(workflowInstID))
	if err != nil {
		t.Fatalf("GetByWorkflowInstance() error = %v", err)
	}

	if len(tasks) != 0 {
		t.Errorf("DeleteByWorkflowInstance() should remove all tasks, got %d remaining", len(tasks))
	}
}

func TestTaskInstanceDAO_toRow_WithOptionalFields(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS task_instances (
			id TEXT PRIMARY KEY,
			workflow_inst_id TEXT NOT NULL,
			task_name TEXT NOT NULL,
			status TEXT NOT NULL,
			started_at TIMESTAMP,
			finished_at TIMESTAMP,
			retry_count INTEGER DEFAULT 0,
			error_message TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewTaskInstanceDAO(db.DB)

	workflowInstID := shared.NewID().String()
	taskInst := &workflow.TaskInstance{
		ID:                 shared.NewID().String(),
		Name:               "test_task",
		WorkflowInstanceID: workflowInstID,
		Status:             "Failed",
		RetryCount:         2,
	}

	// Set optional fields
	startTime := time.Now()
	endTime := startTime.Add(time.Minute)
	taskInst.StartTime = &startTime
	taskInst.EndTime = &endTime
	taskInst.ErrorMessage = "Task failed"

	err = dao.Create(nil, taskInst)
	if err != nil {
		t.Fatalf("Create() with optional fields error = %v", err)
	}

	// Verify the optional fields were saved
	got, err := dao.GetByID(nil, shared.ID(taskInst.ID))
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got == nil {
		t.Fatal("GetByID() returned nil")
	}

	if got.ErrorMessage != "Task failed" {
		t.Errorf("GetByID() ErrorMessage = %s, want Task failed", got.ErrorMessage)
	}

	if got.RetryCount != 2 {
		t.Errorf("GetByID() RetryCount = %d, want 2", got.RetryCount)
	}
}
