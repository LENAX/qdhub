package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/scheduler"
)

// ==================== Mock Implementations ====================

// MockSyncJobRepository is a mock implementation of sync.SyncJobRepository.
// Following DDD, this repository handles both SyncJob (aggregate root) and SyncExecution (child entity).
type MockSyncJobRepository struct {
	jobs       map[shared.ID]*sync.SyncJob
	executions map[shared.ID]*sync.SyncExecution
	createErr  error
	updateErr  error
	deleteErr  error
}

func NewMockSyncJobRepository() *MockSyncJobRepository {
	return &MockSyncJobRepository{
		jobs:       make(map[shared.ID]*sync.SyncJob),
		executions: make(map[shared.ID]*sync.SyncExecution),
	}
}

func (m *MockSyncJobRepository) Create(job *sync.SyncJob) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.jobs[job.ID] = job
	return nil
}

func (m *MockSyncJobRepository) Get(id shared.ID) (*sync.SyncJob, error) {
	job, exists := m.jobs[id]
	if !exists {
		return nil, nil
	}
	return job, nil
}

func (m *MockSyncJobRepository) Update(job *sync.SyncJob) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	m.jobs[job.ID] = job
	return nil
}

func (m *MockSyncJobRepository) Delete(id shared.ID) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	delete(m.jobs, id)
	return nil
}

func (m *MockSyncJobRepository) List() ([]*sync.SyncJob, error) {
	result := make([]*sync.SyncJob, 0, len(m.jobs))
	for _, job := range m.jobs {
		result = append(result, job)
	}
	return result, nil
}

// ==================== Child Entity Operations (SyncExecution) ====================

func (m *MockSyncJobRepository) AddExecution(exec *sync.SyncExecution) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.executions[exec.ID] = exec
	return nil
}

func (m *MockSyncJobRepository) GetExecution(id shared.ID) (*sync.SyncExecution, error) {
	exec, exists := m.executions[id]
	if !exists {
		return nil, nil
	}
	return exec, nil
}

func (m *MockSyncJobRepository) GetExecutionsByJob(jobID shared.ID) ([]*sync.SyncExecution, error) {
	result := make([]*sync.SyncExecution, 0)
	for _, exec := range m.executions {
		if exec.SyncJobID == jobID {
			result = append(result, exec)
		}
	}
	return result, nil
}

func (m *MockSyncJobRepository) UpdateExecution(exec *sync.SyncExecution) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	m.executions[exec.ID] = exec
	return nil
}

// ==================== Extended Query Operations ====================

func (m *MockSyncJobRepository) FindBy(conditions ...shared.QueryCondition) ([]*sync.SyncJob, error) {
	return m.List()
}

func (m *MockSyncJobRepository) FindByWithOrder(orderBy []shared.OrderBy, conditions ...shared.QueryCondition) ([]*sync.SyncJob, error) {
	return m.List()
}

func (m *MockSyncJobRepository) ListWithPagination(pagination shared.Pagination) (*shared.PageResult[sync.SyncJob], error) {
	jobs, _ := m.List()
	return shared.NewPageResult(jobs, int64(len(jobs)), pagination), nil
}

func (m *MockSyncJobRepository) FindByWithPagination(pagination shared.Pagination, conditions ...shared.QueryCondition) (*shared.PageResult[sync.SyncJob], error) {
	return m.ListWithPagination(pagination)
}

func (m *MockSyncJobRepository) Count(conditions ...shared.QueryCondition) (int64, error) {
	return int64(len(m.jobs)), nil
}

func (m *MockSyncJobRepository) Exists(conditions ...shared.QueryCondition) (bool, error) {
	return len(m.jobs) > 0, nil
}

// MockWorkflowDefinitionRepository is a mock implementation.
// Following DDD, this repository handles both WorkflowDefinition (aggregate root) and WorkflowInstance (child entity).
type MockWorkflowDefinitionRepository struct {
	definitions map[string]*workflow.WorkflowDefinition
	instances   map[string]*workflow.WorkflowInstance
	createErr   error
}

func NewMockWorkflowDefinitionRepository() *MockWorkflowDefinitionRepository {
	return &MockWorkflowDefinitionRepository{
		definitions: make(map[string]*workflow.WorkflowDefinition),
		instances:   make(map[string]*workflow.WorkflowInstance),
	}
}

func (m *MockWorkflowDefinitionRepository) Create(def *workflow.WorkflowDefinition) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.definitions[def.ID()] = def
	return nil
}

func (m *MockWorkflowDefinitionRepository) Get(id string) (*workflow.WorkflowDefinition, error) {
	def, exists := m.definitions[id]
	if !exists {
		return nil, nil
	}
	return def, nil
}

func (m *MockWorkflowDefinitionRepository) Update(def *workflow.WorkflowDefinition) error {
	m.definitions[def.ID()] = def
	return nil
}

func (m *MockWorkflowDefinitionRepository) Delete(id string) error {
	delete(m.definitions, id)
	return nil
}

func (m *MockWorkflowDefinitionRepository) List() ([]*workflow.WorkflowDefinition, error) {
	result := make([]*workflow.WorkflowDefinition, 0, len(m.definitions))
	for _, def := range m.definitions {
		result = append(result, def)
	}
	return result, nil
}

// ==================== Child Entity Operations (WorkflowInstance) ====================

func (m *MockWorkflowDefinitionRepository) AddInstance(inst *workflow.WorkflowInstance) error {
	m.instances[inst.ID] = inst
	return nil
}

func (m *MockWorkflowDefinitionRepository) GetInstance(id string) (*workflow.WorkflowInstance, error) {
	inst, exists := m.instances[id]
	if !exists {
		return nil, nil
	}
	return inst, nil
}

func (m *MockWorkflowDefinitionRepository) GetInstancesByDef(workflowDefID string) ([]*workflow.WorkflowInstance, error) {
	result := make([]*workflow.WorkflowInstance, 0)
	for _, inst := range m.instances {
		if inst.WorkflowID == workflowDefID {
			result = append(result, inst)
		}
	}
	return result, nil
}

func (m *MockWorkflowDefinitionRepository) UpdateInstance(inst *workflow.WorkflowInstance) error {
	m.instances[inst.ID] = inst
	return nil
}

func (m *MockWorkflowDefinitionRepository) DeleteInstance(id string) error {
	delete(m.instances, id)
	return nil
}

// ==================== Extended Query Operations ====================

func (m *MockWorkflowDefinitionRepository) FindBy(conditions ...shared.QueryCondition) ([]*workflow.WorkflowDefinition, error) {
	return m.List()
}

func (m *MockWorkflowDefinitionRepository) FindByWithOrder(orderBy []shared.OrderBy, conditions ...shared.QueryCondition) ([]*workflow.WorkflowDefinition, error) {
	return m.List()
}

func (m *MockWorkflowDefinitionRepository) ListWithPagination(pagination shared.Pagination) (*shared.PageResult[workflow.WorkflowDefinition], error) {
	defs, _ := m.List()
	return shared.NewPageResult(defs, int64(len(defs)), pagination), nil
}

func (m *MockWorkflowDefinitionRepository) FindByWithPagination(pagination shared.Pagination, conditions ...shared.QueryCondition) (*shared.PageResult[workflow.WorkflowDefinition], error) {
	return m.ListWithPagination(pagination)
}

func (m *MockWorkflowDefinitionRepository) Count(conditions ...shared.QueryCondition) (int64, error) {
	return int64(len(m.definitions)), nil
}

func (m *MockWorkflowDefinitionRepository) Exists(conditions ...shared.QueryCondition) (bool, error) {
	return len(m.definitions) > 0, nil
}

// MockTaskEngineAdapter is a mock implementation of workflow.TaskEngineAdapter.
type MockTaskEngineAdapter struct {
	submitErr      error
	cancelErr      error
	instanceID     string
	instanceStatus *workflow.WorkflowStatus
}

func NewMockTaskEngineAdapter() *MockTaskEngineAdapter {
	return &MockTaskEngineAdapter{
		instanceID: "test-instance-id",
	}
}

func (m *MockTaskEngineAdapter) SubmitWorkflow(ctx context.Context, definition *workflow.WorkflowDefinition, params map[string]interface{}) (string, error) {
	if m.submitErr != nil {
		return "", m.submitErr
	}
	return m.instanceID, nil
}

func (m *MockTaskEngineAdapter) PauseInstance(ctx context.Context, engineInstanceID string) error {
	return nil
}

func (m *MockTaskEngineAdapter) ResumeInstance(ctx context.Context, engineInstanceID string) error {
	return nil
}

func (m *MockTaskEngineAdapter) CancelInstance(ctx context.Context, engineInstanceID string) error {
	return m.cancelErr
}

func (m *MockTaskEngineAdapter) GetInstanceStatus(ctx context.Context, engineInstanceID string) (*workflow.WorkflowStatus, error) {
	if m.instanceStatus != nil {
		return m.instanceStatus, nil
	}
	return &workflow.WorkflowStatus{
		InstanceID: engineInstanceID,
		Status:     "Running",
		Progress:   50.0,
	}, nil
}

func (m *MockTaskEngineAdapter) RegisterWorkflow(ctx context.Context, definition *workflow.WorkflowDefinition) error {
	return nil
}

func (m *MockTaskEngineAdapter) UnregisterWorkflow(ctx context.Context, definitionID string) error {
	return nil
}

// MockJobScheduler is a mock implementation of sync.JobScheduler.
type MockJobScheduler struct {
	scheduledJobs map[string]string // jobID -> cronExpr
	scheduleErr   error
}

func NewMockJobScheduler() *MockJobScheduler {
	return &MockJobScheduler{
		scheduledJobs: make(map[string]string),
	}
}

func (m *MockJobScheduler) Start() {}

func (m *MockJobScheduler) Stop() context.Context {
	return context.Background()
}

func (m *MockJobScheduler) ScheduleJob(jobID string, cronExpr string) error {
	if m.scheduleErr != nil {
		return m.scheduleErr
	}
	m.scheduledJobs[jobID] = cronExpr
	return nil
}

func (m *MockJobScheduler) UnscheduleJob(jobID string) {
	delete(m.scheduledJobs, jobID)
}

func (m *MockJobScheduler) IsScheduled(jobID string) bool {
	_, exists := m.scheduledJobs[jobID]
	return exists
}

func (m *MockJobScheduler) GetNextRunTime(jobID string) *time.Time {
	return nil
}

// newTestSyncService creates a SyncApplicationService with proper dependencies for testing.
func newTestSyncService(syncJobRepo *MockSyncJobRepository, wfDefRepo *MockWorkflowDefinitionRepository, adapter *MockTaskEngineAdapter) contracts.SyncApplicationService {
	cronCalc := scheduler.NewCronSchedulerCalculatorAdapter()
	jobSched := NewMockJobScheduler()
	return impl.NewSyncApplicationService(syncJobRepo, wfDefRepo, adapter, cronCalc, jobSched)
}

// ==================== Test Cases ====================

func TestSyncApplicationService_CreateSyncJob(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		// Create a workflow definition
		wfDef := workflow.NewWorkflowDefinition("test-wf", "Test Workflow", workflow.WfCategorySync, "yaml: content", false)
		wfDefRepo.Create(wfDef)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		req := contracts.CreateSyncJobRequest{
			Name:          "Test Sync Job",
			Description:   "A test sync job",
			APIMetadataID: shared.NewID(),
			DataStoreID:   shared.NewID(),
			WorkflowDefID: shared.ID(wfDef.ID()),
			Mode:          sync.SyncModeBatch,
		}

		job, err := svc.CreateSyncJob(ctx, req)
		if err != nil {
			t.Fatalf("CreateSyncJob failed: %v", err)
		}
		if job == nil {
			t.Fatal("Expected job to be non-nil")
		}
		if job.Name != req.Name {
			t.Errorf("Expected name %s, got %s", req.Name, job.Name)
		}
	})

	t.Run("WorkflowDefinition not found", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		req := contracts.CreateSyncJobRequest{
			Name:          "Test Sync Job",
			Description:   "A test sync job",
			APIMetadataID: shared.NewID(),
			DataStoreID:   shared.NewID(),
			WorkflowDefID: shared.NewID(), // Non-existent
			Mode:          sync.SyncModeBatch,
		}

		_, err := svc.CreateSyncJob(ctx, req)
		if err == nil {
			t.Fatal("Expected error for non-existent workflow definition")
		}
	})

	t.Run("Repository error", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		syncJobRepo.createErr = errors.New("create error")
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		wfDef := workflow.NewWorkflowDefinition("test-wf", "Test Workflow", workflow.WfCategorySync, "yaml: content", false)
		wfDefRepo.Create(wfDef)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		req := contracts.CreateSyncJobRequest{
			Name:          "Test Sync Job",
			Description:   "A test sync job",
			APIMetadataID: shared.NewID(),
			DataStoreID:   shared.NewID(),
			WorkflowDefID: shared.ID(wfDef.ID()),
			Mode:          sync.SyncModeBatch,
		}

		_, err := svc.CreateSyncJob(ctx, req)
		if err == nil {
			t.Fatal("Expected error for repository failure")
		}
	})
}

func TestSyncApplicationService_GetSyncJob(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		// Create a job directly
		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
		syncJobRepo.Create(job)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		result, err := svc.GetSyncJob(ctx, job.ID)
		if err != nil {
			t.Fatalf("GetSyncJob failed: %v", err)
		}
		if result.ID != job.ID {
			t.Errorf("Expected ID %s, got %s", job.ID, result.ID)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		_, err := svc.GetSyncJob(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent job")
		}
	})
}

func TestSyncApplicationService_UpdateSyncJob(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
		syncJobRepo.Create(job)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		newName := "Updated Name"
		err := svc.UpdateSyncJob(ctx, job.ID, contracts.UpdateSyncJobRequest{
			Name: &newName,
		})
		if err != nil {
			t.Fatalf("UpdateSyncJob failed: %v", err)
		}

		updated, _ := syncJobRepo.Get(job.ID)
		if updated.Name != newName {
			t.Errorf("Expected name %s, got %s", newName, updated.Name)
		}
	})

	t.Run("Cannot update running job", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
		job.MarkRunning()
		syncJobRepo.Create(job)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		newName := "Updated Name"
		err := svc.UpdateSyncJob(ctx, job.ID, contracts.UpdateSyncJobRequest{
			Name: &newName,
		})
		if err == nil {
			t.Fatal("Expected error when updating running job")
		}
	})

	t.Run("Not found", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		newName := "Updated Name"
		err := svc.UpdateSyncJob(ctx, shared.NewID(), contracts.UpdateSyncJobRequest{
			Name: &newName,
		})
		if err == nil {
			t.Fatal("Expected error for non-existent job")
		}
	})
}

func TestSyncApplicationService_DeleteSyncJob(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
		syncJobRepo.Create(job)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		err := svc.DeleteSyncJob(ctx, job.ID)
		if err != nil {
			t.Fatalf("DeleteSyncJob failed: %v", err)
		}

		deleted, _ := syncJobRepo.Get(job.ID)
		if deleted != nil {
			t.Error("Job should be deleted")
		}
	})

	t.Run("Cannot delete running job", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
		job.MarkRunning()
		syncJobRepo.Create(job)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		err := svc.DeleteSyncJob(ctx, job.ID)
		if err == nil {
			t.Fatal("Expected error when deleting running job")
		}
	})

	t.Run("Not found", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		err := svc.DeleteSyncJob(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent job")
		}
	})
}

func TestSyncApplicationService_ExecuteSyncJob(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		// Create workflow definition
		wfDef := workflow.NewWorkflowDefinition("test-wf", "Test", workflow.WfCategorySync, "yaml: test", false)
		wfDefRepo.Create(wfDef)

		// Create job
		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.ID(wfDef.ID()), sync.SyncModeBatch)
		job.Enable()
		syncJobRepo.Create(job)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		execID, err := svc.ExecuteSyncJob(ctx, job.ID)
		if err != nil {
			t.Fatalf("ExecuteSyncJob failed: %v", err)
		}
		if execID.IsEmpty() {
			t.Error("Expected non-empty execution ID")
		}

		// Verify job is marked as running
		updated, _ := syncJobRepo.Get(job.ID)
		if updated.Status != sync.JobStatusRunning {
			t.Errorf("Expected job status running, got %s", updated.Status)
		}
	})

	t.Run("Cannot execute disabled job", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		wfDef := workflow.NewWorkflowDefinition("test-wf", "Test", workflow.WfCategorySync, "yaml: test", false)
		wfDefRepo.Create(wfDef)

		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.ID(wfDef.ID()), sync.SyncModeBatch)
		// Job is disabled by default
		syncJobRepo.Create(job)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		_, err := svc.ExecuteSyncJob(ctx, job.ID)
		if err == nil {
			t.Fatal("Expected error when executing disabled job")
		}
	})

	t.Run("Cannot execute already running job", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		wfDef := workflow.NewWorkflowDefinition("test-wf", "Test", workflow.WfCategorySync, "yaml: test", false)
		wfDefRepo.Create(wfDef)

		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.ID(wfDef.ID()), sync.SyncModeBatch)
		job.MarkRunning()
		syncJobRepo.Create(job)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		_, err := svc.ExecuteSyncJob(ctx, job.ID)
		if err == nil {
			t.Fatal("Expected error when executing already running job")
		}
	})

	t.Run("Task engine submit error", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()
		adapter.submitErr = errors.New("task engine error")

		wfDef := workflow.NewWorkflowDefinition("test-wf", "Test", workflow.WfCategorySync, "yaml: test", false)
		wfDefRepo.Create(wfDef)

		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.ID(wfDef.ID()), sync.SyncModeBatch)
		job.Enable()
		syncJobRepo.Create(job)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		_, err := svc.ExecuteSyncJob(ctx, job.ID)
		if err == nil {
			t.Fatal("Expected error when task engine fails")
		}
	})

	t.Run("Workflow definition not found", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		// Job references a non-existent workflow definition
		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
		job.Enable()
		syncJobRepo.Create(job)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		_, err := svc.ExecuteSyncJob(ctx, job.ID)
		if err == nil {
			t.Fatal("Expected error when workflow definition not found")
		}
	})
}

func TestSyncApplicationService_GetSyncExecution(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		// Create execution directly
		exec := sync.NewSyncExecution(shared.NewID(), shared.NewID())
		syncJobRepo.AddExecution(exec)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		result, err := svc.GetSyncExecution(ctx, exec.ID)
		if err != nil {
			t.Fatalf("GetSyncExecution failed: %v", err)
		}
		if result.ID != exec.ID {
			t.Errorf("Expected ID %s, got %s", exec.ID, result.ID)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		_, err := svc.GetSyncExecution(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent execution")
		}
	})
}

func TestSyncApplicationService_CancelExecution(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
		job.MarkRunning()
		syncJobRepo.Create(job)

		exec := sync.NewSyncExecution(job.ID, shared.NewID())
		exec.MarkRunning()
		syncJobRepo.AddExecution(exec)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		err := svc.CancelExecution(ctx, exec.ID)
		if err != nil {
			t.Fatalf("CancelExecution failed: %v", err)
		}

		updated, _ := syncJobRepo.GetExecution(exec.ID)
		if updated.Status != sync.ExecStatusCancelled {
			t.Errorf("Expected execution status cancelled, got %s", updated.Status)
		}
	})

	t.Run("Cannot cancel completed execution", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
		syncJobRepo.Create(job)

		exec := sync.NewSyncExecution(job.ID, shared.NewID())
		exec.MarkSuccess(100)
		syncJobRepo.AddExecution(exec)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		err := svc.CancelExecution(ctx, exec.ID)
		if err == nil {
			t.Fatal("Expected error when cancelling completed execution")
		}
	})

	t.Run("Not found", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		err := svc.CancelExecution(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent execution")
		}
	})
}

func TestSyncApplicationService_EnableDisableJob(t *testing.T) {
	ctx := context.Background()

	t.Run("Enable job", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
		syncJobRepo.Create(job)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		err := svc.EnableJob(ctx, job.ID)
		if err != nil {
			t.Fatalf("EnableJob failed: %v", err)
		}

		updated, _ := syncJobRepo.Get(job.ID)
		if updated.Status != sync.JobStatusEnabled {
			t.Errorf("Expected job status enabled, got %s", updated.Status)
		}
	})

	t.Run("Enable job with cron expression", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
		job.SetCronExpression("0 0 * * * *")
		syncJobRepo.Create(job)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		err := svc.EnableJob(ctx, job.ID)
		if err != nil {
			t.Fatalf("EnableJob failed: %v", err)
		}

		updated, _ := syncJobRepo.Get(job.ID)
		if updated.NextRunAt == nil {
			t.Error("Expected next run time to be set")
		}
	})

	t.Run("Disable job", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
		job.Enable()
		syncJobRepo.Create(job)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		err := svc.DisableJob(ctx, job.ID)
		if err != nil {
			t.Fatalf("DisableJob failed: %v", err)
		}

		updated, _ := syncJobRepo.Get(job.ID)
		if updated.Status != sync.JobStatusDisabled {
			t.Errorf("Expected job status disabled, got %s", updated.Status)
		}
	})

	t.Run("Enable not found", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		err := svc.EnableJob(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent job")
		}
	})
}

func TestSyncApplicationService_UpdateSchedule(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
		syncJobRepo.Create(job)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		cronExpr := "0 0 * * * *"
		err := svc.UpdateSchedule(ctx, job.ID, cronExpr)
		if err != nil {
			t.Fatalf("UpdateSchedule failed: %v", err)
		}

		updated, _ := syncJobRepo.Get(job.ID)
		if updated.CronExpression == nil || *updated.CronExpression != cronExpr {
			t.Error("Cron expression not updated")
		}
	})

	t.Run("Invalid cron expression", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
		syncJobRepo.Create(job)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		err := svc.UpdateSchedule(ctx, job.ID, "invalid")
		if err == nil {
			t.Fatal("Expected error for invalid cron expression")
		}
	})

	t.Run("Not found", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		err := svc.UpdateSchedule(ctx, shared.NewID(), "0 0 * * * *")
		if err == nil {
			t.Fatal("Expected error for non-existent job")
		}
	})
}

func TestSyncApplicationService_HandleExecutionCallback(t *testing.T) {
	ctx := context.Background()

	t.Run("Success callback", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
		job.MarkRunning()
		syncJobRepo.Create(job)

		exec := sync.NewSyncExecution(job.ID, shared.NewID())
		exec.MarkRunning()
		syncJobRepo.AddExecution(exec)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		err := svc.HandleExecutionCallback(ctx, contracts.ExecutionCallbackRequest{
			ExecutionID: exec.ID,
			Success:     true,
			RecordCount: 100,
		})
		if err != nil {
			t.Fatalf("HandleExecutionCallback failed: %v", err)
		}

		updated, _ := syncJobRepo.GetExecution(exec.ID)
		if updated.Status != sync.ExecStatusSuccess {
			t.Errorf("Expected execution status success, got %s", updated.Status)
		}
		if updated.RecordCount != 100 {
			t.Errorf("Expected record count 100, got %d", updated.RecordCount)
		}
	})

	t.Run("Failure callback", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
		job.MarkRunning()
		syncJobRepo.Create(job)

		exec := sync.NewSyncExecution(job.ID, shared.NewID())
		exec.MarkRunning()
		syncJobRepo.AddExecution(exec)

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		errMsg := "sync failed"
		err := svc.HandleExecutionCallback(ctx, contracts.ExecutionCallbackRequest{
			ExecutionID:  exec.ID,
			Success:      false,
			ErrorMessage: &errMsg,
		})
		if err != nil {
			t.Fatalf("HandleExecutionCallback failed: %v", err)
		}

		updated, _ := syncJobRepo.GetExecution(exec.ID)
		if updated.Status != sync.ExecStatusFailed {
			t.Errorf("Expected execution status failed, got %s", updated.Status)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		syncJobRepo := NewMockSyncJobRepository()
		wfDefRepo := NewMockWorkflowDefinitionRepository()
		adapter := NewMockTaskEngineAdapter()

		svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

		err := svc.HandleExecutionCallback(ctx, contracts.ExecutionCallbackRequest{
			ExecutionID: shared.NewID(),
			Success:     true,
			RecordCount: 100,
		})
		if err == nil {
			t.Fatal("Expected error for non-existent execution")
		}
	})
}

func TestSyncApplicationService_ListSyncJobs(t *testing.T) {
	ctx := context.Background()

	syncJobRepo := NewMockSyncJobRepository()
	wfDefRepo := NewMockWorkflowDefinitionRepository()
	adapter := NewMockTaskEngineAdapter()

	// Create multiple jobs
	for i := 0; i < 3; i++ {
		job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
		syncJobRepo.Create(job)
	}

	svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

	jobs, err := svc.ListSyncJobs(ctx)
	if err != nil {
		t.Fatalf("ListSyncJobs failed: %v", err)
	}
	if len(jobs) != 3 {
		t.Errorf("Expected 3 jobs, got %d", len(jobs))
	}
}

func TestSyncApplicationService_ListSyncExecutions(t *testing.T) {
	ctx := context.Background()

	syncJobRepo := NewMockSyncJobRepository()
	wfDefRepo := NewMockWorkflowDefinitionRepository()
	adapter := NewMockTaskEngineAdapter()

	job := sync.NewSyncJob("Test", "Desc", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
	syncJobRepo.Create(job)

	// Create multiple executions
	for i := 0; i < 3; i++ {
		exec := sync.NewSyncExecution(job.ID, shared.NewID())
		syncJobRepo.AddExecution(exec)
	}

	svc := newTestSyncService(syncJobRepo, wfDefRepo, adapter)

	execs, err := svc.ListSyncExecutions(ctx, job.ID)
	if err != nil {
		t.Fatalf("ListSyncExecutions failed: %v", err)
	}
	if len(execs) != 3 {
		t.Errorf("Expected 3 executions, got %d", len(execs))
	}
}

// Ensure unused imports don't cause errors
var _ = time.Now
