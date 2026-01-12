package repository

import (
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/dao"
)

// WorkflowDefinitionRepositoryImpl implements workflow.WorkflowDefinitionRepository.
type WorkflowDefinitionRepositoryImpl struct {
	db                  *persistence.DB
	workflowDefDAO      *dao.WorkflowDefinitionDAO
	workflowInstDAO     *dao.WorkflowInstanceDAO
	taskInstDAO         *dao.TaskInstanceDAO
}

// NewWorkflowDefinitionRepository creates a new WorkflowDefinitionRepositoryImpl.
func NewWorkflowDefinitionRepository(db *persistence.DB) *WorkflowDefinitionRepositoryImpl {
	return &WorkflowDefinitionRepositoryImpl{
		db:              db,
		workflowDefDAO:  dao.NewWorkflowDefinitionDAO(db.DB),
		workflowInstDAO: dao.NewWorkflowInstanceDAO(db.DB),
		taskInstDAO:     dao.NewTaskInstanceDAO(db.DB),
	}
}

// Create creates a new workflow definition with its aggregated entities.
func (r *WorkflowDefinitionRepositoryImpl) Create(def *workflow.WorkflowDefinition) error {
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		// Create workflow definition
		if err := r.workflowDefDAO.Create(tx, def); err != nil {
			return err
		}

		// Create instances
		for _, inst := range def.Instances {
			if err := r.workflowInstDAO.Create(tx, &inst); err != nil {
				return err
			}

			// Create task instances for each workflow instance
			for _, task := range inst.TaskInstances {
				if err := r.taskInstDAO.Create(tx, &task); err != nil {
					return err
				}
			}
		}

		return nil
	})
}

// Get retrieves a workflow definition by ID with its aggregated entities.
func (r *WorkflowDefinitionRepositoryImpl) Get(id shared.ID) (*workflow.WorkflowDefinition, error) {
	def, err := r.workflowDefDAO.GetByID(nil, id)
	if err != nil {
		return nil, err
	}
	if def == nil {
		return nil, nil
	}

	// Load instances
	instances, err := r.workflowInstDAO.GetByWorkflowDef(nil, id)
	if err != nil {
		return nil, fmt.Errorf("failed to load workflow instances: %w", err)
	}

	def.Instances = make([]workflow.WorkflowInstance, len(instances))
	for i, inst := range instances {
		// Load task instances for each workflow instance
		tasks, err := r.taskInstDAO.GetByWorkflowInstance(nil, inst.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to load task instances: %w", err)
		}
		inst.TaskInstances = make([]workflow.TaskInstance, len(tasks))
		for j, task := range tasks {
			inst.TaskInstances[j] = *task
		}
		def.Instances[i] = *inst
	}

	return def, nil
}

// Update updates a workflow definition.
func (r *WorkflowDefinitionRepositoryImpl) Update(def *workflow.WorkflowDefinition) error {
	return r.workflowDefDAO.Update(nil, def)
}

// Delete deletes a workflow definition and its aggregated entities.
func (r *WorkflowDefinitionRepositoryImpl) Delete(id shared.ID) error {
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		// Get all workflow instances first
		instances, err := r.workflowInstDAO.GetByWorkflowDef(tx, id)
		if err != nil {
			return err
		}

		// Delete task instances for each workflow instance
		for _, inst := range instances {
			if err := r.taskInstDAO.DeleteByWorkflowInstance(tx, inst.ID); err != nil {
				return err
			}
		}

		// Delete workflow instances
		if err := r.workflowInstDAO.DeleteByWorkflowDef(tx, id); err != nil {
			return err
		}

		// Delete workflow definition
		if err := r.workflowDefDAO.DeleteByID(tx, id); err != nil {
			return err
		}

		return nil
	})
}

// List retrieves all workflow definitions (without aggregated entities for performance).
func (r *WorkflowDefinitionRepositoryImpl) List() ([]*workflow.WorkflowDefinition, error) {
	return r.workflowDefDAO.ListAll(nil)
}

// WorkflowInstanceRepositoryImpl implements workflow.WorkflowInstanceRepository.
type WorkflowInstanceRepositoryImpl struct {
	db              *persistence.DB
	workflowInstDAO *dao.WorkflowInstanceDAO
	taskInstDAO     *dao.TaskInstanceDAO
}

// NewWorkflowInstanceRepository creates a new WorkflowInstanceRepositoryImpl.
func NewWorkflowInstanceRepository(db *persistence.DB) *WorkflowInstanceRepositoryImpl {
	return &WorkflowInstanceRepositoryImpl{
		db:              db,
		workflowInstDAO: dao.NewWorkflowInstanceDAO(db.DB),
		taskInstDAO:     dao.NewTaskInstanceDAO(db.DB),
	}
}

// Create creates a new workflow instance with its task instances.
func (r *WorkflowInstanceRepositoryImpl) Create(inst *workflow.WorkflowInstance) error {
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		// Create workflow instance
		if err := r.workflowInstDAO.Create(tx, inst); err != nil {
			return err
		}

		// Create task instances
		for _, task := range inst.TaskInstances {
			if err := r.taskInstDAO.Create(tx, &task); err != nil {
				return err
			}
		}

		return nil
	})
}

// Get retrieves a workflow instance by ID with its task instances.
func (r *WorkflowInstanceRepositoryImpl) Get(id shared.ID) (*workflow.WorkflowInstance, error) {
	inst, err := r.workflowInstDAO.GetByID(nil, id)
	if err != nil {
		return nil, err
	}
	if inst == nil {
		return nil, nil
	}

	// Load task instances
	tasks, err := r.taskInstDAO.GetByWorkflowInstance(nil, id)
	if err != nil {
		return nil, fmt.Errorf("failed to load task instances: %w", err)
	}
	inst.TaskInstances = make([]workflow.TaskInstance, len(tasks))
	for i, task := range tasks {
		inst.TaskInstances[i] = *task
	}

	return inst, nil
}

// GetByWorkflowDef retrieves all workflow instances for a workflow definition.
func (r *WorkflowInstanceRepositoryImpl) GetByWorkflowDef(workflowDefID shared.ID) ([]*workflow.WorkflowInstance, error) {
	return r.workflowInstDAO.GetByWorkflowDef(nil, workflowDefID)
}

// Update updates a workflow instance.
func (r *WorkflowInstanceRepositoryImpl) Update(inst *workflow.WorkflowInstance) error {
	return r.workflowInstDAO.Update(nil, inst)
}

// Delete deletes a workflow instance and its task instances.
func (r *WorkflowInstanceRepositoryImpl) Delete(id shared.ID) error {
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		// Delete task instances first
		if err := r.taskInstDAO.DeleteByWorkflowInstance(tx, id); err != nil {
			return err
		}

		// Delete workflow instance
		if err := r.workflowInstDAO.DeleteByID(tx, id); err != nil {
			return err
		}

		return nil
	})
}
