package repository

import (
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/dao"
)

// SyncJobRepositoryImpl implements sync.SyncJobRepository.
type SyncJobRepositoryImpl struct {
	db               *persistence.DB
	syncJobDAO       *dao.SyncJobDAO
	syncExecutionDAO *dao.SyncExecutionDAO
}

// NewSyncJobRepository creates a new SyncJobRepositoryImpl.
func NewSyncJobRepository(db *persistence.DB) *SyncJobRepositoryImpl {
	return &SyncJobRepositoryImpl{
		db:               db,
		syncJobDAO:       dao.NewSyncJobDAO(db.DB),
		syncExecutionDAO: dao.NewSyncExecutionDAO(db.DB),
	}
}

// Create creates a new sync job with its aggregated entities.
func (r *SyncJobRepositoryImpl) Create(job *sync.SyncJob) error {
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		// Create sync job
		if err := r.syncJobDAO.Create(tx, job); err != nil {
			return err
		}

		// Create executions
		for _, exec := range job.Executions {
			if err := r.syncExecutionDAO.Create(tx, &exec); err != nil {
				return err
			}
		}

		return nil
	})
}

// Get retrieves a sync job by ID with its aggregated entities.
func (r *SyncJobRepositoryImpl) Get(id shared.ID) (*sync.SyncJob, error) {
	job, err := r.syncJobDAO.GetByID(nil, id)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, nil
	}

	// Load executions
	executions, err := r.syncExecutionDAO.GetBySyncJob(nil, id)
	if err != nil {
		return nil, fmt.Errorf("failed to load executions: %w", err)
	}
	job.Executions = make([]sync.SyncExecution, len(executions))
	for i, exec := range executions {
		job.Executions[i] = *exec
	}

	return job, nil
}

// Update updates a sync job.
func (r *SyncJobRepositoryImpl) Update(job *sync.SyncJob) error {
	return r.syncJobDAO.Update(nil, job)
}

// Delete deletes a sync job and its aggregated entities.
func (r *SyncJobRepositoryImpl) Delete(id shared.ID) error {
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		// Delete executions first
		if err := r.syncExecutionDAO.DeleteBySyncJob(tx, id); err != nil {
			return err
		}

		// Delete sync job
		if err := r.syncJobDAO.DeleteByID(tx, id); err != nil {
			return err
		}

		return nil
	})
}

// List retrieves all sync jobs (without aggregated entities for performance).
func (r *SyncJobRepositoryImpl) List() ([]*sync.SyncJob, error) {
	return r.syncJobDAO.ListAll(nil)
}

// SyncExecutionRepositoryImpl implements sync.SyncExecutionRepository.
type SyncExecutionRepositoryImpl struct {
	db               *persistence.DB
	syncExecutionDAO *dao.SyncExecutionDAO
}

// NewSyncExecutionRepository creates a new SyncExecutionRepositoryImpl.
func NewSyncExecutionRepository(db *persistence.DB) *SyncExecutionRepositoryImpl {
	return &SyncExecutionRepositoryImpl{
		db:               db,
		syncExecutionDAO: dao.NewSyncExecutionDAO(db.DB),
	}
}

// Create creates a new sync execution.
func (r *SyncExecutionRepositoryImpl) Create(exec *sync.SyncExecution) error {
	return r.syncExecutionDAO.Create(nil, exec)
}

// Get retrieves a sync execution by ID.
func (r *SyncExecutionRepositoryImpl) Get(id shared.ID) (*sync.SyncExecution, error) {
	return r.syncExecutionDAO.GetByID(nil, id)
}

// GetBySyncJob retrieves all sync executions for a sync job.
func (r *SyncExecutionRepositoryImpl) GetBySyncJob(syncJobID shared.ID) ([]*sync.SyncExecution, error) {
	return r.syncExecutionDAO.GetBySyncJob(nil, syncJobID)
}

// Update updates a sync execution.
func (r *SyncExecutionRepositoryImpl) Update(exec *sync.SyncExecution) error {
	return r.syncExecutionDAO.Update(nil, exec)
}

// Delete deletes a sync execution.
func (r *SyncExecutionRepositoryImpl) Delete(id shared.ID) error {
	return r.syncExecutionDAO.DeleteByID(nil, id)
}
