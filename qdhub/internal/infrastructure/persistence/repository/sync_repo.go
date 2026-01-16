package repository

import (
	"database/sql"
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

// ==================== Child Entity Operations (SyncExecution) ====================

// AddExecution adds a new SyncExecution to a SyncJob.
func (r *SyncJobRepositoryImpl) AddExecution(exec *sync.SyncExecution) error {
	return r.syncExecutionDAO.Create(nil, exec)
}

// GetExecution retrieves a SyncExecution by ID.
func (r *SyncJobRepositoryImpl) GetExecution(id shared.ID) (*sync.SyncExecution, error) {
	return r.syncExecutionDAO.GetByID(nil, id)
}

// GetExecutionsByJob retrieves all SyncExecutions for a SyncJob.
func (r *SyncJobRepositoryImpl) GetExecutionsByJob(jobID shared.ID) ([]*sync.SyncExecution, error) {
	return r.syncExecutionDAO.GetBySyncJob(nil, jobID)
}

// UpdateExecution updates a SyncExecution.
func (r *SyncJobRepositoryImpl) UpdateExecution(exec *sync.SyncExecution) error {
	return r.syncExecutionDAO.Update(nil, exec)
}

// ==================== Extended Query Operations ====================

// FindBy retrieves entities matching the given conditions.
func (r *SyncJobRepositoryImpl) FindBy(conditions ...shared.QueryCondition) ([]*sync.SyncJob, error) {
	return r.findByInternal(nil, nil, conditions...)
}

// FindByWithOrder retrieves entities matching conditions with ordering.
func (r *SyncJobRepositoryImpl) FindByWithOrder(orderBy []shared.OrderBy, conditions ...shared.QueryCondition) ([]*sync.SyncJob, error) {
	return r.findByInternal(orderBy, nil, conditions...)
}

// ListWithPagination retrieves entities with pagination.
func (r *SyncJobRepositoryImpl) ListWithPagination(pagination shared.Pagination) (*shared.PageResult[sync.SyncJob], error) {
	return r.FindByWithPagination(pagination)
}

// FindByWithPagination retrieves entities matching conditions with pagination.
func (r *SyncJobRepositoryImpl) FindByWithPagination(pagination shared.Pagination, conditions ...shared.QueryCondition) (*shared.PageResult[sync.SyncJob], error) {
	total, err := r.Count(conditions...)
	if err != nil {
		return nil, fmt.Errorf("failed to count entities: %w", err)
	}

	items, err := r.findByInternal(nil, &pagination, conditions...)
	if err != nil {
		return nil, err
	}

	return shared.NewPageResult(items, total, pagination), nil
}

// Count returns the total count of entities matching conditions.
func (r *SyncJobRepositoryImpl) Count(conditions ...shared.QueryCondition) (int64, error) {
	whereClause, args := buildWhereClause(conditions...)
	query := fmt.Sprintf("SELECT COUNT(*) FROM sync_jobs%s", whereClause)

	var count int64
	err := r.db.DB.Get(&count, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to count sync_jobs: %w", err)
	}
	return count, nil
}

// Exists checks if any entity matching conditions exists.
func (r *SyncJobRepositoryImpl) Exists(conditions ...shared.QueryCondition) (bool, error) {
	count, err := r.Count(conditions...)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (r *SyncJobRepositoryImpl) findByInternal(orderBy []shared.OrderBy, pagination *shared.Pagination, conditions ...shared.QueryCondition) ([]*sync.SyncJob, error) {
	whereClause, args := buildWhereClause(conditions...)
	orderClause := buildOrderClause(orderBy)
	limitClause := buildLimitClause(pagination)

	query := fmt.Sprintf("SELECT * FROM sync_jobs%s%s%s", whereClause, orderClause, limitClause)

	var rows []dao.SyncJobRow
	err := r.db.DB.Select(&rows, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return []*sync.SyncJob{}, nil
		}
		return nil, fmt.Errorf("failed to find sync_jobs: %w", err)
	}

	entities := make([]*sync.SyncJob, 0, len(rows))
	for _, row := range rows {
		entity, err := r.rowToEntity(&row)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

// rowToEntity converts database row to domain entity.
func (r *SyncJobRepositoryImpl) rowToEntity(row *dao.SyncJobRow) (*sync.SyncJob, error) {
	entity := &sync.SyncJob{
		ID:            shared.ID(row.ID),
		Name:          row.Name,
		Description:   row.Description,
		APIMetadataID: shared.ID(row.APIMetadataID),
		DataStoreID:   shared.ID(row.DataStoreID),
		WorkflowDefID: shared.ID(row.WorkflowDefID),
		Mode:          sync.SyncMode(row.Mode),
		Status:        sync.JobStatus(row.Status),
		CreatedAt:     shared.Timestamp(row.CreatedAt),
		UpdatedAt:     shared.Timestamp(row.UpdatedAt),
	}

	if row.CronExpression.Valid {
		entity.CronExpression = &row.CronExpression.String
	}

	if row.LastRunAt.Valid {
		entity.LastRunAt = &row.LastRunAt.Time
	}

	if row.NextRunAt.Valid {
		entity.NextRunAt = &row.NextRunAt.Time
	}

	if row.Params != "" {
		if err := entity.UnmarshalParamsJSON(row.Params); err != nil {
			return nil, fmt.Errorf("failed to unmarshal params: %w", err)
		}
	} else {
		entity.Params = make(map[string]interface{})
	}

	if row.ParamRules != "" {
		if err := entity.UnmarshalParamRulesJSON(row.ParamRules); err != nil {
			return nil, fmt.Errorf("failed to unmarshal param rules: %w", err)
		}
	} else {
		entity.ParamRules = []sync.ParamRule{}
	}

	return entity, nil
}
