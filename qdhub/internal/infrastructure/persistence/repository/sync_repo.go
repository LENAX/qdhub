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

// SyncPlanRepositoryImpl implements sync.SyncPlanRepository.
type SyncPlanRepositoryImpl struct {
	db               *persistence.DB
	syncPlanDAO      *dao.SyncPlanDAO
	syncTaskDAO      *dao.SyncTaskDAO
	syncExecutionDAO *dao.SyncExecutionDAO
}

// NewSyncPlanRepository creates a new SyncPlanRepositoryImpl.
func NewSyncPlanRepository(db *persistence.DB) *SyncPlanRepositoryImpl {
	return &SyncPlanRepositoryImpl{
		db:               db,
		syncPlanDAO:      dao.NewSyncPlanDAO(db.DB),
		syncTaskDAO:      dao.NewSyncTaskDAO(db.DB),
		syncExecutionDAO: dao.NewSyncExecutionDAO(db.DB),
	}
}

// Create creates a new sync plan with its aggregated entities.
func (r *SyncPlanRepositoryImpl) Create(plan *sync.SyncPlan) error {
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		// Create sync plan
		if err := r.syncPlanDAO.Create(tx, plan); err != nil {
			return err
		}

		// Create tasks
		for _, task := range plan.Tasks {
			if err := r.syncTaskDAO.Create(tx, task); err != nil {
				return err
			}
		}

		// Create executions
		for _, exec := range plan.Executions {
			if err := r.syncExecutionDAO.Create(tx, exec); err != nil {
				return err
			}
		}

		return nil
	})
}

// Get retrieves a sync plan by ID with its aggregated entities.
func (r *SyncPlanRepositoryImpl) Get(id shared.ID) (*sync.SyncPlan, error) {
	plan, err := r.syncPlanDAO.GetByID(nil, id)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, nil
	}

	// Load tasks
	tasks, err := r.syncTaskDAO.GetByPlanID(nil, id)
	if err != nil {
		return nil, fmt.Errorf("failed to load tasks: %w", err)
	}
	plan.Tasks = tasks

	// Load executions (lazy load, only recent ones)
	executions, err := r.syncExecutionDAO.GetByPlanID(nil, id)
	if err != nil {
		return nil, fmt.Errorf("failed to load executions: %w", err)
	}
	plan.Executions = executions

	return plan, nil
}

// Update updates a sync plan.
func (r *SyncPlanRepositoryImpl) Update(plan *sync.SyncPlan) error {
	return r.syncPlanDAO.Update(nil, plan)
}

// Delete deletes a sync plan and its aggregated entities.
func (r *SyncPlanRepositoryImpl) Delete(id shared.ID) error {
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		// Delete tasks first
		if err := r.syncTaskDAO.DeleteByPlanID(tx, id); err != nil {
			return err
		}

		// Delete sync plan
		if err := r.syncPlanDAO.DeleteByID(tx, id); err != nil {
			return err
		}

		return nil
	})
}

// List retrieves all sync plans (without aggregated entities for performance).
func (r *SyncPlanRepositoryImpl) List() ([]*sync.SyncPlan, error) {
	return r.syncPlanDAO.ListAll(nil)
}

// ==================== SyncTask Operations ====================

// AddTask adds a new SyncTask to a SyncPlan.
func (r *SyncPlanRepositoryImpl) AddTask(task *sync.SyncTask) error {
	return r.syncTaskDAO.Create(nil, task)
}

// GetTask retrieves a SyncTask by ID.
func (r *SyncPlanRepositoryImpl) GetTask(id shared.ID) (*sync.SyncTask, error) {
	return r.syncTaskDAO.GetByID(nil, id)
}

// GetTasksByPlan retrieves all SyncTasks for a SyncPlan.
func (r *SyncPlanRepositoryImpl) GetTasksByPlan(planID shared.ID) ([]*sync.SyncTask, error) {
	return r.syncTaskDAO.GetByPlanID(nil, planID)
}

// UpdateTask updates a SyncTask.
func (r *SyncPlanRepositoryImpl) UpdateTask(task *sync.SyncTask) error {
	return r.syncTaskDAO.Update(nil, task)
}

// DeleteTasksByPlan deletes all SyncTasks for a SyncPlan.
func (r *SyncPlanRepositoryImpl) DeleteTasksByPlan(planID shared.ID) error {
	return r.syncTaskDAO.DeleteByPlanID(nil, planID)
}

// ==================== SyncExecution Operations ====================

// AddPlanExecution adds a new SyncExecution to a SyncPlan.
func (r *SyncPlanRepositoryImpl) AddPlanExecution(exec *sync.SyncExecution) error {
	return r.syncExecutionDAO.Create(nil, exec)
}

// GetPlanExecution retrieves a SyncExecution by ID.
func (r *SyncPlanRepositoryImpl) GetPlanExecution(id shared.ID) (*sync.SyncExecution, error) {
	return r.syncExecutionDAO.GetByID(nil, id)
}

// GetExecutionsByPlan retrieves all SyncExecutions for a SyncPlan.
func (r *SyncPlanRepositoryImpl) GetExecutionsByPlan(planID shared.ID) ([]*sync.SyncExecution, error) {
	return r.syncExecutionDAO.GetByPlanID(nil, planID)
}

// UpdatePlanExecution updates a SyncExecution.
func (r *SyncPlanRepositoryImpl) UpdatePlanExecution(exec *sync.SyncExecution) error {
	return r.syncExecutionDAO.Update(nil, exec)
}

// ==================== Query Operations ====================

// GetByDataSource retrieves sync plans by data source ID.
func (r *SyncPlanRepositoryImpl) GetByDataSource(dataSourceID shared.ID) ([]*sync.SyncPlan, error) {
	return r.syncPlanDAO.GetByDataSource(nil, dataSourceID)
}

// GetEnabledPlans retrieves all enabled sync plans.
func (r *SyncPlanRepositoryImpl) GetEnabledPlans() ([]*sync.SyncPlan, error) {
	return r.syncPlanDAO.GetByStatus(nil, sync.PlanStatusEnabled)
}

// GetByStatus retrieves sync plans by status.
func (r *SyncPlanRepositoryImpl) GetByStatus(status sync.PlanStatus) ([]*sync.SyncPlan, error) {
	return r.syncPlanDAO.GetByStatus(nil, status)
}

// ==================== Extended Query Operations ====================

// FindBy retrieves entities matching the given conditions.
func (r *SyncPlanRepositoryImpl) FindBy(conditions ...shared.QueryCondition) ([]*sync.SyncPlan, error) {
	return r.findByInternal(nil, nil, conditions...)
}

// FindByWithOrder retrieves entities matching conditions with ordering.
func (r *SyncPlanRepositoryImpl) FindByWithOrder(orderBy []shared.OrderBy, conditions ...shared.QueryCondition) ([]*sync.SyncPlan, error) {
	return r.findByInternal(orderBy, nil, conditions...)
}

// ListWithPagination retrieves entities with pagination.
func (r *SyncPlanRepositoryImpl) ListWithPagination(pagination shared.Pagination) (*shared.PageResult[sync.SyncPlan], error) {
	return r.FindByWithPagination(pagination)
}

// FindByWithPagination retrieves entities matching conditions with pagination.
func (r *SyncPlanRepositoryImpl) FindByWithPagination(pagination shared.Pagination, conditions ...shared.QueryCondition) (*shared.PageResult[sync.SyncPlan], error) {
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
func (r *SyncPlanRepositoryImpl) Count(conditions ...shared.QueryCondition) (int64, error) {
	whereClause, args := buildWhereClause(conditions...)
	query := fmt.Sprintf("SELECT COUNT(*) FROM sync_plan%s", whereClause)

	var count int64
	err := r.db.DB.Get(&count, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to count sync_plan: %w", err)
	}
	return count, nil
}

// Exists checks if any entity matching conditions exists.
func (r *SyncPlanRepositoryImpl) Exists(conditions ...shared.QueryCondition) (bool, error) {
	count, err := r.Count(conditions...)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (r *SyncPlanRepositoryImpl) findByInternal(orderBy []shared.OrderBy, pagination *shared.Pagination, conditions ...shared.QueryCondition) ([]*sync.SyncPlan, error) {
	whereClause, args := buildWhereClause(conditions...)
	orderClause := buildOrderClause(orderBy)
	limitClause := buildLimitClause(pagination)

	query := fmt.Sprintf("SELECT * FROM sync_plan%s%s%s", whereClause, orderClause, limitClause)

	var rows []dao.SyncPlanRow
	err := r.db.DB.Select(&rows, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return []*sync.SyncPlan{}, nil
		}
		return nil, fmt.Errorf("failed to find sync_plan: %w", err)
	}

	entities := make([]*sync.SyncPlan, 0, len(rows))
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
func (r *SyncPlanRepositoryImpl) rowToEntity(row *dao.SyncPlanRow) (*sync.SyncPlan, error) {
	entity := &sync.SyncPlan{
		ID:           shared.ID(row.ID),
		Name:         row.Name,
		Description:  row.Description,
		DataSourceID: shared.ID(row.DataSourceID),
		Status:       sync.PlanStatus(row.Status),
		CreatedAt:    shared.Timestamp(row.CreatedAt),
		UpdatedAt:    shared.Timestamp(row.UpdatedAt),
	}

	if row.DataStoreID.Valid {
		entity.DataStoreID = shared.ID(row.DataStoreID.String)
	}

	if row.CronExpression.Valid {
		entity.CronExpression = &row.CronExpression.String
	}

	if row.LastExecutedAt.Valid {
		entity.LastExecutedAt = &row.LastExecutedAt.Time
	}

	if row.NextExecuteAt.Valid {
		entity.NextExecuteAt = &row.NextExecuteAt.Time
	}

	if err := entity.UnmarshalSelectedAPIsJSON(row.SelectedAPIs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal selected apis: %w", err)
	}

	if row.ResolvedAPIs != "" {
		if err := entity.UnmarshalResolvedAPIsJSON(row.ResolvedAPIs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal resolved apis: %w", err)
		}
	}

	if row.ExecutionGraph != "" {
		if err := entity.UnmarshalExecutionGraphJSON(row.ExecutionGraph); err != nil {
			return nil, fmt.Errorf("failed to unmarshal execution graph: %w", err)
		}
	}

	return entity, nil
}
