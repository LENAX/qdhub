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
	tx               *sqlx.Tx // External transaction (nil if not in transaction)
	syncPlanDAO      *dao.SyncPlanDAO
	syncTaskDAO      *dao.SyncTaskDAO
	syncExecutionDAO *dao.SyncExecutionDAO
}

// NewSyncPlanRepository creates a new SyncPlanRepositoryImpl.
func NewSyncPlanRepository(db *persistence.DB) *SyncPlanRepositoryImpl {
	return &SyncPlanRepositoryImpl{
		db:               db,
		tx:               nil,
		syncPlanDAO:      dao.NewSyncPlanDAO(db.DB),
		syncTaskDAO:      dao.NewSyncTaskDAO(db.DB),
		syncExecutionDAO: dao.NewSyncExecutionDAO(db.DB),
	}
}

// NewSyncPlanRepositoryWithTx creates a new SyncPlanRepositoryImpl bound to an external transaction.
// All operations will use the provided transaction instead of creating new ones.
func NewSyncPlanRepositoryWithTx(db *persistence.DB, tx *sqlx.Tx) *SyncPlanRepositoryImpl {
	return &SyncPlanRepositoryImpl{
		db:               db,
		tx:               tx,
		syncPlanDAO:      dao.NewSyncPlanDAO(db.DB),
		syncTaskDAO:      dao.NewSyncTaskDAO(db.DB),
		syncExecutionDAO: dao.NewSyncExecutionDAO(db.DB),
	}
}

// Create creates a new sync plan with its aggregated entities.
func (r *SyncPlanRepositoryImpl) Create(plan *sync.SyncPlan) error {
	// If we have an external transaction, use it directly
	if r.tx != nil {
		return r.createInTx(r.tx, plan)
	}
	// Otherwise, create a new transaction
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		return r.createInTx(tx, plan)
	})
}

// createInTx is the internal implementation that performs the actual create operation
func (r *SyncPlanRepositoryImpl) createInTx(tx *sqlx.Tx, plan *sync.SyncPlan) error {
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
}

// Get retrieves a sync plan by ID with its aggregated entities.
func (r *SyncPlanRepositoryImpl) Get(id shared.ID) (*sync.SyncPlan, error) {
	// Use external transaction if available for read consistency
	plan, err := r.syncPlanDAO.GetByID(r.tx, id)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, nil
	}

	// Load tasks
	tasks, err := r.syncTaskDAO.GetByPlanID(r.tx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to load tasks: %w", err)
	}
	plan.Tasks = tasks

	// Load executions (lazy load, only recent ones)
	executions, err := r.syncExecutionDAO.GetByPlanID(r.tx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to load executions: %w", err)
	}
	plan.Executions = executions

	return plan, nil
}

// Update updates a sync plan.
func (r *SyncPlanRepositoryImpl) Update(plan *sync.SyncPlan) error {
	// Use external transaction if available, otherwise nil (no transaction needed for single update)
	return r.syncPlanDAO.Update(r.tx, plan)
}

// Delete deletes a sync plan and its aggregated entities.
func (r *SyncPlanRepositoryImpl) Delete(id shared.ID) error {
	// If we have an external transaction, use it directly
	if r.tx != nil {
		return r.deleteInTx(r.tx, id)
	}
	// Otherwise, create a new transaction
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		return r.deleteInTx(tx, id)
	})
}

// deleteInTx is the internal implementation that performs the actual delete operation
func (r *SyncPlanRepositoryImpl) deleteInTx(tx *sqlx.Tx, id shared.ID) error {
	// Delete tasks first
	if err := r.syncTaskDAO.DeleteByPlanID(tx, id); err != nil {
		return err
	}

	// Delete sync plan
	if err := r.syncPlanDAO.DeleteByID(tx, id); err != nil {
		return err
	}

	return nil
}

// List retrieves all sync plans (without aggregated entities for performance).
func (r *SyncPlanRepositoryImpl) List() ([]*sync.SyncPlan, error) {
	// Use external transaction if available for read consistency
	return r.syncPlanDAO.ListAll(r.tx)
}

// ==================== SyncTask Operations ====================

// AddTask adds a new SyncTask to a SyncPlan.
func (r *SyncPlanRepositoryImpl) AddTask(task *sync.SyncTask) error {
	// Use external transaction if available, otherwise nil
	return r.syncTaskDAO.Create(r.tx, task)
}

// GetTask retrieves a SyncTask by ID.
func (r *SyncPlanRepositoryImpl) GetTask(id shared.ID) (*sync.SyncTask, error) {
	// Use external transaction if available for read consistency
	return r.syncTaskDAO.GetByID(r.tx, id)
}

// GetTasksByPlan retrieves all SyncTasks for a SyncPlan.
func (r *SyncPlanRepositoryImpl) GetTasksByPlan(planID shared.ID) ([]*sync.SyncTask, error) {
	// Use external transaction if available for read consistency
	return r.syncTaskDAO.GetByPlanID(r.tx, planID)
}

// UpdateTask updates a SyncTask.
func (r *SyncPlanRepositoryImpl) UpdateTask(task *sync.SyncTask) error {
	// Use external transaction if available, otherwise nil
	return r.syncTaskDAO.Update(r.tx, task)
}

// DeleteTasksByPlan deletes all SyncTasks for a SyncPlan.
func (r *SyncPlanRepositoryImpl) DeleteTasksByPlan(planID shared.ID) error {
	// Use external transaction if available, otherwise nil
	return r.syncTaskDAO.DeleteByPlanID(r.tx, planID)
}

// ==================== SyncExecution Operations ====================

// AddPlanExecution adds a new SyncExecution to a SyncPlan.
func (r *SyncPlanRepositoryImpl) AddPlanExecution(exec *sync.SyncExecution) error {
	// Use external transaction if available, otherwise nil
	return r.syncExecutionDAO.Create(r.tx, exec)
}

// GetPlanExecution retrieves a SyncExecution by ID.
func (r *SyncPlanRepositoryImpl) GetPlanExecution(id shared.ID) (*sync.SyncExecution, error) {
	// Use external transaction if available for read consistency
	return r.syncExecutionDAO.GetByID(r.tx, id)
}

// GetExecutionsByPlan retrieves all SyncExecutions for a SyncPlan.
func (r *SyncPlanRepositoryImpl) GetExecutionsByPlan(planID shared.ID) ([]*sync.SyncExecution, error) {
	// Use external transaction if available for read consistency
	return r.syncExecutionDAO.GetByPlanID(r.tx, planID)
}

// UpdatePlanExecution updates a SyncExecution.
func (r *SyncPlanRepositoryImpl) UpdatePlanExecution(exec *sync.SyncExecution) error {
	// Use external transaction if available, otherwise nil
	return r.syncExecutionDAO.Update(r.tx, exec)
}

// ==================== Query Operations ====================

// GetByDataSource retrieves sync plans by data source ID.
func (r *SyncPlanRepositoryImpl) GetByDataSource(dataSourceID shared.ID) ([]*sync.SyncPlan, error) {
	// Use external transaction if available for read consistency
	return r.syncPlanDAO.GetByDataSource(r.tx, dataSourceID)
}

// GetEnabledPlans retrieves all enabled sync plans.
func (r *SyncPlanRepositoryImpl) GetEnabledPlans() ([]*sync.SyncPlan, error) {
	// Use external transaction if available for read consistency
	return r.syncPlanDAO.GetByStatus(r.tx, sync.PlanStatusEnabled)
}

// GetByStatus retrieves sync plans by status.
func (r *SyncPlanRepositoryImpl) GetByStatus(status sync.PlanStatus) ([]*sync.SyncPlan, error) {
	// Use external transaction if available for read consistency
	return r.syncPlanDAO.GetByStatus(r.tx, status)
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
