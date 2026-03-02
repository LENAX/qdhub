package dao

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
)

// SyncPlanDAO provides data access operations for SyncPlan.
type SyncPlanDAO struct {
	*SQLBaseDAO[SyncPlanRow]
}

// NewSyncPlanDAO creates a new SyncPlanDAO.
func NewSyncPlanDAO(db *sqlx.DB) *SyncPlanDAO {
	return &SyncPlanDAO{
		SQLBaseDAO: NewSQLBaseDAO[SyncPlanRow](db, "sync_plan", "id"),
	}
}

// Create inserts a new sync plan record.
func (d *SyncPlanDAO) Create(tx *sqlx.Tx, entity *sync.SyncPlan) error {
	query := `INSERT INTO sync_plan (id, name, description, data_source_id, data_store_id,
		selected_apis, resolved_apis, execution_graph, cron_expression, default_execute_params,
		incremental_mode, last_successful_end_date, incremental_start_date_api, incremental_start_date_column,
		status, last_executed_at, next_execute_at, created_at, updated_at)
		VALUES (:id, :name, :description, :data_source_id, :data_store_id,
		:selected_apis, :resolved_apis, :execution_graph, :cron_expression, :default_execute_params,
		:incremental_mode, :last_successful_end_date, :incremental_start_date_api, :incremental_start_date_column,
		:status, :last_executed_at, :next_execute_at, :created_at, :updated_at)`

	row, err := d.toRow(entity)
	if err != nil {
		return err
	}

	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to create sync plan: %w", err)
	}
	return nil
}

// GetByID retrieves a sync plan by ID.
func (d *SyncPlanDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*sync.SyncPlan, error) {
	row, err := d.Get(tx, id.String())
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return d.toEntity(row)
}

// Update updates an existing sync plan record.
func (d *SyncPlanDAO) Update(tx *sqlx.Tx, entity *sync.SyncPlan) error {
	query := `UPDATE sync_plan SET
		name = :name, description = :description, data_store_id = :data_store_id,
		selected_apis = :selected_apis, resolved_apis = :resolved_apis,
		execution_graph = :execution_graph, cron_expression = :cron_expression,
		default_execute_params = :default_execute_params,
		incremental_mode = :incremental_mode, last_successful_end_date = :last_successful_end_date,
		incremental_start_date_api = :incremental_start_date_api, incremental_start_date_column = :incremental_start_date_column,
		status = :status, last_executed_at = :last_executed_at,
		next_execute_at = :next_execute_at, updated_at = :updated_at
		WHERE id = :id`

	row, err := d.toRow(entity)
	if err != nil {
		return err
	}

	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to update sync plan: %w", err)
	}
	return nil
}

// DeleteByID deletes a sync plan by ID.
func (d *SyncPlanDAO) DeleteByID(tx *sqlx.Tx, id shared.ID) error {
	return d.Delete(tx, id.String())
}

// ListAll retrieves all sync plans.
func (d *SyncPlanDAO) ListAll(tx *sqlx.Tx) ([]*sync.SyncPlan, error) {
	rows, err := d.List(tx)
	if err != nil {
		return nil, err
	}

	entities := make([]*sync.SyncPlan, 0, len(rows))
	for _, row := range rows {
		entity, err := d.toEntity(row)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

// GetByDataSource retrieves sync plans by data source ID.
func (d *SyncPlanDAO) GetByDataSource(tx *sqlx.Tx, dataSourceID shared.ID) ([]*sync.SyncPlan, error) {
	query := `SELECT * FROM sync_plan WHERE data_source_id = ?`
	var rows []SyncPlanRow

	var err error
	if tx != nil {
		err = tx.Select(&rows, query, dataSourceID.String())
	} else {
		err = d.DB().Select(&rows, query, dataSourceID.String())
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get sync plans by data source: %w", err)
	}

	entities := make([]*sync.SyncPlan, 0, len(rows))
	for _, row := range rows {
		entity, err := d.toEntity(&row)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

// GetByDataStore retrieves sync plans by data store ID.
func (d *SyncPlanDAO) GetByDataStore(tx *sqlx.Tx, dataStoreID shared.ID) ([]*sync.SyncPlan, error) {
	query := `SELECT * FROM sync_plan WHERE data_store_id = ?`
	var rows []SyncPlanRow
	var err error
	if tx != nil {
		err = tx.Select(&rows, query, dataStoreID.String())
	} else {
		err = d.DB().Select(&rows, query, dataStoreID.String())
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get sync plans by data store: %w", err)
	}
	entities := make([]*sync.SyncPlan, 0, len(rows))
	for _, row := range rows {
		entity, err := d.toEntity(&row)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

// GetByStatus retrieves sync plans by status.
func (d *SyncPlanDAO) GetByStatus(tx *sqlx.Tx, status sync.PlanStatus) ([]*sync.SyncPlan, error) {
	query := `SELECT * FROM sync_plan WHERE status = ?`
	var rows []SyncPlanRow

	var err error
	if tx != nil {
		err = tx.Select(&rows, query, status.String())
	} else {
		err = d.DB().Select(&rows, query, status.String())
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get sync plans by status: %w", err)
	}

	entities := make([]*sync.SyncPlan, 0, len(rows))
	for _, row := range rows {
		entity, err := d.toEntity(&row)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

// GetSchedulablePlans returns plans that have a cron expression and are not disabled
// (status in draft, resolved, enabled, running). Used to restore cron schedules on startup.
func (d *SyncPlanDAO) GetSchedulablePlans(tx *sqlx.Tx) ([]*sync.SyncPlan, error) {
	query := `SELECT * FROM sync_plan WHERE status != ? AND cron_expression IS NOT NULL AND cron_expression != ''`
	var rows []SyncPlanRow
	var err error
	if tx != nil {
		err = tx.Select(&rows, query, sync.PlanStatusDisabled.String())
	} else {
		err = d.DB().Select(&rows, query, sync.PlanStatusDisabled.String())
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get schedulable plans: %w", err)
	}
	entities := make([]*sync.SyncPlan, 0, len(rows))
	for _, row := range rows {
		entity, err := d.toEntity(&row)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

// toRow converts domain entity to database row.
func (d *SyncPlanDAO) toRow(entity *sync.SyncPlan) (*SyncPlanRow, error) {
	selectedAPIs, err := entity.MarshalSelectedAPIsJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal selected apis: %w", err)
	}

	resolvedAPIs, err := entity.MarshalResolvedAPIsJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal resolved apis: %w", err)
	}

	executionGraph, err := entity.MarshalExecutionGraphJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal execution graph: %w", err)
	}

	defaultExecuteParams, err := entity.MarshalDefaultExecuteParamsJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal default execute params: %w", err)
	}

	row := &SyncPlanRow{
		ID:                     entity.ID.String(),
		Name:                   entity.Name,
		Description:            entity.Description,
		DataSourceID:           entity.DataSourceID.String(),
		SelectedAPIs:           selectedAPIs,
		ResolvedAPIs:           resolvedAPIs,
		ExecutionGraph:         executionGraph,
		DefaultExecuteParams:   defaultExecuteParams,
		IncrementalMode:        entity.IncrementalMode,
		Status:                 entity.Status.String(),
		CreatedAt:              entity.CreatedAt.ToTime(),
		UpdatedAt:              entity.UpdatedAt.ToTime(),
	}
	if entity.LastSuccessfulEndDate != nil {
		row.LastSuccessfulEndDate = sql.NullString{String: *entity.LastSuccessfulEndDate, Valid: true}
	}

	if entity.DataStoreID != "" {
		row.DataStoreID = sql.NullString{String: entity.DataStoreID.String(), Valid: true}
	}

	if entity.CronExpression != nil {
		row.CronExpression = sql.NullString{String: *entity.CronExpression, Valid: true}
	}

	if entity.LastExecutedAt != nil {
		row.LastExecutedAt = sql.NullTime{Time: *entity.LastExecutedAt, Valid: true}
	}

	if entity.NextExecuteAt != nil {
		row.NextExecuteAt = sql.NullTime{Time: *entity.NextExecuteAt, Valid: true}
	}

	if entity.IncrementalStartDateAPI != nil {
		row.IncrementalStartDateAPI = sql.NullString{String: *entity.IncrementalStartDateAPI, Valid: true}
	}
	if entity.IncrementalStartDateColumn != nil {
		row.IncrementalStartDateColumn = sql.NullString{String: *entity.IncrementalStartDateColumn, Valid: true}
	}

	return row, nil
}

// toEntity converts database row to domain entity.
func (d *SyncPlanDAO) toEntity(row *SyncPlanRow) (*sync.SyncPlan, error) {
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

	if row.DefaultExecuteParams != "" {
		if err := entity.UnmarshalDefaultExecuteParamsJSON(row.DefaultExecuteParams); err != nil {
			return nil, fmt.Errorf("failed to unmarshal default execute params: %w", err)
		}
	}

	entity.IncrementalMode = row.IncrementalMode
	if row.LastSuccessfulEndDate.Valid {
		entity.LastSuccessfulEndDate = &row.LastSuccessfulEndDate.String
	}
	if row.IncrementalStartDateAPI.Valid {
		entity.IncrementalStartDateAPI = &row.IncrementalStartDateAPI.String
	}
	if row.IncrementalStartDateColumn.Valid {
		entity.IncrementalStartDateColumn = &row.IncrementalStartDateColumn.String
	}

	return entity, nil
}

// SyncTaskDAO provides data access operations for SyncTask.
type SyncTaskDAO struct {
	*SQLBaseDAO[SyncTaskRow]
}

// NewSyncTaskDAO creates a new SyncTaskDAO.
func NewSyncTaskDAO(db *sqlx.DB) *SyncTaskDAO {
	return &SyncTaskDAO{
		SQLBaseDAO: NewSQLBaseDAO[SyncTaskRow](db, "sync_task", "id"),
	}
}

// Create inserts a new sync task record.
func (d *SyncTaskDAO) Create(tx *sqlx.Tx, entity *sync.SyncTask) error {
	query := `INSERT INTO sync_task (id, sync_plan_id, api_name, sync_mode, params,
		param_mappings, dependencies, level, sort_order, sync_frequency, last_synced_at, created_at)
		VALUES (:id, :sync_plan_id, :api_name, :sync_mode, :params,
		:param_mappings, :dependencies, :level, :sort_order, :sync_frequency, :last_synced_at, :created_at)`

	row, err := d.toRow(entity)
	if err != nil {
		return err
	}

	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to create sync task: %w", err)
	}
	return nil
}

// GetByID retrieves a sync task by ID.
func (d *SyncTaskDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*sync.SyncTask, error) {
	row, err := d.Get(tx, id.String())
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return d.toEntity(row)
}

// Update updates an existing sync task record.
func (d *SyncTaskDAO) Update(tx *sqlx.Tx, entity *sync.SyncTask) error {
	query := `UPDATE sync_task SET
		sync_mode = :sync_mode, params = :params, param_mappings = :param_mappings,
		dependencies = :dependencies, level = :level, sort_order = :sort_order,
		sync_frequency = :sync_frequency, last_synced_at = :last_synced_at
		WHERE id = :id`

	row, err := d.toRow(entity)
	if err != nil {
		return err
	}

	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to update sync task: %w", err)
	}
	return nil
}

// GetByPlanID retrieves all sync tasks for a plan.
func (d *SyncTaskDAO) GetByPlanID(tx *sqlx.Tx, planID shared.ID) ([]*sync.SyncTask, error) {
	query := `SELECT * FROM sync_task WHERE sync_plan_id = ? ORDER BY level, sort_order`
	var rows []SyncTaskRow

	var err error
	if tx != nil {
		err = tx.Select(&rows, query, planID.String())
	} else {
		err = d.DB().Select(&rows, query, planID.String())
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get sync tasks by plan: %w", err)
	}

	entities := make([]*sync.SyncTask, 0, len(rows))
	for _, row := range rows {
		entity, err := d.toEntity(&row)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

// DeleteByPlanID deletes all sync tasks for a plan.
func (d *SyncTaskDAO) DeleteByPlanID(tx *sqlx.Tx, planID shared.ID) error {
	query := `DELETE FROM sync_task WHERE sync_plan_id = ?`

	var err error
	if tx != nil {
		_, err = tx.Exec(query, planID.String())
	} else {
		_, err = d.DB().Exec(query, planID.String())
	}

	if err != nil {
		return fmt.Errorf("failed to delete sync tasks by plan: %w", err)
	}
	return nil
}

// toRow converts domain entity to database row.
func (d *SyncTaskDAO) toRow(entity *sync.SyncTask) (*SyncTaskRow, error) {
	params, err := entity.MarshalParamsJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal params: %w", err)
	}

	paramMappings, err := entity.MarshalParamMappingsJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal param mappings: %w", err)
	}

	dependencies, err := entity.MarshalDependenciesJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal dependencies: %w", err)
	}

	row := &SyncTaskRow{
		ID:            entity.ID.String(),
		SyncPlanID:    entity.SyncPlanID.String(),
		APIName:       entity.APIName,
		SyncMode:      entity.SyncMode.String(),
		Params:        params,
		ParamMappings: paramMappings,
		Dependencies:  dependencies,
		Level:         entity.Level,
		SortOrder:     entity.SortOrder,
		SyncFrequency: int64(entity.SyncFrequency),
		CreatedAt:     entity.CreatedAt.ToTime(),
	}

	if entity.LastSyncedAt != nil {
		row.LastSyncedAt = sql.NullTime{Time: *entity.LastSyncedAt, Valid: true}
	}

	return row, nil
}

// toEntity converts database row to domain entity.
func (d *SyncTaskDAO) toEntity(row *SyncTaskRow) (*sync.SyncTask, error) {
	entity := &sync.SyncTask{
		ID:            shared.ID(row.ID),
		SyncPlanID:    shared.ID(row.SyncPlanID),
		APIName:       row.APIName,
		SyncMode:      sync.TaskSyncMode(row.SyncMode),
		Level:         row.Level,
		SortOrder:     row.SortOrder,
		SyncFrequency: time.Duration(row.SyncFrequency),
		CreatedAt:     shared.Timestamp(row.CreatedAt),
	}

	if row.LastSyncedAt.Valid {
		entity.LastSyncedAt = &row.LastSyncedAt.Time
	}

	if err := entity.UnmarshalParamsJSON(row.Params); err != nil {
		return nil, fmt.Errorf("failed to unmarshal params: %w", err)
	}

	if err := entity.UnmarshalParamMappingsJSON(row.ParamMappings); err != nil {
		return nil, fmt.Errorf("failed to unmarshal param mappings: %w", err)
	}

	if err := entity.UnmarshalDependenciesJSON(row.Dependencies); err != nil {
		return nil, fmt.Errorf("failed to unmarshal dependencies: %w", err)
	}

	return entity, nil
}

// SyncExecutionDAO provides data access operations for SyncExecution (for SyncPlan).
type SyncExecutionDAO struct {
	*SQLBaseDAO[SyncExecutionRow]
}

// NewSyncExecutionDAO creates a new SyncExecutionDAO.
func NewSyncExecutionDAO(db *sqlx.DB) *SyncExecutionDAO {
	return &SyncExecutionDAO{
		SQLBaseDAO: NewSQLBaseDAO[SyncExecutionRow](db, "sync_execution", "id"),
	}
}

// Create inserts a new sync execution record.
func (d *SyncExecutionDAO) Create(tx *sqlx.Tx, entity *sync.SyncExecution) error {
	query := `INSERT INTO sync_execution (id, sync_plan_id, workflow_inst_id, status,
		started_at, finished_at, record_count, error_message, workflow_error_message, execute_params, synced_apis, skipped_apis)
		VALUES (:id, :sync_plan_id, :workflow_inst_id, :status,
		:started_at, :finished_at, :record_count, :error_message, :workflow_error_message, :execute_params, :synced_apis, :skipped_apis)`

	row, err := d.toRow(entity)
	if err != nil {
		return err
	}

	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to create sync execution: %w", err)
	}
	return nil
}

// GetByID retrieves a sync execution by ID.
func (d *SyncExecutionDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*sync.SyncExecution, error) {
	row, err := d.Get(tx, id.String())
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return d.toEntity(row)
}

// Update updates an existing sync execution record.
func (d *SyncExecutionDAO) Update(tx *sqlx.Tx, entity *sync.SyncExecution) error {
	query := `UPDATE sync_execution SET
		status = :status, finished_at = :finished_at, record_count = :record_count,
		error_message = :error_message, workflow_error_message = :workflow_error_message
		WHERE id = :id`

	row, err := d.toRow(entity)
	if err != nil {
		return err
	}

	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to update sync execution: %w", err)
	}
	return nil
}

// GetByPlanID retrieves all sync executions for a plan.
func (d *SyncExecutionDAO) GetByPlanID(tx *sqlx.Tx, planID shared.ID) ([]*sync.SyncExecution, error) {
	query := `SELECT * FROM sync_execution WHERE sync_plan_id = ? ORDER BY started_at DESC`
	var rows []SyncExecutionRow

	var err error
	if tx != nil {
		err = tx.Select(&rows, query, planID.String())
	} else {
		err = d.DB().Select(&rows, query, planID.String())
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get sync executions by plan: %w", err)
	}

	entities := make([]*sync.SyncExecution, 0, len(rows))
	for _, row := range rows {
		entity, err := d.toEntity(&row)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

// GetByWorkflowInstID retrieves a sync execution by workflow instance ID.
func (d *SyncExecutionDAO) GetByWorkflowInstID(tx *sqlx.Tx, workflowInstID string) (*sync.SyncExecution, error) {
	query := `SELECT * FROM sync_execution WHERE workflow_inst_id = ? LIMIT 1`
	var row SyncExecutionRow

	var err error
	if tx != nil {
		err = tx.Get(&row, query, workflowInstID)
	} else {
		err = d.DB().Get(&row, query, workflowInstID)
	}

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get sync execution by workflow_inst_id: %w", err)
	}
	return d.toEntity(&row)
}

// GetByPlanIDPaged retrieves sync executions for a plan with limit and offset, ordered by started_at DESC.
func (d *SyncExecutionDAO) GetByPlanIDPaged(tx *sqlx.Tx, planID shared.ID, limit, offset int) ([]*sync.SyncExecution, error) {
	query := `SELECT * FROM sync_execution WHERE sync_plan_id = ? ORDER BY started_at DESC LIMIT ? OFFSET ?`
	var rows []SyncExecutionRow

	var err error
	if tx != nil {
		err = tx.Select(&rows, query, planID.String(), limit, offset)
	} else {
		err = d.DB().Select(&rows, query, planID.String(), limit, offset)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get sync executions by plan paged: %w", err)
	}

	entities := make([]*sync.SyncExecution, 0, len(rows))
	for _, row := range rows {
		entity, err := d.toEntity(&row)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

// CountByPlanID returns the total number of sync executions for a plan.
func (d *SyncExecutionDAO) CountByPlanID(tx *sqlx.Tx, planID shared.ID) (int, error) {
	query := `SELECT COUNT(*) FROM sync_execution WHERE sync_plan_id = ?`
	var count int

	var err error
	if tx != nil {
		err = tx.Get(&count, query, planID.String())
	} else {
		err = d.DB().Get(&count, query, planID.String())
	}

	if err != nil {
		return 0, fmt.Errorf("failed to count sync executions by plan: %w", err)
	}
	return count, nil
}

// toRow converts domain entity to database row.
func (d *SyncExecutionDAO) toRow(entity *sync.SyncExecution) (*SyncExecutionRow, error) {
	executeParams, err := entity.MarshalExecuteParamsJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal execute params: %w", err)
	}

	syncedAPIs, err := entity.MarshalSyncedAPIsJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal synced apis: %w", err)
	}

	skippedAPIs, err := entity.MarshalSkippedAPIsJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal skipped apis: %w", err)
	}

	row := &SyncExecutionRow{
		ID:             entity.ID.String(),
		SyncPlanID:     entity.SyncPlanID.String(),
		WorkflowInstID: entity.WorkflowInstID.String(),
		Status:         entity.Status.String(),
		StartedAt:      entity.StartedAt.ToTime(),
		RecordCount:    entity.RecordCount,
		ExecuteParams:  executeParams,
		SyncedAPIs:     syncedAPIs,
		SkippedAPIs:    skippedAPIs,
	}

	if entity.FinishedAt != nil {
		row.FinishedAt = sql.NullTime{Time: entity.FinishedAt.ToTime(), Valid: true}
	}

	if entity.ErrorMessage != nil {
		row.ErrorMessage = sql.NullString{String: *entity.ErrorMessage, Valid: true}
	}
	if entity.WorkflowErrorMessage != nil {
		row.WorkflowErrorMessage = sql.NullString{String: *entity.WorkflowErrorMessage, Valid: true}
	}

	return row, nil
}

// toEntity converts database row to domain entity.
func (d *SyncExecutionDAO) toEntity(row *SyncExecutionRow) (*sync.SyncExecution, error) {
	entity := &sync.SyncExecution{
		ID:             shared.ID(row.ID),
		SyncPlanID:     shared.ID(row.SyncPlanID),
		WorkflowInstID: shared.ID(row.WorkflowInstID),
		Status:         sync.ExecStatus(row.Status),
		StartedAt:      shared.Timestamp(row.StartedAt),
		RecordCount:    row.RecordCount,
	}

	if row.FinishedAt.Valid {
		ts := shared.Timestamp(row.FinishedAt.Time)
		entity.FinishedAt = &ts
	}

	if row.ErrorMessage.Valid {
		entity.ErrorMessage = &row.ErrorMessage.String
	}
	if row.WorkflowErrorMessage.Valid {
		entity.WorkflowErrorMessage = &row.WorkflowErrorMessage.String
	}

	if err := entity.UnmarshalExecuteParamsJSON(row.ExecuteParams); err != nil {
		return nil, fmt.Errorf("failed to unmarshal execute params: %w", err)
	}

	if err := entity.UnmarshalSyncedAPIsJSON(row.SyncedAPIs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal synced apis: %w", err)
	}

	if err := entity.UnmarshalSkippedAPIsJSON(row.SkippedAPIs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal skipped apis: %w", err)
	}

	return entity, nil
}

// ==================== SyncExecutionDetail DAO ====================

// SyncExecutionDetailDAO provides data access for sync_execution_detail.
type SyncExecutionDetailDAO struct {
	*SQLBaseDAO[SyncExecutionDetailRow]
}

// NewSyncExecutionDetailDAO creates a new SyncExecutionDetailDAO.
func NewSyncExecutionDetailDAO(db *sqlx.DB) *SyncExecutionDetailDAO {
	return &SyncExecutionDetailDAO{
		SQLBaseDAO: NewSQLBaseDAO[SyncExecutionDetailRow](db, "sync_execution_detail", "id"),
	}
}

// Create inserts a sync execution detail record.
func (d *SyncExecutionDetailDAO) Create(tx *sqlx.Tx, entity *sync.SyncExecutionDetail) error {
	query := `INSERT INTO sync_execution_detail (id, execution_id, task_id, api_name, record_count, status, error_message, started_at, finished_at)
		VALUES (:id, :execution_id, :task_id, :api_name, :record_count, :status, :error_message, :started_at, :finished_at)`
	row := d.detailToRow(entity)
	var err error
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}
	if err != nil {
		return fmt.Errorf("failed to create sync execution detail: %w", err)
	}
	return nil
}

// GetByExecutionID returns all details for an execution.
func (d *SyncExecutionDetailDAO) GetByExecutionID(tx *sqlx.Tx, executionID shared.ID) ([]*sync.SyncExecutionDetail, error) {
	query := `SELECT * FROM sync_execution_detail WHERE execution_id = ? ORDER BY created_at ASC`
	var rows []SyncExecutionDetailRow
	var err error
	if tx != nil {
		err = tx.Select(&rows, query, executionID.String())
	} else {
		err = d.DB().Select(&rows, query, executionID.String())
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get execution details: %w", err)
	}
	out := make([]*sync.SyncExecutionDetail, 0, len(rows))
	for i := range rows {
		out = append(out, d.detailRowToEntity(&rows[i]))
	}
	return out, nil
}

func (d *SyncExecutionDetailDAO) detailToRow(entity *sync.SyncExecutionDetail) *SyncExecutionDetailRow {
	row := &SyncExecutionDetailRow{
		ID:          entity.ID.String(),
		ExecutionID: entity.ExecutionID.String(),
		TaskID:      entity.TaskID,
		APIName:     entity.APIName,
		RecordCount: entity.RecordCount,
		Status:      entity.Status,
	}
	if entity.ErrorMessage != nil {
		row.ErrorMessage = sql.NullString{String: *entity.ErrorMessage, Valid: true}
	}
	if entity.StartedAt != nil {
		row.StartedAt = sql.NullTime{Time: entity.StartedAt.ToTime(), Valid: true}
	}
	if entity.FinishedAt != nil {
		row.FinishedAt = sql.NullTime{Time: entity.FinishedAt.ToTime(), Valid: true}
	}
	return row
}

func (d *SyncExecutionDetailDAO) detailRowToEntity(row *SyncExecutionDetailRow) *sync.SyncExecutionDetail {
	entity := &sync.SyncExecutionDetail{
		ID:          shared.ID(row.ID),
		ExecutionID: shared.ID(row.ExecutionID),
		TaskID:      row.TaskID,
		APIName:     row.APIName,
		RecordCount: row.RecordCount,
		Status:      row.Status,
	}
	if row.ErrorMessage.Valid {
		entity.ErrorMessage = &row.ErrorMessage.String
	}
	if row.StartedAt.Valid {
		ts := shared.Timestamp(row.StartedAt.Time)
		entity.StartedAt = &ts
	}
	if row.FinishedAt.Valid {
		ts := shared.Timestamp(row.FinishedAt.Time)
		entity.FinishedAt = &ts
	}
	return entity
}
