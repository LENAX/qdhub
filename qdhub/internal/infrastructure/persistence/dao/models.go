// Package dao provides data access object implementations.
package dao

import (
	"database/sql"
	"time"
)

// ==================== Metadata Domain Models ====================

// DataSourceRow represents data_sources table row.
type DataSourceRow struct {
	ID             string         `db:"id"`
	Name           string         `db:"name"`
	Description    string         `db:"description"`
	BaseURL        string         `db:"base_url"`
	DocURL         string         `db:"doc_url"`
	Status         string         `db:"status"`
	CommonDataAPIs sql.NullString `db:"common_data_apis"` // JSON array of API names, NULL when empty
	CreatedAt      time.Time      `db:"created_at"`
	UpdatedAt      time.Time      `db:"updated_at"`
}

// APICategoryRow represents api_categories table row.
type APICategoryRow struct {
	ID           string         `db:"id"`
	DataSourceID string         `db:"data_source_id"`
	Name         string         `db:"name"`
	Description  string         `db:"description"`
	ParentID     sql.NullString `db:"parent_id"`
	SortOrder    int            `db:"sort_order"`
	DocPath      string         `db:"doc_path"`
	CreatedAt    time.Time      `db:"created_at"`
}

// APIMetadataRow represents api_metadata table row.
type APIMetadataRow struct {
	ID                string         `db:"id"`
	DataSourceID      string         `db:"data_source_id"`
	CategoryID        sql.NullString `db:"category_id"`
	Name              string         `db:"name"`
	DisplayName       string         `db:"display_name"`
	Description       string         `db:"description"`
	Endpoint          string         `db:"endpoint"`
	RequestParams     string         `db:"request_params"`
	ResponseFields    string         `db:"response_fields"`
	RateLimit         string         `db:"rate_limit"`
	Permission        string         `db:"permission"`
	ParamDependencies sql.NullString `db:"param_dependencies"` // JSON: ParamDependency list
	Status            string         `db:"status"`
	CreatedAt         time.Time      `db:"created_at"`
	UpdatedAt         time.Time      `db:"updated_at"`
}

// TokenRow represents tokens table row.
type TokenRow struct {
	ID           string       `db:"id"`
	DataSourceID string       `db:"data_source_id"`
	TokenValue   string       `db:"token_value"`
	ExpiresAt    sql.NullTime `db:"expires_at"`
	CreatedAt    time.Time    `db:"created_at"`
}

// ==================== Datastore Domain Models ====================

// QuantDataStoreRow represents quant_data_stores table row.
type QuantDataStoreRow struct {
	ID          string    `db:"id"`
	Name        string    `db:"name"`
	Description string    `db:"description"`
	Type        string    `db:"type"`
	DSN         string    `db:"dsn"`
	StoragePath string    `db:"storage_path"`
	Status      string    `db:"status"`
	CreatedAt   time.Time `db:"created_at"`
	UpdatedAt   time.Time `db:"updated_at"`
}

// TableSchemaRow represents table_schemas table row.
type TableSchemaRow struct {
	ID            string         `db:"id"`
	DataStoreID   string         `db:"data_store_id"`
	APIMetadataID string         `db:"api_meta_id"`
	TableName     string         `db:"table_name"`
	Columns       string         `db:"columns"`
	PrimaryKeys   string         `db:"primary_keys"`
	Indexes       string         `db:"indexes"`
	Status        string         `db:"status"`
	ErrorMessage  sql.NullString `db:"error_message"`
	CreatedAt     time.Time      `db:"created_at"`
	UpdatedAt     time.Time      `db:"updated_at"`
}

// DataTypeMappingRuleRow represents data_type_mapping_rules table row.
type DataTypeMappingRuleRow struct {
	ID             string         `db:"id"`
	DataSourceType string         `db:"data_source_type"`
	SourceType     string         `db:"source_type"`
	TargetDBType   string         `db:"target_db_type"`
	TargetType     string         `db:"target_type"`
	FieldPattern   sql.NullString `db:"field_pattern"`
	Priority       int            `db:"priority"`
	IsDefault      bool           `db:"is_default"`
	CreatedAt      time.Time      `db:"created_at"`
	UpdatedAt      time.Time      `db:"updated_at"`
}

// ==================== API Sync Strategy Models ====================

// APISyncStrategyRow represents api_sync_strategies table row.
type APISyncStrategyRow struct {
	ID                      string         `db:"id"`
	DataSourceID            string         `db:"data_source_id"`
	APIName                 string         `db:"api_name"`
	PreferredParam          string         `db:"preferred_param"`
	SupportDateRange        int            `db:"support_date_range"` // 0: false, 1: true
	RequiredParams          sql.NullString `db:"required_params"`    // JSON array
	Dependencies            sql.NullString `db:"dependencies"`       // JSON array
	FixedParams             sql.NullString `db:"fixed_params"`       // JSON object
	FixedParamKeys          sql.NullString `db:"fixed_param_keys"`   // JSON array
	RealtimeTsCodeChunkSize int            `db:"realtime_ts_code_chunk_size"`
	RealtimeTsCodeFormat    string         `db:"realtime_ts_code_format"`
	IterateParams           sql.NullString `db:"iterate_params"` // JSON object map[string][]string
	Description             sql.NullString `db:"description"`
	CreatedAt               time.Time      `db:"created_at"`
	UpdatedAt               time.Time      `db:"updated_at"`
}

// ==================== Sync Domain Models ====================

// SyncExecutionRow represents sync_execution table row.
type SyncExecutionRow struct {
	ID                   string         `db:"id"`
	SyncPlanID           string         `db:"sync_plan_id"`
	WorkflowInstID       string         `db:"workflow_inst_id"`
	Status               string         `db:"status"`
	StartedAt            time.Time      `db:"started_at"`
	FinishedAt           sql.NullTime   `db:"finished_at"`
	RecordCount          int64          `db:"record_count"`
	ErrorMessage         sql.NullString `db:"error_message"`
	WorkflowErrorMessage sql.NullString `db:"workflow_error_message"`
	ExecuteParams        string         `db:"execute_params"`
	SyncedAPIs           string         `db:"synced_apis"`
	SkippedAPIs          string         `db:"skipped_apis"`
}

// SyncPlanRow represents sync_plan table row.
type SyncPlanRow struct {
	ID                         string         `db:"id"`
	Name                       string         `db:"name"`
	Description                string         `db:"description"`
	DataSourceID               string         `db:"data_source_id"`
	DataStoreID                sql.NullString `db:"data_store_id"`
	PlanMode                   string         `db:"plan_mode"`
	SelectedAPIs               string         `db:"selected_apis"`
	ResolvedAPIs               sql.NullString `db:"resolved_apis"`
	ExecutionGraph             sql.NullString `db:"execution_graph"`
	CronExpression             sql.NullString `db:"cron_expression"`
	ScheduleStartCron          sql.NullString `db:"schedule_start_cron"`
	ScheduleEndCron            sql.NullString `db:"schedule_end_cron"`
	SchedulePauseStartCron     sql.NullString `db:"schedule_pause_start_cron"`
	SchedulePauseEndCron       sql.NullString `db:"schedule_pause_end_cron"`
	PullIntervalSeconds        int            `db:"pull_interval_seconds"`
	DefaultExecuteParams       sql.NullString `db:"default_execute_params"` // JSON: ExecuteParams
	IncrementalMode            bool           `db:"incremental_mode"`
	LastSuccessfulEndDate      sql.NullString `db:"last_successful_end_date"`
	IncrementalStartDateAPI    sql.NullString `db:"incremental_start_date_api"`
	IncrementalStartDateColumn sql.NullString `db:"incremental_start_date_column"`
	Status                     string         `db:"status"`
	LastExecutedAt             sql.NullTime   `db:"last_executed_at"`
	NextExecuteAt              sql.NullTime   `db:"next_execute_at"`
	CreatedAt                  time.Time      `db:"created_at"`
	UpdatedAt                  time.Time      `db:"updated_at"`
}

// SyncExecutionDetailRow represents sync_execution_detail table row.
type SyncExecutionDetailRow struct {
	ID           string         `db:"id"`
	ExecutionID  string         `db:"execution_id"`
	TaskID       string         `db:"task_id"`
	APIName      string         `db:"api_name"`
	RecordCount  int64          `db:"record_count"`
	Status       string         `db:"status"`
	ErrorMessage sql.NullString `db:"error_message"`
	StartedAt    sql.NullTime   `db:"started_at"`
	FinishedAt   sql.NullTime   `db:"finished_at"`
	CreatedAt    time.Time      `db:"created_at"`
}

// SyncTaskRow represents sync_task table row.
// Params, ParamMappings, Dependencies may be NULL in DB (e.g. migration 026).
type SyncTaskRow struct {
	ID            string         `db:"id"`
	SyncPlanID    string         `db:"sync_plan_id"`
	APIName       string         `db:"api_name"`
	SyncMode      string         `db:"sync_mode"`
	Params        sql.NullString `db:"params"`
	ParamMappings sql.NullString `db:"param_mappings"`
	Dependencies  sql.NullString `db:"dependencies"`
	Level         int            `db:"level"`
	SortOrder     int            `db:"sort_order"`
	SyncFrequency int64          `db:"sync_frequency"`
	LastSyncedAt  sql.NullTime   `db:"last_synced_at"`
	CreatedAt     time.Time      `db:"created_at"`
}

// ==================== Workflow Domain Models ====================

// WorkflowDefinitionRow represents workflow_definitions table row.
type WorkflowDefinitionRow struct {
	ID             string    `db:"id"`
	Name           string    `db:"name"`
	Description    string    `db:"description"`
	Category       string    `db:"category"`
	DefinitionYAML string    `db:"definition_yaml"`
	Version        int       `db:"version"`
	Status         string    `db:"status"`
	IsSystem       bool      `db:"is_system"`
	CreatedAt      time.Time `db:"created_at"`
	UpdatedAt      time.Time `db:"updated_at"`
}

// WorkflowInstanceRow represents workflow_instances table row.
type WorkflowInstanceRow struct {
	ID               string         `db:"id"`
	WorkflowDefID    string         `db:"workflow_def_id"`
	EngineInstanceID string         `db:"engine_instance_id"`
	TriggerType      string         `db:"trigger_type"`
	TriggerParams    string         `db:"trigger_params"`
	Status           string         `db:"status"`
	Progress         float64        `db:"progress"`
	StartedAt        time.Time      `db:"started_at"`
	FinishedAt       sql.NullTime   `db:"finished_at"`
	ErrorMessage     sql.NullString `db:"error_message"`
	CreatedAt        sql.NullTime   `db:"created_at"`
}

// TaskInstanceRow represents task_instances table row.
type TaskInstanceRow struct {
	ID             string         `db:"id"`
	WorkflowInstID string         `db:"workflow_inst_id"`
	TaskName       string         `db:"task_name"`
	Status         string         `db:"status"`
	StartedAt      sql.NullTime   `db:"started_at"`
	FinishedAt     sql.NullTime   `db:"finished_at"`
	RetryCount     int            `db:"retry_count"`
	ErrorMessage   sql.NullString `db:"error_message"`
	CreatedAt      sql.NullTime   `db:"created_at"`
}

// RealtimeSourceRow represents realtime_sources table row.
type RealtimeSourceRow struct {
	ID                   string         `db:"id"`
	Name                 string         `db:"name"`
	Type                 string         `db:"type"`
	Config               string         `db:"config"`
	Priority             int            `db:"priority"`
	IsPrimary            int            `db:"is_primary"`
	HealthCheckOnStartup int            `db:"health_check_on_startup"`
	Enabled              int            `db:"enabled"`
	LastHealthStatus     sql.NullString `db:"last_health_status"`
	LastHealthAt         sql.NullTime   `db:"last_health_at"`
	LastHealthError      sql.NullString `db:"last_health_error"`
	CreatedAt            time.Time      `db:"created_at"`
	UpdatedAt            time.Time      `db:"updated_at"`
}
