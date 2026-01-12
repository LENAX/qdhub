// Package sync contains the sync domain entities.
package sync

import (
	"encoding/json"
	"time"

	"qdhub/internal/domain/shared"
)

// ==================== 聚合根 ====================

// SyncJob represents a sync job aggregate root.
// Responsibilities:
//   - Manage sync job configuration
//   - Manage execution records
//   - Manage scheduling strategy
type SyncJob struct {
	ID             shared.ID
	Name           string
	Description    string
	APIMetadataID  shared.ID
	DataStoreID    shared.ID
	WorkflowDefID  shared.ID
	Mode           SyncMode
	CronExpression *string
	Params         map[string]interface{}
	ParamRules     []ParamRule
	Status         JobStatus
	LastRunAt      *time.Time
	NextRunAt      *time.Time
	CreatedAt      shared.Timestamp
	UpdatedAt      shared.Timestamp

	// Aggregated entities (lazy loaded)
	Executions []SyncExecution
}

// NewSyncJob creates a new SyncJob aggregate.
func NewSyncJob(name, description string, apiMetadataID, dataStoreID, workflowDefID shared.ID, mode SyncMode) *SyncJob {
	now := shared.Now()
	return &SyncJob{
		ID:            shared.NewID(),
		Name:          name,
		Description:   description,
		APIMetadataID: apiMetadataID,
		DataStoreID:   dataStoreID,
		WorkflowDefID: workflowDefID,
		Mode:          mode,
		Params:        make(map[string]interface{}),
		ParamRules:    []ParamRule{},
		Status:        JobStatusDisabled,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

// Enable enables the sync job.
func (sj *SyncJob) Enable() error {
	if sj.Status == JobStatusRunning {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "cannot enable a running job", nil)
	}
	sj.Status = JobStatusEnabled
	sj.UpdatedAt = shared.Now()
	return nil
}

// Disable disables the sync job.
func (sj *SyncJob) Disable() error {
	if sj.Status == JobStatusRunning {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "cannot disable a running job", nil)
	}
	sj.Status = JobStatusDisabled
	sj.UpdatedAt = shared.Now()
	return nil
}

// MarkRunning marks the job as running.
func (sj *SyncJob) MarkRunning() {
	now := time.Now()
	sj.Status = JobStatusRunning
	sj.LastRunAt = &now
	sj.UpdatedAt = shared.Now()
}

// MarkCompleted marks the job as completed and returns to enabled status.
func (sj *SyncJob) MarkCompleted(nextRunAt *time.Time) {
	sj.Status = JobStatusEnabled
	sj.NextRunAt = nextRunAt
	sj.UpdatedAt = shared.Now()
}

// SetCronExpression sets the cron expression for scheduled execution.
func (sj *SyncJob) SetCronExpression(cronExpr string) {
	sj.CronExpression = &cronExpr
	sj.UpdatedAt = shared.Now()
}

// SetParams sets the fixed parameters.
func (sj *SyncJob) SetParams(params map[string]interface{}) {
	sj.Params = params
	sj.UpdatedAt = shared.Now()
}

// AddParamRule adds a parameter rule.
func (sj *SyncJob) AddParamRule(rule ParamRule) {
	sj.ParamRules = append(sj.ParamRules, rule)
	sj.UpdatedAt = shared.Now()
}

// MarshalParamsJSON marshals params to JSON string.
func (sj *SyncJob) MarshalParamsJSON() (string, error) {
	data, err := json.Marshal(sj.Params)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalParamsJSON unmarshals params from JSON string.
func (sj *SyncJob) UnmarshalParamsJSON(jsonStr string) error {
	return json.Unmarshal([]byte(jsonStr), &sj.Params)
}

// MarshalParamRulesJSON marshals param rules to JSON string.
func (sj *SyncJob) MarshalParamRulesJSON() (string, error) {
	data, err := json.Marshal(sj.ParamRules)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalParamRulesJSON unmarshals param rules from JSON string.
func (sj *SyncJob) UnmarshalParamRulesJSON(jsonStr string) error {
	return json.Unmarshal([]byte(jsonStr), &sj.ParamRules)
}

// ==================== 聚合内实体 ====================

// SyncExecution represents a sync execution entity.
// Belongs to: SyncJob aggregate
type SyncExecution struct {
	ID             shared.ID
	SyncJobID      shared.ID
	WorkflowInstID shared.ID
	Status         ExecStatus
	StartedAt      shared.Timestamp
	FinishedAt     *shared.Timestamp
	RecordCount    int64
	ErrorMessage   *string
}

// NewSyncExecution creates a new SyncExecution.
func NewSyncExecution(syncJobID, workflowInstID shared.ID) *SyncExecution {
	return &SyncExecution{
		ID:             shared.NewID(),
		SyncJobID:      syncJobID,
		WorkflowInstID: workflowInstID,
		Status:         ExecStatusPending,
		StartedAt:      shared.Now(),
	}
}

// MarkRunning marks the execution as running.
func (se *SyncExecution) MarkRunning() {
	se.Status = ExecStatusRunning
}

// MarkSuccess marks the execution as successful.
func (se *SyncExecution) MarkSuccess(recordCount int64) {
	now := shared.Now()
	se.Status = ExecStatusSuccess
	se.FinishedAt = &now
	se.RecordCount = recordCount
}

// MarkFailed marks the execution as failed.
func (se *SyncExecution) MarkFailed(errorMsg string) {
	now := shared.Now()
	se.Status = ExecStatusFailed
	se.FinishedAt = &now
	se.ErrorMessage = &errorMsg
}

// MarkCancelled marks the execution as cancelled.
func (se *SyncExecution) MarkCancelled() {
	now := shared.Now()
	se.Status = ExecStatusCancelled
	se.FinishedAt = &now
}

// ==================== 值对象 ====================

// ParamRule represents parameter rule (value object).
type ParamRule struct {
	ParamName  string      `json:"param_name"`
	RuleType   string      `json:"rule_type"`
	RuleConfig interface{} `json:"rule_config"`
}

// ==================== 枚举类型 ====================

// SyncMode represents sync mode.
type SyncMode string

const (
	SyncModeBatch    SyncMode = "batch"
	SyncModeRealtime SyncMode = "realtime"
)

// String returns the string representation of the sync mode.
func (sm SyncMode) String() string {
	return string(sm)
}

// JobStatus represents job status.
type JobStatus string

const (
	JobStatusEnabled  JobStatus = "enabled"
	JobStatusDisabled JobStatus = "disabled"
	JobStatusRunning  JobStatus = "running"
)

// String returns the string representation of the job status.
func (js JobStatus) String() string {
	return string(js)
}

// ExecStatus represents execution status.
type ExecStatus string

const (
	ExecStatusPending   ExecStatus = "pending"
	ExecStatusRunning   ExecStatus = "running"
	ExecStatusSuccess   ExecStatus = "success"
	ExecStatusFailed    ExecStatus = "failed"
	ExecStatusCancelled ExecStatus = "cancelled"
)

// String returns the string representation of the execution status.
func (es ExecStatus) String() string {
	return string(es)
}
