// Package sync contains the sync domain entities.
package sync

import (
	"encoding/json"
	"time"

	"qdhub/internal/domain/shared"
)

// ==================== 聚合内实体 ====================

// SyncExecution represents a sync execution entity.
// Belongs to: SyncPlan aggregate
type SyncExecution struct {
	ID             shared.ID
	SyncPlanID     shared.ID // 关联 SyncPlan
	WorkflowInstID shared.ID
	Status         ExecStatus
	StartedAt      shared.Timestamp
	FinishedAt     *shared.Timestamp
	RecordCount    int64
	ErrorMessage   *string

	// New fields for SyncPlan
	ExecuteParams *ExecuteParams // 执行参数快照
	SyncedAPIs    []string       // 本次实际同步的 API 列表
	SkippedAPIs   []string       // 本次跳过的 API 列表（基于频率）
}

// NewSyncExecution creates a new SyncExecution for SyncPlan.
func NewSyncExecution(syncPlanID, workflowInstID shared.ID) *SyncExecution {
	return &SyncExecution{
		ID:             shared.NewID(),
		SyncPlanID:     syncPlanID,
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

// MarshalExecuteParamsJSON marshals execute params to JSON string.
func (se *SyncExecution) MarshalExecuteParamsJSON() (string, error) {
	if se.ExecuteParams == nil {
		return "", nil
	}
	data, err := json.Marshal(se.ExecuteParams)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalExecuteParamsJSON unmarshals execute params from JSON string.
func (se *SyncExecution) UnmarshalExecuteParamsJSON(jsonStr string) error {
	if jsonStr == "" {
		se.ExecuteParams = nil
		return nil
	}
	var params ExecuteParams
	if err := json.Unmarshal([]byte(jsonStr), &params); err != nil {
		return err
	}
	se.ExecuteParams = &params
	return nil
}

// MarshalSyncedAPIsJSON marshals synced apis to JSON string.
func (se *SyncExecution) MarshalSyncedAPIsJSON() (string, error) {
	if len(se.SyncedAPIs) == 0 {
		return "", nil
	}
	data, err := json.Marshal(se.SyncedAPIs)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalSyncedAPIsJSON unmarshals synced apis from JSON string.
func (se *SyncExecution) UnmarshalSyncedAPIsJSON(jsonStr string) error {
	if jsonStr == "" {
		se.SyncedAPIs = nil
		return nil
	}
	return json.Unmarshal([]byte(jsonStr), &se.SyncedAPIs)
}

// MarshalSkippedAPIsJSON marshals skipped apis to JSON string.
func (se *SyncExecution) MarshalSkippedAPIsJSON() (string, error) {
	if len(se.SkippedAPIs) == 0 {
		return "", nil
	}
	data, err := json.Marshal(se.SkippedAPIs)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalSkippedAPIsJSON unmarshals skipped apis from JSON string.
func (se *SyncExecution) UnmarshalSkippedAPIsJSON(jsonStr string) error {
	if jsonStr == "" {
		se.SkippedAPIs = nil
		return nil
	}
	return json.Unmarshal([]byte(jsonStr), &se.SkippedAPIs)
}

// ==================== 值对象 ====================

// ExecuteParams 执行参数（值对象）
// 用于记录 SyncPlan 执行时的参数快照
type ExecuteParams struct {
	TargetDBPath string `json:"target_db_path"`
	StartDate    string `json:"start_date"`
	EndDate      string `json:"end_date"`
	StartTime    string `json:"start_time,omitempty"`
	EndTime      string `json:"end_time,omitempty"`
}

// ParamMapping 参数映射（值对象）
// 定义如何从上游任务获取参数值
type ParamMapping struct {
	ParamName   string `json:"param_name"`             // 参数名，如 "ts_code"
	SourceTask  string `json:"source_task"`            // 上游任务名，如 "FetchStockBasic"
	SourceField string `json:"source_field"`           // 上游结果字段，如 "ts_code"
	IsList      bool   `json:"is_list"`                // 是否是列表（需要拆分子任务）
	Select      string `json:"select"`                 // 选择策略: first | last | all
	FilterField string `json:"filter_field,omitempty"` // 过滤字段（可选）
	FilterValue any    `json:"filter_value,omitempty"` // 过滤值（可选）
}

// TaskConfig 任务配置（值对象）
// ExecutionGraph 中每个 API 的配置
type TaskConfig struct {
	APIName       string         `json:"api_name"`
	SyncMode      TaskSyncMode   `json:"sync_mode"`
	Dependencies  []string       `json:"dependencies"`
	ParamMappings []ParamMapping `json:"param_mappings"`
}

// ExecutionGraph 执行图（值对象）
// 依赖解析后的执行计划
type ExecutionGraph struct {
	Levels      [][]string             `json:"levels"`       // 分层执行顺序
	MissingAPIs []string               `json:"missing_apis"` // 自动补充的依赖 API
	TaskConfigs map[string]*TaskConfig `json:"task_configs"` // 每个 API 的任务配置
}

// MarshalJSON marshals ExecutionGraph to JSON.
func (eg *ExecutionGraph) MarshalJSON() ([]byte, error) {
	type Alias ExecutionGraph
	return json.Marshal((*Alias)(eg))
}

// UnmarshalJSON unmarshals ExecutionGraph from JSON.
func (eg *ExecutionGraph) UnmarshalJSON(data []byte) error {
	type Alias ExecutionGraph
	return json.Unmarshal(data, (*Alias)(eg))
}

// ==================== 新聚合根：SyncPlan ====================

// SyncPlan 同步计划聚合根
// 职责：
//   - 管理多 API 同步计划的完整配置
//   - 维护 SyncTask 集合（单个 API 的同步参数）
//   - 维护 SyncExecution 集合（执行记录）
//   - 存储解析后的执行图
type SyncPlan struct {
	ID           shared.ID
	Name         string
	Description  string
	DataSourceID shared.ID
	DataStoreID  shared.ID // 目标数据存储

	// 用户配置
	SelectedAPIs []string // 用户选择的 API 列表

	// 解析结果
	ResolvedAPIs   []string        // 解析后的完整 API 列表（含自动补充的）
	ExecutionGraph *ExecutionGraph // 依赖解析后的执行图

	// 调度配置
	CronExpression *string // 定时表达式（可选）

	// 默认执行参数（用于定时触发）
	DefaultExecuteParams *ExecuteParams

	// 状态
	Status         PlanStatus
	LastExecutedAt *time.Time
	NextExecuteAt  *time.Time

	// 时间戳
	CreatedAt shared.Timestamp
	UpdatedAt shared.Timestamp

	// 聚合内实体（懒加载）
	Tasks      []*SyncTask      // 各 API 的同步配置
	Executions []*SyncExecution // 执行记录
}

// NewSyncPlan creates a new SyncPlan aggregate.
func NewSyncPlan(name, description string, dataSourceID shared.ID, selectedAPIs []string) *SyncPlan {
	now := shared.Now()
	return &SyncPlan{
		ID:           shared.NewID(),
		Name:         name,
		Description:  description,
		DataSourceID: dataSourceID,
		SelectedAPIs: selectedAPIs,
		Status:       PlanStatusDraft,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
}

// SetDataStore sets the target data store.
func (sp *SyncPlan) SetDataStore(dataStoreID shared.ID) {
	sp.DataStoreID = dataStoreID
	sp.UpdatedAt = shared.Now()
}

// SetExecutionGraph sets the resolved execution graph.
func (sp *SyncPlan) SetExecutionGraph(graph *ExecutionGraph, resolvedAPIs []string) {
	sp.ExecutionGraph = graph
	sp.ResolvedAPIs = resolvedAPIs
	sp.Status = PlanStatusResolved
	sp.UpdatedAt = shared.Now()
}

// SetCronExpression sets the cron expression for scheduled execution.
func (sp *SyncPlan) SetCronExpression(cronExpr string) {
	sp.CronExpression = &cronExpr
	sp.UpdatedAt = shared.Now()
}

// SetDefaultExecuteParams sets the default execute params for scheduled runs.
func (sp *SyncPlan) SetDefaultExecuteParams(p *ExecuteParams) {
	sp.DefaultExecuteParams = p
	sp.UpdatedAt = shared.Now()
}

// Enable enables the sync plan.
func (sp *SyncPlan) Enable() error {
	if sp.Status == PlanStatusRunning {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "cannot enable a running plan", nil)
	}
	if sp.Status == PlanStatusDraft {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "cannot enable a draft plan, resolve dependencies first", nil)
	}
	sp.Status = PlanStatusEnabled
	sp.UpdatedAt = shared.Now()
	return nil
}

// Disable disables the sync plan.
func (sp *SyncPlan) Disable() error {
	if sp.Status == PlanStatusRunning {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "cannot disable a running plan", nil)
	}
	sp.Status = PlanStatusDisabled
	sp.UpdatedAt = shared.Now()
	return nil
}

// MarkRunning marks the plan as running.
func (sp *SyncPlan) MarkRunning() {
	now := time.Now()
	sp.Status = PlanStatusRunning
	sp.LastExecutedAt = &now
	sp.UpdatedAt = shared.Now()
}

// MarkCompleted marks the plan as completed and returns to enabled status.
func (sp *SyncPlan) MarkCompleted(nextRunAt *time.Time) {
	sp.Status = PlanStatusEnabled
	sp.NextExecuteAt = nextRunAt
	sp.UpdatedAt = shared.Now()
}

// AddTask adds a SyncTask to the plan.
func (sp *SyncPlan) AddTask(task *SyncTask) {
	task.SyncPlanID = sp.ID
	sp.Tasks = append(sp.Tasks, task)
}

// MarshalSelectedAPIsJSON marshals selected apis to JSON string.
func (sp *SyncPlan) MarshalSelectedAPIsJSON() (string, error) {
	data, err := json.Marshal(sp.SelectedAPIs)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalSelectedAPIsJSON unmarshals selected apis from JSON string.
func (sp *SyncPlan) UnmarshalSelectedAPIsJSON(jsonStr string) error {
	return json.Unmarshal([]byte(jsonStr), &sp.SelectedAPIs)
}

// MarshalResolvedAPIsJSON marshals resolved apis to JSON string.
func (sp *SyncPlan) MarshalResolvedAPIsJSON() (string, error) {
	if len(sp.ResolvedAPIs) == 0 {
		return "", nil
	}
	data, err := json.Marshal(sp.ResolvedAPIs)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalResolvedAPIsJSON unmarshals resolved apis from JSON string.
func (sp *SyncPlan) UnmarshalResolvedAPIsJSON(jsonStr string) error {
	if jsonStr == "" {
		sp.ResolvedAPIs = nil
		return nil
	}
	return json.Unmarshal([]byte(jsonStr), &sp.ResolvedAPIs)
}

// MarshalExecutionGraphJSON marshals execution graph to JSON string.
func (sp *SyncPlan) MarshalExecutionGraphJSON() (string, error) {
	if sp.ExecutionGraph == nil {
		return "", nil
	}
	data, err := json.Marshal(sp.ExecutionGraph)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalExecutionGraphJSON unmarshals execution graph from JSON string.
func (sp *SyncPlan) UnmarshalExecutionGraphJSON(jsonStr string) error {
	if jsonStr == "" {
		sp.ExecutionGraph = nil
		return nil
	}
	var graph ExecutionGraph
	if err := json.Unmarshal([]byte(jsonStr), &graph); err != nil {
		return err
	}
	sp.ExecutionGraph = &graph
	return nil
}

// MarshalDefaultExecuteParamsJSON marshals default execute params to JSON string.
func (sp *SyncPlan) MarshalDefaultExecuteParamsJSON() (string, error) {
	if sp.DefaultExecuteParams == nil {
		return "", nil
	}
	data, err := json.Marshal(sp.DefaultExecuteParams)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalDefaultExecuteParamsJSON unmarshals default execute params from JSON string.
func (sp *SyncPlan) UnmarshalDefaultExecuteParamsJSON(jsonStr string) error {
	if jsonStr == "" {
		sp.DefaultExecuteParams = nil
		return nil
	}
	var p ExecuteParams
	if err := json.Unmarshal([]byte(jsonStr), &p); err != nil {
		return err
	}
	sp.DefaultExecuteParams = &p
	return nil
}

// ==================== 聚合内实体：SyncTask ====================

// SyncTask 单个 API 的同步配置（聚合内实体）
// 替代原 SyncJob 的参数配置部分
type SyncTask struct {
	ID         shared.ID
	SyncPlanID shared.ID
	APIName    string       // API 名称
	SyncMode   TaskSyncMode // 同步模式: direct | template

	// 参数配置
	Params        map[string]interface{} // 固定参数
	ParamMappings []ParamMapping         // 参数映射规则（从上游获取）

	// 任务级依赖（解析后填充）
	Dependencies []string // 依赖的任务名称

	// 排序
	Level     int // 执行层级（0=无依赖）
	SortOrder int // 同层级内的排序

	// 同步频率控制
	SyncFrequency time.Duration // 同步频率（如 24h, 168h=7d, 720h=30d）
	LastSyncedAt  *time.Time    // 上次成功同步时间

	CreatedAt shared.Timestamp
}

// 常用同步频率常量
const (
	SyncFrequencyDaily   = 24 * time.Hour      // 每天
	SyncFrequencyWeekly  = 7 * 24 * time.Hour  // 每周
	SyncFrequencyMonthly = 30 * 24 * time.Hour // 每月
	SyncFrequencyOnce    = -1 * time.Hour      // 只同步一次（特殊值）
	SyncFrequencyAlways  = 0                   // 每次都同步
)

// NewSyncTask creates a new SyncTask.
func NewSyncTask(apiName string, syncMode TaskSyncMode, level int) *SyncTask {
	return &SyncTask{
		ID:            shared.NewID(),
		APIName:       apiName,
		SyncMode:      syncMode,
		Level:         level,
		Params:        make(map[string]interface{}),
		ParamMappings: []ParamMapping{},
		Dependencies:  []string{},
		SyncFrequency: SyncFrequencyAlways,
		CreatedAt:     shared.Now(),
	}
}

// NeedsSync 判断是否需要同步
func (t *SyncTask) NeedsSync() bool {
	// 频率为 0 表示每次都同步
	if t.SyncFrequency == 0 {
		return true
	}
	// 特殊值 -1 表示只同步一次
	if t.SyncFrequency < 0 && t.LastSyncedAt != nil {
		return false
	}
	// 从未同步过
	if t.LastSyncedAt == nil {
		return true
	}
	// 检查是否超过频率周期
	return time.Since(*t.LastSyncedAt) >= t.SyncFrequency
}

// MarkSynced 标记已同步
func (t *SyncTask) MarkSynced() {
	now := time.Now()
	t.LastSyncedAt = &now
}

// SetParamMappings sets the parameter mappings.
func (t *SyncTask) SetParamMappings(mappings []ParamMapping) {
	t.ParamMappings = mappings
}

// SetDependencies sets the task dependencies.
func (t *SyncTask) SetDependencies(deps []string) {
	t.Dependencies = deps
}

// SetSyncFrequency sets the sync frequency.
func (t *SyncTask) SetSyncFrequency(freq time.Duration) {
	t.SyncFrequency = freq
}

// MarshalParamsJSON marshals params to JSON string.
func (t *SyncTask) MarshalParamsJSON() (string, error) {
	if len(t.Params) == 0 {
		return "", nil
	}
	data, err := json.Marshal(t.Params)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalParamsJSON unmarshals params from JSON string.
func (t *SyncTask) UnmarshalParamsJSON(jsonStr string) error {
	if jsonStr == "" {
		t.Params = make(map[string]interface{})
		return nil
	}
	return json.Unmarshal([]byte(jsonStr), &t.Params)
}

// MarshalParamMappingsJSON marshals param mappings to JSON string.
func (t *SyncTask) MarshalParamMappingsJSON() (string, error) {
	if len(t.ParamMappings) == 0 {
		return "", nil
	}
	data, err := json.Marshal(t.ParamMappings)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalParamMappingsJSON unmarshals param mappings from JSON string.
func (t *SyncTask) UnmarshalParamMappingsJSON(jsonStr string) error {
	if jsonStr == "" {
		t.ParamMappings = []ParamMapping{}
		return nil
	}
	return json.Unmarshal([]byte(jsonStr), &t.ParamMappings)
}

// MarshalDependenciesJSON marshals dependencies to JSON string.
func (t *SyncTask) MarshalDependenciesJSON() (string, error) {
	if len(t.Dependencies) == 0 {
		return "", nil
	}
	data, err := json.Marshal(t.Dependencies)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalDependenciesJSON unmarshals dependencies from JSON string.
func (t *SyncTask) UnmarshalDependenciesJSON(jsonStr string) error {
	if jsonStr == "" {
		t.Dependencies = []string{}
		return nil
	}
	return json.Unmarshal([]byte(jsonStr), &t.Dependencies)
}

// ==================== 枚举类型 ====================

// TaskSyncMode represents sync mode for SyncTask.
type TaskSyncMode string

const (
	TaskSyncModeDirect   TaskSyncMode = "direct"   // 直接同步（不拆分）
	TaskSyncModeTemplate TaskSyncMode = "template" // 模板任务（按参数拆分子任务）
)

// String returns the string representation of the task sync mode.
func (tsm TaskSyncMode) String() string {
	return string(tsm)
}

// PlanStatus represents sync plan status.
type PlanStatus string

const (
	PlanStatusDraft    PlanStatus = "draft"    // 草稿，未解析依赖
	PlanStatusResolved PlanStatus = "resolved" // 已解析依赖
	PlanStatusEnabled  PlanStatus = "enabled"  // 已启用
	PlanStatusDisabled PlanStatus = "disabled" // 已禁用
	PlanStatusRunning  PlanStatus = "running"  // 执行中
)

// String returns the string representation of the plan status.
func (ps PlanStatus) String() string {
	return string(ps)
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
