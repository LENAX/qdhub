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
	ID             shared.ID         `json:"id"`
	SyncPlanID     shared.ID         `json:"sync_plan_id"`
	WorkflowInstID shared.ID         `json:"workflow_instance_id,omitempty"`
	Status         ExecStatus       `json:"status"`
	StartedAt      shared.Timestamp  `json:"started_at"`
	FinishedAt     *shared.Timestamp `json:"finished_at,omitempty"`
	RecordCount    int64             `json:"record_count"`
	ErrorMessage   *string           `json:"error_message,omitempty"`
	// WorkflowErrorMessage 当「工作流报失败但明细全成功」被纠正为成功时，保留工作流原始错误信息，便于前端展示警告、排查引擎问题
	WorkflowErrorMessage *string `json:"workflow_error_message,omitempty"`

	// New fields for SyncPlan
	ExecuteParams *ExecuteParams `json:"execute_params,omitempty"`
	SyncedAPIs    []string       `json:"synced_apis,omitempty"`
	SkippedAPIs   []string       `json:"skipped_apis,omitempty"`
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

// SyncExecutionDetail 单次同步任务明细（按 API/任务记录行数、状态、错误信息）
type SyncExecutionDetail struct {
	ID           shared.ID         `json:"id"`
	ExecutionID  shared.ID         `json:"execution_id"`
	TaskID       string            `json:"task_id"`
	APIName      string            `json:"api_name"`
	RecordCount  int64             `json:"record_count"`
	Status       string            `json:"status"` // success / failed
	ErrorMessage *string           `json:"error_message,omitempty"`
	StartedAt    *shared.Timestamp `json:"started_at,omitempty"`
	FinishedAt   *shared.Timestamp `json:"finished_at,omitempty"`
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
	ID           shared.ID  `json:"id"`
	Name         string     `json:"name"`
	Description  string     `json:"description"`
	DataSourceID shared.ID  `json:"data_source_id"`
	DataStoreID  shared.ID  `json:"data_store_id"`

	// PlanMode 控制同步模式：
	//   - batch: 现有批量同步（默认）
	//   - realtime: 新增实时同步（流式工作流）
	Mode PlanMode `json:"mode"`

	// 用户配置
	SelectedAPIs []string `json:"selected_apis,omitempty"`

	// 解析结果
	ResolvedAPIs   []string         `json:"resolved_apis,omitempty"`
	ExecutionGraph *ExecutionGraph `json:"execution_graph,omitempty"`

	// 调度配置
	CronExpression *string `json:"cron_expression,omitempty"`
	// 运行时段（仅 realtime 模式）：在 start 与 end 之间自动启动，之外自动停止
	// 去掉 omitempty，便于前端在 /sync-plans 与 /sync-plans/:id 中始终看到字段（未配置时为 null）。
	ScheduleStartCron *string `json:"schedule_start_cron"` // 如 "0 0 9 * * 1-5" 工作日 9:00 启动
	ScheduleEndCron   *string `json:"schedule_end_cron"`   // 如 "0 30 15 * * 1-5" 工作日 15:30 停止
	// Pull 模式拉取间隔（秒），0 表示使用默认 60；仅 realtime 模式生效
	PullIntervalSeconds int `json:"pull_interval_seconds,omitempty"`

	// 默认执行参数（用于定时触发）
	DefaultExecuteParams *ExecuteParams `json:"default_execute_params,omitempty"`

	// 增量模式：定时触发时用上次成功执行的 EndDate 作为本次 StartDate，EndDate 为当前日期；首次无记录时用 DefaultExecuteParams
	IncrementalMode       bool    `json:"incremental_mode"`
	LastSuccessfulEndDate *string `json:"last_successful_end_date,omitempty"`
	// 可选：用于取“数据最新日期”的 API（表名）与列名，从目标 DuckDB 执行 MAX(列) 得到
	IncrementalStartDateAPI    *string `json:"incremental_start_date_api,omitempty"`
	IncrementalStartDateColumn *string `json:"incremental_start_date_column,omitempty"`

	// 状态
	Status         PlanStatus  `json:"status"`
	LastExecutedAt *time.Time  `json:"last_run_at,omitempty"`
	NextExecuteAt  *time.Time  `json:"next_run_at,omitempty"`

	// LastExecutionStatus 最近一次执行状态，仅用于列表等接口展示，不持久化
	LastExecutionStatus *ExecStatus `json:"last_execution_status,omitempty"`

	// 时间戳
	CreatedAt shared.Timestamp `json:"created_at"`
	UpdatedAt shared.Timestamp `json:"updated_at"`

	// 聚合内实体（懒加载）
	Tasks      []*SyncTask      `json:"tasks,omitempty"`
	Executions []*SyncExecution `json:"executions,omitempty"`
}

// NewSyncPlan creates a new SyncPlan aggregate.
func NewSyncPlan(name, description string, dataSourceID shared.ID, selectedAPIs []string) *SyncPlan {
	now := shared.Now()
	return &SyncPlan{
		ID:           shared.NewID(),
		Name:         name,
		Description:  description,
		DataSourceID: dataSourceID,
		Mode:         PlanModeBatch,
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

// SetScheduleWindow sets the running window crons (start/end) for realtime mode; empty string clears.
func (sp *SyncPlan) SetScheduleWindow(startCron, endCron string) {
	if startCron == "" {
		sp.ScheduleStartCron = nil
	} else {
		sp.ScheduleStartCron = &startCron
	}
	if endCron == "" {
		sp.ScheduleEndCron = nil
	} else {
		sp.ScheduleEndCron = &endCron
	}
	sp.UpdatedAt = shared.Now()
}

// SetPullIntervalSeconds sets the pull interval in seconds for realtime mode (0 = use default 60).
func (sp *SyncPlan) SetPullIntervalSeconds(seconds int) {
	sp.PullIntervalSeconds = seconds
	sp.UpdatedAt = shared.Now()
}

// SetDefaultExecuteParams sets the default execute params for scheduled runs.
func (sp *SyncPlan) SetDefaultExecuteParams(p *ExecuteParams) {
	sp.DefaultExecuteParams = p
	sp.UpdatedAt = shared.Now()
}

// SetIncrementalMode sets whether scheduled runs use incremental date range (last successful EndDate -> today).
func (sp *SyncPlan) SetIncrementalMode(enabled bool) {
	sp.IncrementalMode = enabled
	sp.UpdatedAt = shared.Now()
}

// SetLastSuccessfulEndDate sets the EndDate of the last successful execution (used for next incremental run).
func (sp *SyncPlan) SetLastSuccessfulEndDate(endDate string) {
	if endDate == "" {
		sp.LastSuccessfulEndDate = nil
	} else {
		sp.LastSuccessfulEndDate = &endDate
	}
	sp.UpdatedAt = shared.Now()
}

// SetIncrementalStartDateSource sets the optional table name and date column for incremental sync.
// SyncPlan 增量不依赖 sync_checkpoint 表：执行时在目标 DuckDB 上执行 MAX(column) FROM table 得到「表中最新日期」，
// 与 LastSuccessfulEndDate 一起决定本次同步的起始日，结束日由执行日当天决定。
func (sp *SyncPlan) SetIncrementalStartDateSource(api, column string) {
	if api == "" && column == "" {
		sp.IncrementalStartDateAPI = nil
		sp.IncrementalStartDateColumn = nil
	} else {
		if api != "" {
			sp.IncrementalStartDateAPI = &api
		} else {
			sp.IncrementalStartDateAPI = nil
		}
		if column != "" {
			sp.IncrementalStartDateColumn = &column
		} else {
			sp.IncrementalStartDateColumn = nil
		}
	}
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
	ID         shared.ID     `json:"id"`
	SyncPlanID shared.ID     `json:"sync_plan_id"`
	APIName    string        `json:"api_name"`
	SyncMode   TaskSyncMode  `json:"sync_mode"`

	// 参数配置
	Params        map[string]interface{} `json:"params,omitempty"`
	ParamMappings []ParamMapping         `json:"param_mappings,omitempty"`

	// 任务级依赖（解析后填充）
	Dependencies []string `json:"dependencies,omitempty"`

	// 排序
	Level     int `json:"level"`
	SortOrder int `json:"sort_order"`

	// 同步频率控制
	SyncFrequency time.Duration `json:"sync_frequency,omitempty"`
	LastSyncedAt  *time.Time   `json:"last_synced_at,omitempty"`

	CreatedAt shared.Timestamp `json:"created_at"`
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

// PlanMode represents sync plan execution mode.
// batch    - 现有批量同步模式，通过 BatchDataSync 工作流按时间/代码维度拉取历史数据。
// realtime - 新增实时同步模式，通过流式工作流与 RealtimeAdapter 获取实时行情。
type PlanMode string

const (
	PlanModeBatch    PlanMode = "batch"
	PlanModeRealtime PlanMode = "realtime"
)

// String returns the string representation of the plan mode.
func (pm PlanMode) String() string {
	return string(pm)
}

// IsValid returns whether the plan mode is valid.
func (pm PlanMode) IsValid() bool {
	return pm == PlanModeBatch || pm == PlanModeRealtime
}

// RealtimeAllowedAPIs 白名单：仅这些 API 支持在 PlanModeRealtime 下运行。
// 注意：ETF 实时分钟行情与 rt_min 共用策略，因此复用 "rt_min"，不单独列出。
var RealtimeAllowedAPIs = []string{
	"rt_min",
	"realtime_quote",
	"realtime_tick",
	"realtime_list",
	"rt_idx_min",
	"ts_realtime_mkt_tick",
}

// IsRealtimeAPI checks whether the given apiName is allowed in realtime mode.
func IsRealtimeAPI(apiName string) bool {
	for _, v := range RealtimeAllowedAPIs {
		if v == apiName {
			return true
		}
	}
	return false
}

// ExecStatus represents execution status.
type ExecStatus string

const (
	ExecStatusPending   ExecStatus = "pending"
	ExecStatusRunning   ExecStatus = "running"
	ExecStatusPaused    ExecStatus = "paused" // 仅由进度接口从引擎合并，不持久化
	ExecStatusSuccess   ExecStatus = "success"
	ExecStatusFailed    ExecStatus = "failed"
	ExecStatusCancelled ExecStatus = "cancelled"
)

// String returns the string representation of the execution status.
func (es ExecStatus) String() string {
	return string(es)
}
