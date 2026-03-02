// Package sync contains the sync domain repository interfaces.
package sync

import "qdhub/internal/domain/shared"

// SyncPlanRepository 同步计划仓储接口
// 管理 SyncPlan 聚合根及其子实体（SyncTask、SyncExecution）
type SyncPlanRepository interface {
	// 继承基础仓储操作
	shared.Repository[SyncPlan]

	// ==================== SyncTask 操作 ====================

	// AddTask 添加 SyncTask
	AddTask(task *SyncTask) error

	// GetTask 获取单个 SyncTask
	GetTask(id shared.ID) (*SyncTask, error)

	// GetTasksByPlan 获取计划的所有任务
	GetTasksByPlan(planID shared.ID) ([]*SyncTask, error)

	// UpdateTask 更新 SyncTask（用于更新 LastSyncedAt 等）
	UpdateTask(task *SyncTask) error

	// DeleteTasksByPlan 删除计划的所有任务
	DeleteTasksByPlan(planID shared.ID) error

	// ==================== SyncExecution 操作 ====================

	// AddExecution 添加执行记录
	AddPlanExecution(exec *SyncExecution) error

	// GetExecution 获取执行记录
	GetPlanExecution(id shared.ID) (*SyncExecution, error)

	// GetExecutionsByPlan 获取计划的所有执行记录
	GetExecutionsByPlan(planID shared.ID) ([]*SyncExecution, error)

	// GetExecutionsByPlanPaged 分页获取计划的执行记录，按 started_at DESC，返回列表与总数
	GetExecutionsByPlanPaged(planID shared.ID, limit, offset int) ([]*SyncExecution, int, error)

	// GetExecutionByWorkflowInstID 按 workflow 实例 ID 获取执行记录（用于完成回调）
	GetExecutionByWorkflowInstID(workflowInstID string) (*SyncExecution, error)

	// UpdateExecution 更新执行记录
	UpdatePlanExecution(exec *SyncExecution) error

	// AddExecutionDetail 添加一条执行明细（某 API 任务成功/失败及行数、错误信息）
	AddExecutionDetail(detail *SyncExecutionDetail) error

	// GetExecutionDetailsByExecutionID 按执行 ID 获取所有明细（用于统计与错误排查）
	GetExecutionDetailsByExecutionID(executionID shared.ID) ([]*SyncExecutionDetail, error)

	// ==================== 查询 ====================

	// GetByDataSource 按数据源获取计划列表
	GetByDataSource(dataSourceID shared.ID) ([]*SyncPlan, error)

	// GetByDataStore 按数据存储获取计划列表（用于数据质量缺失分析：判断是否有计划曾同步该 store）
	GetByDataStore(dataStoreID shared.ID) ([]*SyncPlan, error)

	// GetEnabledPlans 获取所有启用的计划
	GetEnabledPlans() ([]*SyncPlan, error)

	// GetSchedulablePlans 获取需纳入 cron 触发的计划：有 cron 且状态非 disabled（含 draft/resolved/enabled/running）
	GetSchedulablePlans() ([]*SyncPlan, error)

	// GetByStatus 按状态获取计划列表
	GetByStatus(status PlanStatus) ([]*SyncPlan, error)
}
