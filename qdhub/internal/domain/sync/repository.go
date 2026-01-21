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

	// UpdateExecution 更新执行记录
	UpdatePlanExecution(exec *SyncExecution) error

	// ==================== 查询 ====================

	// GetByDataSource 按数据源获取计划列表
	GetByDataSource(dataSourceID shared.ID) ([]*SyncPlan, error)

	// GetEnabledPlans 获取所有启用的计划
	GetEnabledPlans() ([]*SyncPlan, error)

	// GetByStatus 按状态获取计划列表
	GetByStatus(status PlanStatus) ([]*SyncPlan, error)
}
