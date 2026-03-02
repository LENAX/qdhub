// Package sync contains the sync domain services.
package sync

import (
	"context"
	"time"
)

// ==================== 领域服务接口（纯业务逻辑）====================

// DependencyResolver 依赖解析器（领域服务）
// 用于解析用户选择的 API 之间的依赖关系
// Implementation: sync/dependency_resolver.go
type DependencyResolver interface {
	// Resolve 解析依赖关系
	// 输入：用户选择的 API、所有 API 的依赖规则
	// 输出：执行图（含自动补充的依赖 API）、完整的 API 列表
	Resolve(selectedAPIs []string, allAPIDependencies map[string][]ParamDependency) (*ExecutionGraph, []string, error)
}

// ParamDependency 参数依赖规则
// 从 metadata.ParamDependency 复制，避免循环依赖
type ParamDependency struct {
	ParamName   string // 参数名，如 "ts_code"
	SourceAPI   string // 来源 API，如 "stock_basic"
	SourceField string // 来源字段，如 "ts_code"
	IsList      bool   // 是否是列表（需要拆分子任务）
	FilterField string // 过滤字段（可选）
	FilterValue any    // 过滤值（可选）
}

// CronScheduleCalculator defines domain service for cron schedule calculation.
// Implementation: infrastructure/scheduler/cron_scheduler.go (CronSchedulerCalculatorAdapter)
type CronScheduleCalculator interface {
	// CalculateNextRunTime calculates the next run time based on cron expression.
	CalculateNextRunTime(cronExpr string, fromTime time.Time) (*time.Time, error)

	// ParseCronExpression parses cron expression and validates it.
	ParseCronExpression(cronExpr string) error
}

// ==================== 调度器接口（基础设施层实现）====================

// PlanScheduler defines the interface for scheduling sync plans.
// Implementation: infrastructure/scheduler/cron_scheduler.go
type PlanScheduler interface {
	// Start starts the scheduler.
	Start()

	// Stop stops the scheduler.
	Stop() context.Context

	// SchedulePlan schedules a plan with the given cron expression.
	SchedulePlan(planID string, cronExpr string) error

	// UnschedulePlan removes a plan from the scheduler.
	UnschedulePlan(planID string)

	// GetScheduledPlanIDs returns IDs of plans currently registered in the scheduler (for reconciliation).
	GetScheduledPlanIDs() []string

	// IsScheduled returns true if the plan is currently scheduled.
	IsScheduled(planID string) bool

	// GetNextRunTime returns the next scheduled run time for a plan.
	GetNextRunTime(planID string) *time.Time
}
