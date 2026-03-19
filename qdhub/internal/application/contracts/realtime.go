// Package contracts defines application service interfaces and DTOs.
package contracts

import (
	"context"

	"qdhub/internal/domain/realtime"
	"qdhub/internal/domain/shared"
)

// RealtimeSourceApplicationService defines application service for realtime data source management.
type RealtimeSourceApplicationService interface {
	List(ctx context.Context) ([]*realtime.RealtimeSource, error)
	Get(ctx context.Context, id shared.ID) (*realtime.RealtimeSource, error)
	Create(ctx context.Context, req CreateRealtimeSourceRequest) (*realtime.RealtimeSource, error)
	Update(ctx context.Context, id shared.ID, req UpdateRealtimeSourceRequest) (*realtime.RealtimeSource, error)
	Delete(ctx context.Context, id shared.ID) error
	// TriggerHealthCheck triggers a one-off health check for the given source and returns the result (optional).
	TriggerHealthCheck(ctx context.Context, id shared.ID) (status string, errMsg string, err error)
}

// CreateRealtimeSourceRequest is the request to create a realtime source.
type CreateRealtimeSourceRequest struct {
	Name                  string
	Type                  string
	Config                string // JSON: type-specific (ws_url, rsa_public_key_path, etc.)
	Priority              int
	IsPrimary             bool
	HealthCheckOnStartup  bool
	Enabled               bool
}

// UpdateRealtimeSourceRequest is the request to update a realtime source (partial fields).
type UpdateRealtimeSourceRequest struct {
	Name                  *string
	Config                *string
	Priority              *int
	IsPrimary             *bool
	HealthCheckOnStartup  *bool
	Enabled               *bool
}

// RealtimeSyncController 统一启动/停止实时同步的领域操作。
// 所有入口（自动调度、灾备切换、前端 connect/disconnect）应只通过此接口启停，保证语义一致。
type RealtimeSyncController interface {
	// StartRealtimeSync 根据 realtime_sources.id 找到对应源与映射的 SyncPlan，若该计划无运行中执行则启动实时工作流，并更新 Selector 为当前源。
	StartRealtimeSync(ctx context.Context, sourceID string) error
	// StopRealtimeSync 查找由该源驱动的实时执行，若有则取消执行并更新状态。
	StopRealtimeSync(ctx context.Context, sourceID string) error
	// IsSourceConnected 返回该源当前是否处于连接/同步运行中（新闻轮询或对应 SyncPlan 正在执行），供前端展示连接/断开按钮状态。
	IsSourceConnected(sourceID string) bool
}
