package http

import (
	"context"

	"qdhub/internal/application/contracts"
)

// RealtimeSyncConnector 实现 RealtimeSourceConnector，薄封装 RealtimeSyncController：
// Connect/Disconnect 等价于 StartRealtimeSync/StopRealtimeSync，保证语义统一。
type RealtimeSyncConnector struct {
	controller contracts.RealtimeSyncController
}

// NewRealtimeSyncConnector 创建基于 RealtimeSyncController 的 Connector。
func NewRealtimeSyncConnector(controller contracts.RealtimeSyncController) *RealtimeSyncConnector {
	return &RealtimeSyncConnector{controller: controller}
}

// Connect 调用 StartRealtimeSync，在交易时间窗内启动该源对应的实时工作流（若未运行）并切换 Selector。
func (c *RealtimeSyncConnector) Connect(id string) error {
	return c.controller.StartRealtimeSync(context.Background(), id)
}

// Disconnect 调用 StopRealtimeSync，终止该源对应的实时工作流执行。
func (c *RealtimeSyncConnector) Disconnect(id string) error {
	return c.controller.StopRealtimeSync(context.Background(), id)
}

// IsConnected 返回该源是否已连接（同步运行中）。
func (c *RealtimeSyncConnector) IsConnected(id string) bool {
	return c.controller.IsSourceConnected(id)
}
