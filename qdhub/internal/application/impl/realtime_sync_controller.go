// Package impl contains application service implementations.
package impl

import (
	"context"
	"fmt"
	"runtime"
	gosync "sync"
	"time"

	"github.com/sirupsen/logrus"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/realtime"
	"qdhub/internal/domain/shared"
	syncDomain "qdhub/internal/domain/sync"
	"qdhub/internal/infrastructure/realtimestore"
)

// 实时源 type -> 标准 SyncPlan id（与 migration 033、032 约定一致）
var realtimeSourceTypeToPlanID = map[string]string{
	realtime.TypeTushareProxy: "realtime-ts-forward",
	realtime.TypeSina:         "realtime-sina-quote",
	realtime.TypeTushareWS:    "realtime-tushare-ws-tick",
	realtime.TypeNews:         "realtime-news",
}

// syncPlanRepoForRealtime 仅需 GetRunningExecutionByPlanID，避免循环依赖 SyncPlanRepository 全量接口。
type syncPlanRepoForRealtime interface {
	GetRunningExecutionByPlanID(planID shared.ID) (*syncDomain.SyncExecution, error)
}

// RealtimeSyncControllerImpl 实现 RealtimeSyncController，通过 SyncApplicationService 启停实时 SyncPlan，并更新 Selector。
type RealtimeSyncControllerImpl struct {
	realtimeRepo realtime.RealtimeSourceRepository
	syncSvc      contracts.SyncApplicationService
	syncPlanRepo syncPlanRepoForRealtime
	selector     *realtimestore.RealtimeSourceSelector

	mu          gosync.Mutex
	newsCancel  context.CancelFunc
	newsSourceID string // 当前新闻轮询对应的 sourceID，用于 IsSourceConnected
}

// NewRealtimeSyncController 创建 RealtimeSyncController 实现。
func NewRealtimeSyncController(
	realtimeRepo realtime.RealtimeSourceRepository,
	syncSvc contracts.SyncApplicationService,
	syncPlanRepo syncPlanRepoForRealtime,
	selector *realtimestore.RealtimeSourceSelector,
) contracts.RealtimeSyncController {
	return &RealtimeSyncControllerImpl{
		realtimeRepo: realtimeRepo,
		syncSvc:      syncSvc,
		syncPlanRepo: syncPlanRepo,
		selector:     selector,
	}
}

// StartRealtimeSync 根据 sourceID 查源与映射的 SyncPlan，启动实时同步。
// 对新闻源使用轮询循环，对行情源使用 SyncPlan。
func (c *RealtimeSyncControllerImpl) StartRealtimeSync(ctx context.Context, sourceID string) error {
	src, err := c.realtimeRepo.Get(shared.ID(sourceID))
	if err != nil {
		return fmt.Errorf("get realtime source: %w", err)
	}
	if src == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "realtime source not found", nil)
	}
	if !src.Enabled {
		return shared.NewDomainError(shared.ErrCodeValidation, "realtime source is disabled", nil)
	}
	if realtimestore.SinaRealtimeDisabled && src.Type == realtime.TypeSina {
		return shared.NewDomainError(shared.ErrCodeValidation, "新浪实时行情已禁用，请仅使用 tushare_proxy（内地 ts_proxy）", nil)
	}
	if realtimestore.TushareWSRealtimeDisabled && src.Type == realtime.TypeTushareWS {
		return shared.NewDomainError(shared.ErrCodeValidation, "直连 Tushare WebSocket 已禁用，请仅使用 tushare_proxy（内地 ts_proxy）", nil)
	}

	if src.Type == realtime.TypeNews {
		c.mu.Lock()
		c.newsSourceID = sourceID
		c.mu.Unlock()
		return c.startNewsPolling(sourceID)
	}

	planID, ok := realtimeSourceTypeToPlanID[src.Type]
	if !ok {
		return shared.NewDomainError(shared.ErrCodeValidation, "no sync plan mapped for source type: "+src.Type, nil)
	}

	running, err := c.syncPlanRepo.GetRunningExecutionByPlanID(shared.ID(planID))
	if err != nil {
		return fmt.Errorf("get running execution for plan %s: %w", planID, err)
	}
	if running != nil {
		logrus.Infof("[RealtimeSyncController] plan %s already running (exec %s), skip start", planID, running.ID)
		if c.selector != nil {
			c.selector.SwitchTo(src.Type)
		}
		return nil
	}

	if c.selector != nil {
		c.selector.SwitchTo(src.Type)
	}
	_, err = c.syncSvc.ExecuteSyncPlan(ctx, shared.ID(planID), contracts.ExecuteSyncPlanRequest{})
	if err != nil {
		return fmt.Errorf("execute sync plan %s: %w", planID, err)
	}
	logrus.Infof("[RealtimeSyncController] started plan %s for source %s (%s)", planID, sourceID, src.Type)
	return nil
}

// StopRealtimeSync 停止实时同步。新闻源停止轮询，行情源取消执行。
func (c *RealtimeSyncControllerImpl) StopRealtimeSync(ctx context.Context, sourceID string) error {
	src, err := c.realtimeRepo.Get(shared.ID(sourceID))
	if err != nil {
		return fmt.Errorf("get realtime source: %w", err)
	}
	if src == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "realtime source not found", nil)
	}

	if src.Type == realtime.TypeNews {
		c.stopNewsPolling()
		c.mu.Lock()
		c.newsSourceID = ""
		c.mu.Unlock()
		logrus.Infof("[RealtimeSyncController] stopped news polling for source %s", sourceID)
		return nil
	}

	planID, ok := realtimeSourceTypeToPlanID[src.Type]
	if !ok {
		return shared.NewDomainError(shared.ErrCodeValidation, "no sync plan mapped for source type: "+src.Type, nil)
	}

	running, err := c.syncPlanRepo.GetRunningExecutionByPlanID(shared.ID(planID))
	if err != nil {
		return fmt.Errorf("get running execution for plan %s: %w", planID, err)
	}
	if running == nil {
		logrus.Infof("[RealtimeSyncController] plan %s has no running execution, skip stop", planID)
		return nil
	}

	if err := c.syncSvc.CancelExecution(ctx, running.ID); err != nil {
		return fmt.Errorf("cancel execution %s: %w", running.ID, err)
	}
	logrus.Infof("[RealtimeSyncController] stopped plan %s (cancelled exec %s) for source %s", planID, running.ID, sourceID)
	return nil
}

// startNewsPolling 启动新闻轮询 goroutine，每 60 秒执行一次轻量级新闻工作流。
func (c *RealtimeSyncControllerImpl) startNewsPolling(sourceID string) error {
	c.stopNewsPolling()
	newsCtx, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	c.newsCancel = cancel
	c.newsSourceID = sourceID
	c.mu.Unlock()

	go c.newsPollingLoop(newsCtx)
	logrus.Infof("[RealtimeSyncController] started news polling (60s interval) for source %s", sourceID)
	return nil
}

// stopNewsPolling 停止新闻轮询 goroutine。
func (c *RealtimeSyncControllerImpl) stopNewsPolling() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.newsCancel != nil {
		c.newsCancel()
		c.newsCancel = nil
	}
}

// IsSourceConnected 返回该源是否处于连接/同步运行中（新闻轮询或对应 SyncPlan 正在执行）。
func (c *RealtimeSyncControllerImpl) IsSourceConnected(sourceID string) bool {
	src, err := c.realtimeRepo.Get(shared.ID(sourceID))
	if err != nil || src == nil {
		return false
	}
	if src.Type == realtime.TypeNews {
		c.mu.Lock()
		connected := c.newsCancel != nil && c.newsSourceID == sourceID
		c.mu.Unlock()
		return connected
	}
	planID, ok := realtimeSourceTypeToPlanID[src.Type]
	if !ok {
		return false
	}
	running, err := c.syncPlanRepo.GetRunningExecutionByPlanID(shared.ID(planID))
	if err != nil {
		return false
	}
	return running != nil
}

// newsPollingLoop 每 60 秒调用 ExecuteNewsRealtimeOnce 执行一次新闻拉取。
func (c *RealtimeSyncControllerImpl) newsPollingLoop(ctx context.Context) {
	for {
		start := time.Now()
		if err := c.syncSvc.ExecuteNewsRealtimeOnce(ctx); err != nil {
			logrus.Warnf("[RealtimeSyncController] news sync: %v", err)
		} else {
			logrus.Infof("[RealtimeSyncController] news sync done (%s)", time.Since(start).Round(time.Millisecond))
		}
		runtime.GC()
		select {
		case <-ctx.Done():
			return
		case <-time.After(60 * time.Second):
		}
	}
}
