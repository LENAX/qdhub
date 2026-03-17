package realtime

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	coreRealtime "github.com/LENAX/task-engine/pkg/core/realtime"
	"github.com/sirupsen/logrus"
)

// QuotePullCollector 实现 core realtime.DataCollector，用于按固定间隔从 RealtimeAdapterRegistry
// 以 Pull 模式拉取实时行情数据（如 realtime_quote），并通过 publish(EventDataArrived, ...)
// 将数据写入 Task Engine 的内部 DataBuffer，由 Streaming Workflow 的 StreamProcessor 消费。
//
// 设计遵循 doc/note/realtime-workflow-guide.md：
// - Run(ctx, config, publish) 是一个“仅在 ctx.Done() 时退出”的长循环；
// - Pull 周期由 SyncPlan 的 pull_interval_seconds 决定（未配置时使用默认 60 秒）；
// - 每轮按策略/配置分片 ts_code，并调用 Sina/Eastmoney RealtimeAdapter.Fetch；
// - 对于网络错误/限流（如 403），本轮记录错误后跳过，等待下一轮重试，不导致 Workflow 退出。
// - 若判定为「被 ban/多 IP 限制」（IsRealtimeBanError），则 Run 返回该错误，主动终止工作流（由引擎将工作流置为失败并停止）。
// - 正常运行期间每 3 秒打印一次诊断：工作状态与已获取数据条数。
type QuotePullCollector struct {
	// 基本配置：由 SyncPlan/执行层在构造时注入
	DataSourceName   string
	Token            string
	TargetDBPath     string
	APINames         []string
	TsCodes          []string
	IndexCodes       []string
	PullIntervalSecs int

	// 实时 adapter 注册表（sina/eastmoney）
	AdapterRegistry RealtimeAdapterRegistry

	// 目前简化：仅支持 src=sina，后续可扩展或从策略注入
	Sources []string

	// 诊断与 ban 检测（Run 生命周期内使用）
	mu       sync.Mutex
	banErr   error   // 发生 ban 时写入，Run 读后返回
	totalRows int64  // 本轮已获取数据条数，atomic
}

// Ensure QuotePullCollector implements core realtime.DataCollector.
var _ coreRealtime.DataCollector = (*QuotePullCollector)(nil)

const diagnosticInterval = 3 * time.Second

// Run 以 Pull 方式持续拉取实时行情。
// 注意：为了控制同步范围与批量策略，本实现当前只支持 realtime_quote 且使用简单的 ts_code 分片（最多 50 个一组）。
// 若发生被 ban/多 IP 限制类错误，返回该错误以取消工作流；正常运行每 3 秒打印一次工作状态与已获取数据条数。
func (c *QuotePullCollector) Run(
	ctx context.Context,
	cfg *coreRealtime.ContinuousTaskConfig,
	publish coreRealtime.PublishFunc,
) error {
	if len(c.APINames) == 0 {
		return nil
	}
	interval := c.effectiveInterval(cfg)
	sources := c.effectiveSources()

	// 诊断：每 3 秒打印工作状态与已获取数据条数
	ticker := time.NewTicker(diagnosticInterval)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n := atomic.LoadInt64(&c.totalRows)
				logrus.Infof("[RealtimeMarketStreaming] 工作状态=running 已获取数据条数=%d", n)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		for _, apiName := range c.APINames {
			switch apiName {
			case "realtime_quote":
				c.pullRealtimeQuoteOnce(ctx, apiName, sources, publish)
			case "realtime_list":
				c.pullRealtimeListOnce(ctx, apiName, sources, publish)
			case "realtime_tick":
				c.pullRealtimeTickOnce(ctx, apiName, sources, publish)
			default:
			}
			// 一旦出现 ban 立即终止，不再执行同轮其他 API
			c.mu.Lock()
			banErr := c.banErr
			c.mu.Unlock()
			if banErr != nil {
				return fmt.Errorf("实时行情接口被限制，工作流已取消: %w", banErr)
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}
	}
}

func (c *QuotePullCollector) effectiveInterval(cfg *coreRealtime.ContinuousTaskConfig) time.Duration {
	secs := c.PullIntervalSecs
	if cfg != nil && cfg.FlushInterval > 0 {
		secs = int(cfg.FlushInterval.Seconds())
	}
	if secs <= 0 {
		secs = 60
	}
	return time.Duration(secs) * time.Second
}

func (c *QuotePullCollector) effectiveSources() []string {
	if len(c.Sources) > 0 {
		return c.Sources
	}
	// 默认仅使用 sina，后续可结合策略/配置扩展 eastmoney 等
	return []string{"sina"}
}

// pullRealtimeQuoteOnce 针对单次循环执行 realtime_quote 的拉取与 publish。
func (c *QuotePullCollector) pullRealtimeQuoteOnce(
	ctx context.Context,
	apiName string,
	sources []string,
	publish coreRealtime.PublishFunc,
) {
	if c.AdapterRegistry == nil {
		return
	}
	if len(c.TsCodes) == 0 {
		return
	}

	// 简单按照 50 个 ts_code 一组分片，避免单次请求过大。
	const maxSymbolsPerRequest = 50
	chunks := chunkStrings(c.TsCodes, maxSymbolsPerRequest)

	for _, src := range sources {
		adapter, ok := c.AdapterRegistry.Get(src)
		if !ok {
			continue
		}
		if !adapter.Supports(apiName) {
			continue
		}
		for _, chunk := range chunks {
			select {
			case <-ctx.Done():
				return
			default:
			}

			params := map[string]interface{}{
				"ts_code": strings.Join(chunk, ","),
				"src":     src,
			}
			data, err := adapter.Fetch(ctx, apiName, params)
			if err != nil {
				if IsRealtimeBanError(err) {
					c.mu.Lock()
					c.banErr = err
					c.mu.Unlock()
					return
				}
				continue
			}
			if len(data) == 0 {
				continue
			}
			atomic.AddInt64(&c.totalRows, int64(len(data)))
			// 将目标库路径附加到每行数据，供 Streaming DataHandler 在缺少 task 参数时推断 target_db_path。
			if c.TargetDBPath != "" {
				for _, row := range data {
					if _, ok := row["target_db_path"]; !ok {
						row["target_db_path"] = c.TargetDBPath
					}
				}
			}
			logrus.Infof("[QuotePullCollector] api=%s src=%s batch=%d rows=%d target_db_path_sample=%v",
				apiName, src, len(chunks), len(data), func() interface{} {
					if len(data) > 0 {
						return data[0]["target_db_path"]
					}
					return nil
				}())

			event := coreRealtime.NewRealtimeEvent(coreRealtime.EventDataArrived, "", "", &coreRealtime.DataArrivedPayload{
				Data:   data,
				Source: src,
			})
			_ = publish(event) // 由引擎负责错误处理与指标计数
		}
	}
}

// pullRealtimeListOnce 针对单次循环执行 realtime_list 的拉取与 publish（全市场快照）。
func (c *QuotePullCollector) pullRealtimeListOnce(
	ctx context.Context,
	apiName string,
	sources []string,
	publish coreRealtime.PublishFunc,
) {
	if c.AdapterRegistry == nil {
		return
	}

	for _, src := range sources {
		adapter, ok := c.AdapterRegistry.Get(src)
		if !ok {
			continue
		}
		if !adapter.Supports(apiName) {
			continue
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		params := map[string]interface{}{
			"src": src,
		}
		data, err := adapter.Fetch(ctx, apiName, params)
		if err != nil {
			if IsRealtimeBanError(err) {
				c.mu.Lock()
				c.banErr = err
				c.mu.Unlock()
				return
			}
			continue
		}
		if len(data) == 0 {
			continue
		}
		atomic.AddInt64(&c.totalRows, int64(len(data)))
		if c.TargetDBPath != "" {
			for _, row := range data {
				if _, ok := row["target_db_path"]; !ok {
					row["target_db_path"] = c.TargetDBPath
				}
			}
		}
		logrus.Infof("[QuotePullCollector] api=%s src=%s rows=%d target_db_path_sample=%v",
			apiName, src, len(data), func() interface{} {
				if len(data) > 0 {
					return data[0]["target_db_path"]
				}
				return nil
			}())

		event := coreRealtime.NewRealtimeEvent(coreRealtime.EventDataArrived, "", "", &coreRealtime.DataArrivedPayload{
			Data:   data,
			Source: src,
		})
		_ = publish(event)
	}
}

// chunkStrings 按 size 将切片分段。
func chunkStrings(slice []string, size int) [][]string {
	if size <= 0 || len(slice) == 0 {
		return [][]string{slice}
	}
	var result [][]string
	for i := 0; i < len(slice); i += size {
		end := i + size
		if end > len(slice) {
			end = len(slice)
		}
		result = append(result, slice[i:end])
	}
	return result
}

// pullRealtimeTickOnce 针对单次循环执行 realtime_tick 的拉取与 publish（按单码请求）。
func (c *QuotePullCollector) pullRealtimeTickOnce(
	ctx context.Context,
	apiName string,
	sources []string,
	publish coreRealtime.PublishFunc,
) {
	if c.AdapterRegistry == nil {
		return
	}
	if len(c.TsCodes) == 0 {
		return
	}

	for _, src := range sources {
		adapter, ok := c.AdapterRegistry.Get(src)
		if !ok {
			continue
		}
		if !adapter.Supports(apiName) {
			continue
		}
		for _, code := range c.TsCodes {
			select {
			case <-ctx.Done():
				return
			default:
			}

			code = strings.TrimSpace(code)
			if code == "" {
				continue
			}

			params := map[string]interface{}{
				"ts_code": code,
				"src":     src,
			}
			data, err := adapter.Fetch(ctx, apiName, params)
			if err != nil {
				if IsRealtimeBanError(err) {
					c.mu.Lock()
					c.banErr = err
					c.mu.Unlock()
					return
				}
				continue
			}
			if len(data) == 0 {
				continue
			}
			atomic.AddInt64(&c.totalRows, int64(len(data)))
			if c.TargetDBPath != "" {
				for _, row := range data {
					if _, ok := row["target_db_path"]; !ok {
						row["target_db_path"] = c.TargetDBPath
					}
				}
			}
			logrus.Infof("[QuotePullCollector] api=%s src=%s ts_code=%s rows=%d target_db_path_sample=%v",
				apiName, src, code, len(data), func() interface{} {
					if len(data) > 0 {
						return data[0]["target_db_path"]
					}
					return nil
				}())

			event := coreRealtime.NewRealtimeEvent(coreRealtime.EventDataArrived, "", "", &coreRealtime.DataArrivedPayload{
				Data:   data,
				Source: src,
			})
			_ = publish(event)
		}
	}
}

// TickPushCollector 使用 RealtimeAdapter.StartStream 以 Push（SSE）模式持续消费东财 realtime_tick 分时明细。
// 与 QuotePullCollector 不同，TickPushCollector 不再按间隔轮询，而是依赖 SSE 长连接有数据即推。
type TickPushCollector struct {
	DataSourceName string
	Token          string
	TargetDBPath   string

	// 订阅的股票代码列表；通常由 SyncPlan 解析自 stock_basic。
	TsCodes []string

	// AdapterRegistry 提供 eastmoney 实时适配器。
	AdapterRegistry RealtimeAdapterRegistry
}

// Ensure TickPushCollector implements core realtime.DataCollector.
var _ coreRealtime.DataCollector = (*TickPushCollector)(nil)

// Run 建立一个或多个 SSE 连接，并在 ctx 生命周期内持续从东财消费分笔数据。
func (c *TickPushCollector) Run(
	ctx context.Context,
	cfg *coreRealtime.ContinuousTaskConfig,
	publish coreRealtime.PublishFunc,
) error {
	if c.AdapterRegistry == nil {
		return nil
	}
	if len(c.TsCodes) == 0 {
		return nil
	}

	adapter, ok := c.AdapterRegistry.Get(eastmoneySource)
	if !ok {
		return nil
	}
	if !adapter.Supports("realtime_tick") || !adapter.SupportsPush("realtime_tick") {
		return nil
	}

	// 简单起见：限制同时启动的 SSE 连接数量，避免过多连接压力。
	const maxStreams = 8
	codes := c.TsCodes
	if len(codes) > maxStreams {
		codes = codes[:maxStreams]
	}

	var wg sync.WaitGroup
	for _, code := range codes {
		code = strings.TrimSpace(code)
		if code == "" {
			continue
		}
		wg.Add(1)
		go func(tsCode string) {
			defer wg.Done()
			params := map[string]interface{}{
				"ts_code": tsCode,
				"src":     eastmoneySource,
			}
			_ = adapter.StartStream(ctx, "realtime_tick", params, func(data []map[string]interface{}) error {
				if len(data) == 0 {
					return nil
				}
				if c.TargetDBPath != "" {
					for _, row := range data {
						if _, ok := row["target_db_path"]; !ok {
							row["target_db_path"] = c.TargetDBPath
						}
					}
				}
				logrus.Infof("[TickPushCollector] api=realtime_tick src=%s ts_code=%s rows=%d target_db_path_sample=%v",
					eastmoneySource, tsCode, len(data), func() interface{} {
						if len(data) > 0 {
							return data[0]["target_db_path"]
						}
						return nil
					}())

				event := coreRealtime.NewRealtimeEvent(coreRealtime.EventDataArrived, "", "", &coreRealtime.DataArrivedPayload{
					Data:   data,
					Source: eastmoneySource,
				})
				return publish(event)
			})
		}(code)
	}

	<-ctx.Done()
	wg.Wait()
	return nil
}

// NOTE: 事件中的 taskID/instanceID 由 Task Engine 在内部补齐，此处使用空字符串占位。

// NOTE: 事件中的 taskID/instanceID 由 Task Engine 在内部补齐，此处使用空字符串占位。
