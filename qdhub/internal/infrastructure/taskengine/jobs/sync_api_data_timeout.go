package jobs

import (
	"qdhub/internal/infrastructure/datasource/tushare"

	"github.com/LENAX/task-engine/pkg/core/task"
)

// DefaultSyncAPIDataJobTimeoutSeconds 与 container 默认 task_engine 单任务超时对齐（秒）。
const DefaultSyncAPIDataJobTimeoutSeconds = 120

// EffectiveSyncAPIDataJobTimeoutSeconds 计算 SyncAPIData 任务应使用的超时（秒）：
// - configured<=0 时使用 DefaultSyncAPIDataJobTimeoutSeconds
// - 不低于 Tushare HTTP 默认超时 + 余量（排队/限流重试前缓冲）
func EffectiveSyncAPIDataJobTimeoutSeconds(configured int) int {
	min := tushare.DefaultTimeout + 30
	if configured <= 0 {
		configured = DefaultSyncAPIDataJobTimeoutSeconds
	}
	if configured < min {
		return min
	}
	return configured
}

// SyncAPIDataJobTimeoutSecondsFromContext 读取引擎注入的单任务超时（秒）；缺失时返回 Effective(0)。
func SyncAPIDataJobTimeoutSecondsFromContext(tc *task.TaskContext) int {
	if tc != nil {
		if v, ok := tc.GetDependency("SyncAPIDataJobTimeoutSeconds"); ok {
			if n, ok := v.(int); ok && n > 0 {
				return n
			}
		}
	}
	return EffectiveSyncAPIDataJobTimeoutSeconds(0)
}
