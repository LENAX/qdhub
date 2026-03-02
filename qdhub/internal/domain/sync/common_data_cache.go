// Package sync defines sync domain interfaces and entities.
package sync

import (
	"context"
	"time"
)

// CommonDataCache 公共数据缓存接口：按 key 缓存 API 结果，便于跨工作流复用。
// Key 建议包含：数据源名 + API 名 + 参数哈希（如 paramsHash），避免不同数据源/参数共享条目。
// 基础设施可提供内存实现（TTL 24h），后续可扩展 Redis 等。
type CommonDataCache interface {
	// Get 获取缓存数据。key 由调用方构造（如 dataSourceName + apiName + paramsHash）。
	// 返回 (data, true) 表示命中；([]map[string]any, false) 表示未命中。
	Get(ctx context.Context, key string) (data []map[string]any, ok bool)

	// Set 写入缓存。ttl 为过期时间，如 24*time.Hour。
	Set(ctx context.Context, key string, data []map[string]any, ttl time.Duration) error
}
