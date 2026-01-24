// Package contracts defines application service interfaces and DTOs.
package contracts

import (
	"context"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/sync"
)

// UnitOfWork 提供应用层事务控制
// 采用函数式模式，自动管理事务生命周期（提交和回滚）
type UnitOfWork interface {
	// Do 在事务中执行操作，自动处理提交和回滚
	// 如果 fn 返回错误，事务会自动回滚；否则提交
	Do(ctx context.Context, fn func(repos Repositories) error) error
}

// Repositories 提供事务内的仓库访问
// 所有通过此接口获取的仓库实例都在同一个事务上下文中
type Repositories interface {
	// SyncPlanRepo 返回同步计划仓库（事务化）
	SyncPlanRepo() sync.SyncPlanRepository

	// DataSourceRepo 返回数据源仓库（事务化）
	DataSourceRepo() metadata.DataSourceRepository

	// DataStoreRepo 返回数据存储仓库（事务化）
	DataStoreRepo() datastore.QuantDataStoreRepository

	// MetadataRepo 返回元数据仓库（事务化）
	MetadataRepo() metadata.Repository
}
