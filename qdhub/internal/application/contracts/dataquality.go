// Package contracts defines data quality application service (datastore domain).
package contracts

import (
	"context"

	"qdhub/internal/domain/datastore"
)

// DataQualityApplicationService 数据质量分析应用服务（独立服务，归属 datastore 领域）
// 职责：按维度统计表数据量分布、缺失分析、建议/创建同步计划补数
type DataQualityApplicationService interface {
	// GetDimensionDistribution 按选定维度统计表数据量分布
	// 返回多维结果（维度数等于请求中的维度数），便于前端热力图/条形图展示
	GetDimensionDistribution(ctx context.Context, req datastore.DimensionStatsRequest) (*datastore.DimensionStatsResult, error)
}
