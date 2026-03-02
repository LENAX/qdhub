// Package contracts defines data quality application service (datastore domain).
package contracts

import (
	"context"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/shared"
)

// DataQualityApplicationService 数据质量分析应用服务（独立服务，归属 datastore 领域）
// 职责：综合质量报告、4 大维度分析、单维度分布、修复建议与一键修复
type DataQualityApplicationService interface {
	// GenerateQualityReport 综合质量报告（一键生成全部分析 + 评分 + 修复建议）
	GenerateQualityReport(ctx context.Context, req datastore.QualityReportRequest) (*datastore.QualityReport, error)

	// GetEffectiveRange 有效区间分析
	GetEffectiveRange(ctx context.Context, req datastore.EffectiveRangeRequest) (*datastore.EffectiveRangeResult, error)

	// AnalyzeMissing 缺失分析
	AnalyzeMissing(ctx context.Context, req datastore.MissingAnalysisRequest) (*datastore.MissingAnalysisResult, error)

	// AnalyzeDuplicates 重复分析
	AnalyzeDuplicates(ctx context.Context, req datastore.DuplicateAnalysisRequest) (*datastore.DuplicateAnalysisResult, error)

	// AnalyzeAnomalies 异常值分析
	AnalyzeAnomalies(ctx context.Context, req datastore.AnomalyAnalysisRequest) (*datastore.AnomalyAnalysisResult, error)

	// GetSingleDimensionStats 单维度分布（简化版）
	GetSingleDimensionStats(ctx context.Context, req datastore.SingleDimensionStatsRequest) (*datastore.SingleDimensionStatsResult, error)

	// ApplyFix 修复执行（一键修复）
	ApplyFix(ctx context.Context, req datastore.ApplyFixRequest) (shared.ID, error)
}
