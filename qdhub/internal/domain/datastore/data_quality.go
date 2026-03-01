// Package datastore contains data quality analysis value objects (data store domain).
package datastore

import "qdhub/internal/domain/shared"

// ============ 公共参数 ============

// QualityAnalysisParams 质量分析公共参数
type QualityAnalysisParams struct {
	DataStoreID  shared.ID `json:"data_store_id"`
	TableName    string    `json:"table_name"`
	DateColumn   string    `json:"date_column"`
	EntityColumn string    `json:"entity_column,omitempty"`
	PrimaryKeys  []string  `json:"primary_keys,omitempty"`
}

// ============ 维度过滤（单维度分布等复用） ============

// DimensionFilter 维度过滤条件
type DimensionFilter struct {
	ColumnName string `json:"column_name"` // 表列名
	Start     string `json:"start"`       // 闭区间起点，如 "20250101"
	End       string `json:"end"`         // 闭区间终点
}

// ============ 有效区间 ============

// EffectiveRangeRequest 有效区间分析请求
type EffectiveRangeRequest struct {
	QualityAnalysisParams
	EndDate           string `json:"end_date,omitempty"`
	RefTableName      string `json:"ref_table_name,omitempty"`
	RefDateColumn     string `json:"ref_date_column,omitempty"`
	RefDateColumnEnd  string `json:"ref_date_column_end,omitempty"` // 参考表结束日期列，如 delist_date（退市日）
}

// EffectiveRangeResult 有效区间分析结果
type EffectiveRangeResult struct {
	EarliestDate   string            `json:"earliest_date"`
	LatestDate     string            `json:"latest_date"`
	TotalDays      int               `json:"total_days"`
	PerEntityStart map[string]string `json:"per_entity_start,omitempty"`
	PerEntityEnd   map[string]string `json:"per_entity_end,omitempty"` // 按实体的有效结束日期（如退市日）
}

// ============ 缺失分析 ============

// MissingAnalysisRequest 缺失分析请求
type MissingAnalysisRequest struct {
	QualityAnalysisParams
	RefTableName     string `json:"ref_table_name,omitempty"`
	RefDateColumn    string `json:"ref_date_column,omitempty"`
	RefDateColumnEnd string `json:"ref_date_column_end,omitempty"`
}

// MissingSummary 缺失分析汇总
type MissingSummary struct {
	ExpectedCount  int64   `json:"expected_count"`
	ActualCount    int64   `json:"actual_count"`
	MissingCount   int64   `json:"missing_count"`
	MissingRatePct float64 `json:"missing_rate_pct"`
}

// MissingByDateItem 按日期统计的缺失项
type MissingByDateItem struct {
	Date         string `json:"date"`
	MissingCount int64  `json:"missing_count"`
}

// MissingSampleItem 缺失样本：一条缺失的（日期, 实体）组合，便于用户看到「具体缺了哪些」
type MissingSampleItem struct {
	Date   string `json:"date"`
	Entity string `json:"entity,omitempty"`
}

// MissingAnalysisResult 缺失分析结果
type MissingAnalysisResult struct {
	Summary        MissingSummary        `json:"summary"`
	MissingDates   []string              `json:"missing_dates,omitempty"`
	MissingByDate  []MissingByDateItem   `json:"missing_by_date,omitempty"`
	MissingSamples []MissingSampleItem   `json:"missing_samples,omitempty"`
}

// ============ 重复分析 ============

// DuplicateAnalysisRequest 重复分析请求
type DuplicateAnalysisRequest struct {
	QualityAnalysisParams
}

// DuplicateDetail 重复明细
type DuplicateDetail struct {
	KeyValues  map[string]any   `json:"key_values"`
	Count      int64            `json:"count"`
	SampleRows []map[string]any `json:"sample_rows,omitempty"`
}

// DuplicateAnalysisResult 重复分析结果
type DuplicateAnalysisResult struct {
	HasDuplicates   bool             `json:"has_duplicates"`
	DuplicateCount int64            `json:"duplicate_count"`
	DuplicateRate  float64          `json:"duplicate_rate"`
	Details         []DuplicateDetail `json:"details,omitempty"`
}

// ============ 异常值分析 ============

// AnomalyAnalysisRequest 异常值分析请求
type AnomalyAnalysisRequest struct {
	QualityAnalysisParams
}

// ColumnAnomalySummary 列级异常汇总
type ColumnAnomalySummary struct {
	ColumnName   string `json:"column_name"`
	AnomalyType  string `json:"anomaly_type"`
	AnomalyCount int64  `json:"anomaly_count"`
}

// RowAnomaly 行级异常明细
type RowAnomaly struct {
	TableName    string         `json:"table_name"`
	PrimaryKey   map[string]any `json:"primary_key"`
	ColumnName   string         `json:"column_name"`
	AnomalyType  string         `json:"anomaly_type"`
	CurrentValue any            `json:"current_value,omitempty"`
}

// AnomalyAnalysisResult 异常值分析结果
type AnomalyAnalysisResult struct {
	ColumnSummary []ColumnAnomalySummary `json:"column_summary"`
	RowDetails    []RowAnomaly           `json:"row_details,omitempty"`
	AnomalyRate   float64                `json:"anomaly_rate"`
}

// ============ 综合质量报告 ============

// QualityReportRequest 综合质量报告请求
type QualityReportRequest struct {
	QualityAnalysisParams
	RefTableName     string `json:"ref_table_name,omitempty"`
	RefDateColumn    string `json:"ref_date_column,omitempty"`
	RefDateColumnEnd string `json:"ref_date_column_end,omitempty"`
}

// QualityReport 综合质量报告
type QualityReport struct {
	DataStoreID shared.ID `json:"data_store_id"`
	TableName   string    `json:"table_name"`
	GeneratedAt shared.Timestamp `json:"generated_at"`

	OverallScore      float64 `json:"overall_score"`
	CompletenessScore float64 `json:"completeness_score"`
	UniquenessScore   float64 `json:"uniqueness_score"`
	TimelinessScore   float64 `json:"timeliness_score"`
	ValidityScore     float64 `json:"validity_score"`

	EffectiveRange *EffectiveRangeResult   `json:"effective_range"`
	Missing        *MissingAnalysisResult  `json:"missing"`
	Duplicates     *DuplicateAnalysisResult `json:"duplicates"`
	Anomalies      *AnomalyAnalysisResult   `json:"anomalies"`

	FixSuggestions []FixSuggestion `json:"fix_suggestions"`
}

// ============ 修复建议 ============

// FixType 修复类型
type FixType string

const (
	FixTypeSyncMissing      FixType = "sync_missing_data"
	FixTypeRemoveDuplicates FixType = "remove_duplicates"
	FixTypeSyncLatest       FixType = "sync_latest_data"
)

// Severity 严重程度
type Severity string

const (
	SeverityHigh   Severity = "high"
	SeverityMedium Severity = "medium"
	SeverityLow    Severity = "low"
)

// FixAction 修复动作
type FixAction struct {
	Type   string         `json:"type"`   // "create_sync_plan" | "execute_sql"
	Params map[string]any `json:"params"`
}

// FixSuggestion 修复建议
type FixSuggestion struct {
	ID          string     `json:"id"`
	Type        FixType    `json:"type"`
	Severity    Severity   `json:"severity"`
	Title       string     `json:"title"`
	Description string     `json:"description"`
	Action      *FixAction `json:"action,omitempty"`
}

// ============ 修复执行请求 ============

// ApplyFixRequest 修复执行请求
type ApplyFixRequest struct {
	DataStoreID  shared.ID `json:"data_store_id"`
	TableName    string    `json:"table_name"`
	SuggestionID string    `json:"suggestion_id"`
	FixType      FixType   `json:"fix_type"`
	Params       map[string]any `json:"params"`
}

// ============ 单维度分布（简化版） ============

// SingleDimensionStatsRequest 单维度统计请求
type SingleDimensionStatsRequest struct {
	DataStoreID shared.ID         `json:"data_store_id"`
	TableName   string            `json:"table_name"`
	Dimension   string            `json:"dimension"`
	Filter      *DimensionFilter  `json:"filter,omitempty"`
	Limit       int               `json:"limit,omitempty"`
}

// SingleDimensionStatsResult 单维度统计结果
type SingleDimensionStatsResult struct {
	Dimension string           `json:"dimension"`
	Rows      []map[string]any `json:"rows"`
	TotalRows int64            `json:"total_rows"`
	Truncated bool             `json:"truncated,omitempty"`
}
