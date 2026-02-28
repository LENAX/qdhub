// Package datastore contains data quality analysis value objects (data store domain).
package datastore

import "qdhub/internal/domain/shared"

// DimensionDef 维度定义（值对象）
// 用于数据质量分析：按表字段或用户给的范围作为统计维度
type DimensionDef struct {
	// Type: "column" 表示表字段维度；"range" 表示用户给的范围（作为过滤或离散轴）
	Type string `json:"type"`
	// ColumnName 表列名，当 Type=column 时必填，须在表列白名单内
	ColumnName string `json:"column_name,omitempty"`
	// RangeStart, RangeEnd 当 Type=range 时可选，如日期范围
	RangeStart string `json:"range_start,omitempty"`
	RangeEnd   string `json:"range_end,omitempty"`
}

// DimensionStatsRequest 按维度统计请求（值对象）
type DimensionStatsRequest struct {
	DataStoreID shared.ID     `json:"data_store_id"`
	TableName   string        `json:"table_name"`
	Dimensions  []DimensionDef `json:"dimensions"` // 参与 GROUP BY 的维度，仅支持 column 类型
	// Filter 可选：只统计该范围内的数据，如按 trade_date 过滤
	Filter *DimensionFilter `json:"filter,omitempty"`
}

// DimensionFilter 维度过滤条件
type DimensionFilter struct {
	ColumnName string `json:"column_name"` // 表列名
	Start     string `json:"start"`       // 闭区间起点，如 "20250101"
	End       string `json:"end"`         // 闭区间终点
}

// DimensionStatsResult 按维度统计结果（值对象）
// 返回多维数据：维度数等于用户给定的维度数，每行为一个维度组合 + count，便于前端热力图/条形图
type DimensionStatsResult struct {
	// Dimensions 维度名列表，与请求中顺序一致，如 ["trade_date", "ts_code"]
	Dimensions []string `json:"dimensions"`
	// Rows 每行包含各维度取值 + "count"，如 [{"trade_date":"20250101","ts_code":"000001.SZ","count":1}, ...]
	// 前端可据此构建 N 维数组或 2D 热力图矩阵
	Rows []map[string]any `json:"rows"`
	// TotalRows 总行数（表中参与统计的记录数）
	TotalRows int64 `json:"total_rows"`
}
