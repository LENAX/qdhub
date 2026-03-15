// Package impl contains data quality application service implementation.
package impl

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
)

// DataQualityApplicationServiceImpl implements DataQualityApplicationService.
type DataQualityApplicationServiceImpl struct {
	dataStoreRepo  datastore.QuantDataStoreRepository
	syncPlanRepo   sync.SyncPlanRepository
	quantDBAdapter QuantDBAdapter
	syncSvc        contracts.SyncApplicationService
}

// NewDataQualityApplicationService creates a new DataQualityApplicationService implementation.
func NewDataQualityApplicationService(
	dataStoreRepo datastore.QuantDataStoreRepository,
	syncPlanRepo sync.SyncPlanRepository,
	quantDBAdapter QuantDBAdapter,
	syncSvc contracts.SyncApplicationService,
) contracts.DataQualityApplicationService {
	return &DataQualityApplicationServiceImpl{
		dataStoreRepo:  dataStoreRepo,
		syncPlanRepo:   syncPlanRepo,
		quantDBAdapter: quantDBAdapter,
		syncSvc:        syncSvc,
	}
}

// GenerateQualityReport 综合质量报告
func (s *DataQualityApplicationServiceImpl) GenerateQualityReport(ctx context.Context, req datastore.QualityReportRequest) (*datastore.QualityReport, error) {
	ds, err := s.resolveDataStore(ctx, req.DataStoreID, req.TableName)
	if err != nil {
		return nil, err
	}

	report := &datastore.QualityReport{
		DataStoreID: req.DataStoreID,
		TableName:   req.TableName,
		GeneratedAt: shared.Now(),
	}

	// 1. 有效区间
	rangeReq := datastore.EffectiveRangeRequest{
		QualityAnalysisParams: req.QualityAnalysisParams,
		RefTableName:          req.RefTableName,
		RefDateColumn:         req.RefDateColumn,
		RefDateColumnEnd:      req.RefDateColumnEnd,
	}
	rangeResult, err := s.GetEffectiveRange(ctx, rangeReq)
	if err != nil {
		return nil, fmt.Errorf("effective range: %w", err)
	}
	report.EffectiveRange = rangeResult

	// 2. 缺失分析
	missingReq := datastore.MissingAnalysisRequest{
		QualityAnalysisParams: req.QualityAnalysisParams,
		RefTableName:          req.RefTableName,
		RefDateColumn:         req.RefDateColumn,
		RefDateColumnEnd:      req.RefDateColumnEnd,
	}
	missingResult, err := s.AnalyzeMissing(ctx, missingReq)
	if err != nil {
		return nil, fmt.Errorf("missing analysis: %w", err)
	}
	report.Missing = missingResult

	// 3. 重复分析
	dupReq := datastore.DuplicateAnalysisRequest{QualityAnalysisParams: req.QualityAnalysisParams}
	dupResult, err := s.AnalyzeDuplicates(ctx, dupReq)
	if err != nil {
		return nil, fmt.Errorf("duplicate analysis: %w", err)
	}
	report.Duplicates = dupResult

	// 4. 异常值分析
	anomReq := datastore.AnomalyAnalysisRequest{QualityAnalysisParams: req.QualityAnalysisParams}
	anomResult, err := s.AnalyzeAnomalies(ctx, anomReq)
	if err != nil {
		return nil, fmt.Errorf("anomaly analysis: %w", err)
	}
	report.Anomalies = anomResult

	// 5. 计算各维度分数
	report.CompletenessScore = s.calcCompletenessScore(missingResult)
	report.UniquenessScore = s.calcUniquenessScore(dupResult)
	report.TimelinessScore = s.calcTimelinessScore(rangeResult)
	report.ValidityScore = s.calcValidityScore(anomResult)

	// 6. 加权总分
	report.OverallScore = 0.40*report.CompletenessScore + 0.25*report.UniquenessScore +
		0.20*report.TimelinessScore + 0.15*report.ValidityScore

	// 7. 生成修复建议
	report.FixSuggestions = s.generateFixSuggestions(report, ds, req.PrimaryKeys)
	return report, nil
}

// GetEffectiveRange 有效区间分析
func (s *DataQualityApplicationServiceImpl) GetEffectiveRange(ctx context.Context, req datastore.EffectiveRangeRequest) (*datastore.EffectiveRangeResult, error) {
	ds, err := s.resolveDataStore(ctx, req.DataStoreID, req.TableName)
	if err != nil {
		return nil, err
	}
	if req.DateColumn == "" {
		return nil, shared.NewDomainError(shared.ErrCodeValidation, "date_column is required", nil)
	}

	qt := quoteIdentifierDQ(req.TableName)
	dc := quoteIdentifierDQ(req.DateColumn)

	// 表级：最早、最晚、总天数
	sql := fmt.Sprintf("SELECT MIN(%s) as earliest, MAX(%s) as latest, COUNT(DISTINCT %s) as total_days FROM %s",
		dc, dc, dc, qt)
	var rangeArgs []any
	if req.EndDate != "" {
		sql += fmt.Sprintf(" WHERE %s <= ?", dc)
		rangeArgs = append(rangeArgs, req.EndDate)
	}
	var rows []map[string]any
	if len(rangeArgs) > 0 {
		rows, err = s.quantDBAdapter.Query(ctx, ds, sql, rangeArgs...)
	} else {
		rows, err = s.quantDBAdapter.Query(ctx, ds, sql)
	}
	if err != nil {
		return nil, fmt.Errorf("query effective range: %w", err)
	}
	if len(rows) == 0 {
		return &datastore.EffectiveRangeResult{
			EarliestDate: "",
			LatestDate:   "",
			TotalDays:    0,
		}, nil
	}

	r := rows[0]
	earliest := fmtVal(r["earliest"])
	latest := fmtVal(r["latest"])
	totalDays := 0
	if n, ok := r["total_days"]; ok {
		totalDays = int(toInt64(n))
	}

	result := &datastore.EffectiveRangeResult{
		EarliestDate: earliest,
		LatestDate:   latest,
		TotalDays:    totalDays,
	}

	// 按实体起点/终点（有 entity_column 且 ref_table 时）
	if req.EntityColumn != "" && req.RefTableName != "" && req.RefDateColumn != "" {
		ec := quoteIdentifierDQ(req.EntityColumn)
		rt := quoteIdentifierDQ(req.RefTableName)
		rdc := quoteIdentifierDQ(req.RefDateColumn)
		selectCols := fmt.Sprintf("t.%s as entity, COALESCE(MAX(r.%s), MIN(t.%s)) as effective_start", ec, rdc, dc)
		if req.RefDateColumnEnd != "" {
			rdcEnd := quoteIdentifierDQ(req.RefDateColumnEnd)
			selectCols += fmt.Sprintf(", MAX(r.%s) as effective_end", rdcEnd)
		}
		sql = fmt.Sprintf(`SELECT %s FROM %s t LEFT JOIN %s r ON t.%s = r.%s GROUP BY t.%s`,
			selectCols, qt, rt, ec, ec, ec)
		perRows, err := s.quantDBAdapter.Query(ctx, ds, sql)
		if err == nil && len(perRows) > 0 {
			result.PerEntityStart = make(map[string]string)
			if req.RefDateColumnEnd != "" {
				result.PerEntityEnd = make(map[string]string)
			}
			for _, pr := range perRows {
				entity := fmtVal(pr["entity"])
				start := fmtVal(pr["effective_start"])
				if entity != "" && start != "" {
					result.PerEntityStart[entity] = start
				}
				if req.RefDateColumnEnd != "" {
					if end := fmtVal(pr["effective_end"]); entity != "" && end != "" {
						result.PerEntityEnd[entity] = end
					}
				}
			}
		}
	}
	return result, nil
}

// AnalyzeMissing 缺失分析
func (s *DataQualityApplicationServiceImpl) AnalyzeMissing(ctx context.Context, req datastore.MissingAnalysisRequest) (*datastore.MissingAnalysisResult, error) {
	ds, err := s.resolveDataStore(ctx, req.DataStoreID, req.TableName)
	if err != nil {
		return nil, err
	}
	if req.DateColumn == "" {
		return nil, shared.NewDomainError(shared.ErrCodeValidation, "date_column is required", nil)
	}

	qt := quoteIdentifierDQ(req.TableName)
	dc := quoteIdentifierDQ(req.DateColumn)

	// 从数据推断：所有日期 x 所有实体 = 期望数
	// 无 entity 时：期望 = 去重日期数，实际 = 总行数
	if req.EntityColumn == "" {
		sql := fmt.Sprintf(`
			WITH dates AS (SELECT DISTINCT %s as d FROM %s),
			     cnt AS (SELECT COUNT(*) as c FROM %s)
			SELECT (SELECT COUNT(*) FROM dates) as expected_count, (SELECT c FROM cnt) as actual_count`,
			dc, qt, qt)
		rows, err := s.quantDBAdapter.Query(ctx, ds, sql)
		if err != nil {
			return nil, fmt.Errorf("query missing: %w", err)
		}
		if len(rows) == 0 {
			return &datastore.MissingAnalysisResult{Summary: datastore.MissingSummary{}}, nil
		}
		r := rows[0]
		expected := toInt64(r["expected_count"])
		actual := toInt64(r["actual_count"])
		missing := expected - actual
		if missing < 0 {
			missing = 0
		}
		rate := 0.0
		if expected > 0 {
			rate = 100 * float64(missing) / float64(expected)
		}
		return &datastore.MissingAnalysisResult{
			Summary: datastore.MissingSummary{
				ExpectedCount:  expected,
				ActualCount:    actual,
				MissingCount:   missing,
				MissingRatePct: rate,
			},
		}, nil
	}

	ec := quoteIdentifierDQ(req.EntityColumn)
	// 有参考表+参考表日期列时：只对「日期在 [有效起点, 有效终点] 内」的 (日期, 实体) 计为期望（未上市前、退市后不算缺失）
	useRefTable := req.RefTableName != "" && req.RefDateColumn != ""
	useRefEnd := useRefTable && req.RefDateColumnEnd != ""
	var sql string
	if useRefTable {
		rt := quoteIdentifierDQ(req.RefTableName)
		rdc := quoteIdentifierDQ(req.RefDateColumn)
		entityRangeSelect := fmt.Sprintf("SELECT t.%s as entity, COALESCE(MAX(r.%s), MIN(t.%s)) as effective_start", ec, rdc, dc)
		if useRefEnd {
			rdcEnd := quoteIdentifierDQ(req.RefDateColumnEnd)
			entityRangeSelect += fmt.Sprintf(", MAX(r.%s) as effective_end", rdcEnd)
		}
		dateFilter := "d.d >= e.effective_start"
		if useRefEnd {
			dateFilter += " AND (e.effective_end IS NULL OR d.d <= e.effective_end)"
		}
		sql = fmt.Sprintf(`
			WITH dates AS (SELECT DISTINCT %s as d FROM %s),
			     entity_ranges AS (
			       %s
			       FROM %s t LEFT JOIN %s r ON t.%s = r.%s
			       GROUP BY t.%s
			     ),
			     all_combos AS (
			       SELECT d.d as date_val, e.entity as entity_val
			       FROM dates d
			       INNER JOIN entity_ranges e ON %s
			     ),
			     actual AS (SELECT %s as date_val, %s as entity_val FROM %s)
			SELECT (SELECT COUNT(*) FROM all_combos) as expected_count, (SELECT COUNT(*) FROM actual) as actual_count`,
			dc, qt, entityRangeSelect, qt, rt, ec, ec, ec, dateFilter, dc, ec, qt)
	} else {
		sql = fmt.Sprintf(`
			WITH all_combos AS (
				SELECT d.d as date_val, e.e as entity_val
				FROM (SELECT DISTINCT %s as d FROM %s) d
				CROSS JOIN (SELECT DISTINCT %s as e FROM %s) e
			),
			actual AS (SELECT %s as date_val, %s as entity_val FROM %s)
			SELECT (SELECT COUNT(*) FROM all_combos) as expected_count, (SELECT COUNT(*) FROM actual) as actual_count`,
			dc, qt, ec, qt, dc, ec, qt)
	}
	rows, err := s.quantDBAdapter.Query(ctx, ds, sql)
	if err != nil {
		return nil, fmt.Errorf("query missing: %w", err)
	}
	if len(rows) == 0 {
		return &datastore.MissingAnalysisResult{Summary: datastore.MissingSummary{}}, nil
	}
	r := rows[0]
	expected := toInt64(r["expected_count"])
	actual := toInt64(r["actual_count"])
	missing := expected - actual
	if missing < 0 {
		missing = 0
	}
	rate := 0.0
	if expected > 0 {
		rate = 100 * float64(missing) / float64(expected)
	}
	res := &datastore.MissingAnalysisResult{
		Summary: datastore.MissingSummary{
			ExpectedCount:  expected,
			ActualCount:    actual,
			MissingCount:   missing,
			MissingRatePct: rate,
		},
	}
	// 返回缺失样本：期望组合中不在实际数据里的 (date, entity)，最多 500 条；有参考表时已按有效区间过滤
	if missing > 0 {
		var sqlMissing string
		if useRefTable {
			rt := quoteIdentifierDQ(req.RefTableName)
			rdc := quoteIdentifierDQ(req.RefDateColumn)
			entityRangeSelect := fmt.Sprintf("SELECT t.%s as entity, COALESCE(MAX(r.%s), MIN(t.%s)) as effective_start", ec, rdc, dc)
			dateFilter := "d.d >= e.effective_start"
			if useRefEnd {
				rdcEnd := quoteIdentifierDQ(req.RefDateColumnEnd)
				entityRangeSelect += fmt.Sprintf(", MAX(r.%s) as effective_end", rdcEnd)
				dateFilter += " AND (e.effective_end IS NULL OR d.d <= e.effective_end)"
			}
			sqlMissing = fmt.Sprintf(`
				WITH dates AS (SELECT DISTINCT %s as d FROM %s),
				     entity_ranges AS (
				       %s
				       FROM %s t LEFT JOIN %s r ON t.%s = r.%s
				       GROUP BY t.%s
				     ),
				     all_combos AS (
				       SELECT d.d as date_val, e.entity as entity_val
				       FROM dates d INNER JOIN entity_ranges e ON %s
				     ),
				     actual AS (SELECT %s as date_val, %s as entity_val FROM %s)
				SELECT ac.date_val, ac.entity_val FROM all_combos ac
				LEFT JOIN actual a ON ac.date_val = a.date_val AND ac.entity_val = a.entity_val
				WHERE a.date_val IS NULL
				LIMIT 500`, dc, qt, entityRangeSelect, qt, rt, ec, ec, ec, dateFilter, dc, ec, qt)
		} else {
			sqlMissing = fmt.Sprintf(`
				WITH all_combos AS (
					SELECT d.d as date_val, e.e as entity_val
					FROM (SELECT DISTINCT %s as d FROM %s) d
					CROSS JOIN (SELECT DISTINCT %s as e FROM %s) e
				),
				actual AS (SELECT %s as date_val, %s as entity_val FROM %s)
				SELECT ac.date_val, ac.entity_val FROM all_combos ac
				LEFT JOIN actual a ON ac.date_val = a.date_val AND ac.entity_val = a.entity_val
				WHERE a.date_val IS NULL
				LIMIT 500`, dc, qt, ec, qt, dc, ec, qt)
		}
		missingRows, err := s.quantDBAdapter.Query(ctx, ds, sqlMissing)
		if err == nil {
			for _, row := range missingRows {
				res.MissingSamples = append(res.MissingSamples, datastore.MissingSampleItem{
					Date:   fmtVal(row["date_val"]),
					Entity: fmtVal(row["entity_val"]),
				})
			}
		}
	}
	return res, nil
}

// AnalyzeDuplicates 重复分析
func (s *DataQualityApplicationServiceImpl) AnalyzeDuplicates(ctx context.Context, req datastore.DuplicateAnalysisRequest) (*datastore.DuplicateAnalysisResult, error) {
	ds, err := s.resolveDataStore(ctx, req.DataStoreID, req.TableName)
	if err != nil {
		return nil, err
	}
	if len(req.PrimaryKeys) == 0 {
		return nil, shared.NewDomainError(shared.ErrCodeValidation, "primary_keys is required for duplicate analysis", nil)
	}

	qt := quoteIdentifierDQ(req.TableName)
	pkCols := make([]string, len(req.PrimaryKeys))
	for i, pk := range req.PrimaryKeys {
		pkCols[i] = quoteIdentifierDQ(pk)
	}
	groupBy := strings.Join(pkCols, ", ")

	sql := fmt.Sprintf(`
		SELECT %s, COUNT(*) as cnt FROM %s GROUP BY %s HAVING COUNT(*) > 1 ORDER BY cnt DESC LIMIT 100`,
		groupBy, qt, groupBy)
	rows, err := s.quantDBAdapter.Query(ctx, ds, sql)
	if err != nil {
		return nil, fmt.Errorf("query duplicates: %w", err)
	}

	// 总行数
	countSQL := fmt.Sprintf("SELECT COUNT(*) as c FROM %s", qt)
	countRows, _ := s.quantDBAdapter.Query(ctx, ds, countSQL)
	totalRows := int64(0)
	if len(countRows) > 0 {
		totalRows = toInt64(countRows[0]["c"])
	}

	details := make([]datastore.DuplicateDetail, 0, len(rows))
	var duplicateRecords int64
	for _, r := range rows {
		cnt := toInt64(r["cnt"])
		duplicateRecords += cnt
		kv := make(map[string]any)
		for _, pk := range req.PrimaryKeys {
			if v, ok := r[pk]; ok {
				kv[pk] = v
			}
		}
		details = append(details, datastore.DuplicateDetail{KeyValues: kv, Count: cnt})
	}
	// 每组重复主键取最多 5 条样本行，最多查 20 组
	const maxDetailSamples = 20
	const sampleRowsPerKey = 5
	for i := 0; i < len(details) && i < maxDetailSamples; i++ {
		d := &details[i]
		conds := make([]string, len(req.PrimaryKeys))
		args := make([]any, len(req.PrimaryKeys))
		for j, pk := range req.PrimaryKeys {
			conds[j] = quoteIdentifierDQ(pk) + "=?"
			if v, ok := d.KeyValues[pk]; ok {
				args[j] = v
			}
		}
		sampleSQL := fmt.Sprintf("SELECT * FROM %s WHERE %s LIMIT %d", qt, strings.Join(conds, " AND "), sampleRowsPerKey)
		sampleRows, err := s.quantDBAdapter.Query(ctx, ds, sampleSQL, args...)
		if err != nil {
			continue
		}
		d.SampleRows = sampleRows
	}

	dupRate := 0.0
	if totalRows > 0 {
		dupRate = 100 * float64(duplicateRecords) / float64(totalRows)
	}
	return &datastore.DuplicateAnalysisResult{
		HasDuplicates:  len(rows) > 0,
		DuplicateCount: int64(len(rows)),
		DuplicateRate:  dupRate,
		Details:        details,
	}, nil
}

// AnalyzeAnomalies 异常值分析（NULL 检测）
func (s *DataQualityApplicationServiceImpl) AnalyzeAnomalies(ctx context.Context, req datastore.AnomalyAnalysisRequest) (*datastore.AnomalyAnalysisResult, error) {
	ds, err := s.resolveDataStore(ctx, req.DataStoreID, req.TableName)
	if err != nil {
		return nil, err
	}

	cols, err := s.getTableColumnNames(ctx, ds, req.TableName)
	if err != nil {
		return nil, err
	}

	qt := quoteIdentifierDQ(req.TableName)
	countSQL := fmt.Sprintf("SELECT COUNT(*) as c FROM %s", qt)
	countRows, _ := s.quantDBAdapter.Query(ctx, ds, countSQL)
	totalRows := int64(0)
	if len(countRows) > 0 {
		totalRows = toInt64(countRows[0]["c"])
	}
	if totalRows == 0 {
		return &datastore.AnomalyAnalysisResult{
			ColumnSummary: nil,
			AnomalyRate:   0,
		}, nil
	}

	var columnSummary []datastore.ColumnAnomalySummary
	var totalAnomalies int64
	for _, col := range cols {
		qc := quoteIdentifierDQ(col)
		sql := fmt.Sprintf("SELECT COUNT(*) - COUNT(%s) as null_count FROM %s", qc, qt)
		rows, err := s.quantDBAdapter.Query(ctx, ds, sql)
		if err != nil {
			continue
		}
		if len(rows) == 0 {
			continue
		}
		nullCount := toInt64(rows[0]["null_count"])
		if nullCount > 0 {
			columnSummary = append(columnSummary, datastore.ColumnAnomalySummary{
				ColumnName:   col,
				AnomalyType:  "null",
				AnomalyCount: nullCount,
			})
			totalAnomalies += nullCount
		}
	}

	anomalyRate := 0.0
	totalCells := totalRows * int64(len(cols))
	if totalCells > 0 {
		anomalyRate = 100 * float64(totalAnomalies) / float64(totalCells)
	}
	res := &datastore.AnomalyAnalysisResult{
		ColumnSummary: columnSummary,
		AnomalyRate:   anomalyRate,
	}
	// 行级异常明细：每列 NULL 采样，主键 + 列名 + 异常类型，最多 200 条
	const maxRowDetails = 200
	if len(req.PrimaryKeys) > 0 && len(columnSummary) > 0 {
		pkCols := make([]string, len(req.PrimaryKeys))
		for j, pk := range req.PrimaryKeys {
			pkCols[j] = quoteIdentifierDQ(pk)
		}
		selectPk := strings.Join(pkCols, ", ")
		for _, cs := range columnSummary {
			if cs.AnomalyType != "null" {
				continue
			}
			qc := quoteIdentifierDQ(cs.ColumnName)
			sql := fmt.Sprintf("SELECT %s, %s as _cur FROM %s WHERE %s IS NULL LIMIT 50", selectPk, qc, qt, qc)
			anomRows, err := s.quantDBAdapter.Query(ctx, ds, sql)
			if err != nil {
				continue
			}
			for _, row := range anomRows {
				pkMap := make(map[string]any)
				for _, pk := range req.PrimaryKeys {
					if v, ok := row[pk]; ok {
						pkMap[pk] = v
					}
				}
				res.RowDetails = append(res.RowDetails, datastore.RowAnomaly{
					TableName:    req.TableName,
					PrimaryKey:   pkMap,
					ColumnName:   cs.ColumnName,
					AnomalyType:  "null",
					CurrentValue: row["_cur"],
				})
				if len(res.RowDetails) >= maxRowDetails {
					break
				}
			}
			if len(res.RowDetails) >= maxRowDetails {
				break
			}
		}
	}
	return res, nil
}

// GetSingleDimensionStats 单维度分布
func (s *DataQualityApplicationServiceImpl) GetSingleDimensionStats(ctx context.Context, req datastore.SingleDimensionStatsRequest) (*datastore.SingleDimensionStatsResult, error) {
	ds, err := s.resolveDataStore(ctx, req.DataStoreID, req.TableName)
	if err != nil {
		return nil, err
	}
	if req.Dimension == "" {
		return nil, shared.NewDomainError(shared.ErrCodeValidation, "dimension is required", nil)
	}

	cols, err := s.getTableColumnNames(ctx, ds, req.TableName)
	if err != nil {
		return nil, err
	}
	colSet := make(map[string]bool)
	for _, c := range cols {
		colSet[c] = true
	}
	if !colSet[req.Dimension] {
		return nil, shared.NewDomainError(shared.ErrCodeValidation, "dimension column not in table: "+req.Dimension, nil)
	}

	qt := quoteIdentifierDQ(req.TableName)
	dim := quoteIdentifierDQ(req.Dimension)
	sql := fmt.Sprintf("SELECT %s, COUNT(*) AS count FROM %s", dim, qt)
	var args []any
	if req.Filter != nil && req.Filter.ColumnName != "" && colSet[req.Filter.ColumnName] {
		fc := quoteIdentifierDQ(req.Filter.ColumnName)
		sql += fmt.Sprintf(" WHERE %s >= ? AND %s <= ?", fc, fc)
		args = append(args, req.Filter.Start, req.Filter.End)
	}
	sql += fmt.Sprintf(" GROUP BY %s ORDER BY count DESC", dim)
	limit := req.Limit
	if limit <= 0 {
		limit = 20000
	}
	sql += fmt.Sprintf(" LIMIT %d", limit+1)

	var rows []map[string]any
	if len(args) > 0 {
		rows, err = s.quantDBAdapter.Query(ctx, ds, sql, args...)
	} else {
		rows, err = s.quantDBAdapter.Query(ctx, ds, sql)
	}
	if err != nil {
		return nil, fmt.Errorf("query dimension stats: %w", err)
	}

	truncated := len(rows) > limit
	if truncated {
		rows = rows[:limit]
	}
	var totalRows int64
	for _, r := range rows {
		totalRows += toInt64(r["count"])
	}
	if truncated {
		totalSQL := fmt.Sprintf("SELECT COUNT(*) as c FROM %s", qt)
		if req.Filter != nil && req.Filter.ColumnName != "" {
			fc := quoteIdentifierDQ(req.Filter.ColumnName)
			totalSQL = fmt.Sprintf("SELECT COUNT(*) as c FROM %s WHERE %s >= ? AND %s <= ?", qt, fc, fc)
		}
		var trArgs []any
		if req.Filter != nil {
			trArgs = append(trArgs, req.Filter.Start, req.Filter.End)
		}
		var tr []map[string]any
		if len(trArgs) > 0 {
			tr, _ = s.quantDBAdapter.Query(ctx, ds, totalSQL, trArgs...)
		} else {
			tr, _ = s.quantDBAdapter.Query(ctx, ds, totalSQL)
		}
		if len(tr) > 0 {
			totalRows = toInt64(tr[0]["c"])
		}
	}

	return &datastore.SingleDimensionStatsResult{
		Dimension: req.Dimension,
		Rows:      rows,
		TotalRows: totalRows,
		Truncated: truncated,
	}, nil
}

// ApplyFix 修复执行
func (s *DataQualityApplicationServiceImpl) ApplyFix(ctx context.Context, req datastore.ApplyFixRequest) (shared.ID, error) {
	ds, err := s.resolveDataStore(ctx, req.DataStoreID, req.TableName)
	if err != nil {
		return "", err
	}

	switch req.FixType {
	case datastore.FixTypeSyncMissing, datastore.FixTypeSyncLatest:
		planID, ok := req.Params["sync_plan_id"].(string)
		if !ok || planID == "" {
			planID = s.findSyncPlanForTable(ctx, req.DataStoreID, req.TableName)
		}
		if planID == "" {
			return "", shared.NewDomainError(shared.ErrCodeValidation, "no sync plan found for this table, cannot apply sync fix", nil)
		}
		startDate, _ := req.Params["start_date"].(string)
		endDate, _ := req.Params["end_date"].(string)
		execID, err := s.syncSvc.ExecuteSyncPlan(ctx, shared.ID(planID), contracts.ExecuteSyncPlanRequest{
			StartDate: startDate,
			EndDate:   endDate,
		})
		if err != nil {
			return "", fmt.Errorf("execute sync plan: %w", err)
		}
		return execID, nil

	case datastore.FixTypeRemoveDuplicates:
		dedupSQL, ok := req.Params["dedup_sql"].(string)
		if !ok || dedupSQL == "" {
			return "", shared.NewDomainError(shared.ErrCodeValidation, "dedup_sql is required", nil)
		}
		_, err := s.quantDBAdapter.Execute(ctx, ds, dedupSQL)
		if err != nil {
			return "", fmt.Errorf("execute dedup: %w", err)
		}
		return shared.ID("dedup-success"), nil
	}

	return "", shared.NewDomainError(shared.ErrCodeValidation, "unsupported fix type: "+string(req.FixType), nil)
}

// --- helpers ---

func (s *DataQualityApplicationServiceImpl) resolveDataStore(ctx context.Context, id shared.ID, tableName string) (*datastore.QuantDataStore, error) {
	if s.quantDBAdapter == nil {
		return nil, shared.NewDomainError(shared.ErrCodeValidation, "data quality service not available", nil)
	}
	ds, err := s.dataStoreRepo.Get(id)
	if err != nil {
		return nil, fmt.Errorf("get data store: %w", err)
	}
	if ds == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil)
	}
	tables, err := s.quantDBAdapter.ListTables(ctx, ds)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	allowed := false
	for _, t := range tables {
		if t == tableName {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "table not found", nil)
	}
	return ds, nil
}

func (s *DataQualityApplicationServiceImpl) getTableColumnNames(ctx context.Context, ds *datastore.QuantDataStore, tableName string) ([]string, error) {
	query := "SELECT column_name FROM information_schema.columns WHERE table_schema = 'main' AND table_name = ? ORDER BY ordinal_position"
	rows, err := s.quantDBAdapter.Query(ctx, ds, query, tableName)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(rows))
	for _, r := range rows {
		if cn, ok := r["column_name"]; ok && cn != nil {
			names = append(names, fmt.Sprintf("%v", cn))
		}
	}
	return names, nil
}

func (s *DataQualityApplicationServiceImpl) findSyncPlanForTable(ctx context.Context, dataStoreID shared.ID, tableName string) string {
	plans, err := s.syncPlanRepo.GetByDataStore(dataStoreID)
	if err != nil || len(plans) == 0 {
		return ""
	}
	for _, p := range plans {
		for _, api := range p.ResolvedAPIs {
			if api == tableName {
				return p.ID.String()
			}
		}
		for _, api := range p.SelectedAPIs {
			if api == tableName {
				return p.ID.String()
			}
		}
	}
	return ""
}

func (s *DataQualityApplicationServiceImpl) calcCompletenessScore(m *datastore.MissingAnalysisResult) float64 {
	if m == nil || m.Summary.ExpectedCount == 0 {
		return 100
	}
	rate := float64(m.Summary.MissingCount) / float64(m.Summary.ExpectedCount)
	return 100 * (1 - rate)
}

func (s *DataQualityApplicationServiceImpl) calcUniquenessScore(d *datastore.DuplicateAnalysisResult) float64 {
	if d == nil || !d.HasDuplicates {
		return 100
	}
	return 100 * (1 - d.DuplicateRate/100)
}

func (s *DataQualityApplicationServiceImpl) calcTimelinessScore(r *datastore.EffectiveRangeResult) float64 {
	if r == nil || r.LatestDate == "" {
		return 100
	}
	latest, err := time.Parse("20060102", r.LatestDate)
	if err != nil {
		return 100
	}
	today := time.Now()
	staleDays := int(today.Sub(latest).Hours() / 24)
	score := 100.0 - float64(staleDays*5)
	if score < 0 {
		score = 0
	}
	return score
}

func (s *DataQualityApplicationServiceImpl) calcValidityScore(a *datastore.AnomalyAnalysisResult) float64 {
	if a == nil {
		return 100
	}
	return 100 * (1 - a.AnomalyRate/100)
}

func (s *DataQualityApplicationServiceImpl) generateFixSuggestions(report *datastore.QualityReport, ds *datastore.QuantDataStore, primaryKeys []string) []datastore.FixSuggestion {
	var suggestions []datastore.FixSuggestion
	planID := s.findSyncPlanForTable(context.Background(), report.DataStoreID, report.TableName)

	// 1. 时效性
	if report.TimelinessScore < 80 && report.EffectiveRange != nil && report.EffectiveRange.LatestDate != "" {
		today := time.Now().Format("20060102")
		action := &datastore.FixAction{
			Type: "create_sync_plan",
			Params: map[string]any{
				"start_date":   report.EffectiveRange.LatestDate,
				"end_date":     today,
				"sync_plan_id": planID,
			},
		}
		suggestions = append(suggestions, datastore.FixSuggestion{
			ID:          uuid.New().String(),
			Type:        datastore.FixTypeSyncLatest,
			Severity:    datastore.SeverityHigh,
			Title:       "数据不够新",
			Description: fmt.Sprintf("最新数据日期为 %s，建议同步至今天", report.EffectiveRange.LatestDate),
			Action:      action,
		})
	}

	// 2. 完整性
	if report.CompletenessScore < 95 && report.Missing != nil && report.Missing.Summary.MissingCount > 0 &&
		report.EffectiveRange != nil {
		action := &datastore.FixAction{
			Type: "create_sync_plan",
			Params: map[string]any{
				"start_date":   report.EffectiveRange.EarliestDate,
				"end_date":     report.EffectiveRange.LatestDate,
				"sync_plan_id": planID,
			},
		}
		suggestions = append(suggestions, datastore.FixSuggestion{
			ID:       uuid.New().String(),
			Type:     datastore.FixTypeSyncMissing,
			Severity: datastore.SeverityMedium,
			Title:    fmt.Sprintf("缺失 %d 条数据", report.Missing.Summary.MissingCount),
			Description: fmt.Sprintf("应有 %d 条，实际 %d 条，缺失率 %.1f%%",
				report.Missing.Summary.ExpectedCount, report.Missing.Summary.ActualCount, report.Missing.Summary.MissingRatePct),
			Action: action,
		})
	}

	// 3. 唯一性
	if report.Duplicates != nil && report.Duplicates.HasDuplicates && len(primaryKeys) > 0 {
		dedupSQL := s.generateDedupSQL(report.TableName, primaryKeys)
		action := &datastore.FixAction{
			Type: "execute_sql",
			Params: map[string]any{
				"dedup_sql": dedupSQL,
			},
		}
		suggestions = append(suggestions, datastore.FixSuggestion{
			ID:          uuid.New().String(),
			Type:        datastore.FixTypeRemoveDuplicates,
			Severity:    datastore.SeverityMedium,
			Title:       fmt.Sprintf("存在 %d 组重复数据", report.Duplicates.DuplicateCount),
			Description: fmt.Sprintf("重复率 %.2f%%，建议执行去重", report.Duplicates.DuplicateRate),
			Action:      action,
		})
	}

	return suggestions
}

func (s *DataQualityApplicationServiceImpl) generateDedupSQL(tableName string, primaryKeys []string) string {
	qt := quoteIdentifierDQ(tableName)
	pkCols := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		pkCols[i] = quoteIdentifierDQ(pk)
	}
	partitionBy := strings.Join(pkCols, ", ")
	// DuckDB: 用 rowid 删除重复行，保留每组第一条
	return fmt.Sprintf(`DELETE FROM %s WHERE rowid IN (
		SELECT rowid FROM (SELECT rowid, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY rowid) as rn FROM %s) sub WHERE rn > 1
	)`, qt, partitionBy, qt)
}

func quoteIdentifierDQ(name string) string {
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}

func fmtVal(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func toInt64(v any) int64 {
	if v == nil {
		return 0
	}
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case float64:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}
