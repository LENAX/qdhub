// Package impl contains data quality application service implementation.
package impl

import (
	"context"
	"fmt"
	"strings"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/shared"
)

// DataQualityApplicationServiceImpl implements DataQualityApplicationService.
type DataQualityApplicationServiceImpl struct {
	dataStoreRepo  datastore.QuantDataStoreRepository
	quantDBAdapter QuantDBAdapter
}

// NewDataQualityApplicationService creates a new DataQualityApplicationService implementation.
func NewDataQualityApplicationService(
	dataStoreRepo datastore.QuantDataStoreRepository,
	quantDBAdapter QuantDBAdapter,
) contracts.DataQualityApplicationService {
	return &DataQualityApplicationServiceImpl{
		dataStoreRepo:  dataStoreRepo,
		quantDBAdapter: quantDBAdapter,
	}
}

// GetDimensionDistribution 按选定维度统计表数据量分布，返回多维结果（维度数=请求维度数）.
func (s *DataQualityApplicationServiceImpl) GetDimensionDistribution(ctx context.Context, req datastore.DimensionStatsRequest) (*datastore.DimensionStatsResult, error) {
	ds, err := s.dataStoreRepo.Get(req.DataStoreID)
	if err != nil {
		return nil, fmt.Errorf("get data store: %w", err)
	}
	if ds == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil)
	}
	if s.quantDBAdapter == nil {
		return nil, shared.NewDomainError(shared.ErrCodeValidation, "data quality service not available", nil)
	}

	tables, err := s.quantDBAdapter.ListTables(ctx, ds)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	tableAllowed := false
	for _, t := range tables {
		if t == req.TableName {
			tableAllowed = true
			break
		}
	}
	if !tableAllowed {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "table not found", nil)
	}

	colNames, err := s.getTableColumnNames(ctx, ds, req.TableName)
	if err != nil {
		return nil, fmt.Errorf("get table columns: %w", err)
	}
	colSet := make(map[string]bool)
	for _, c := range colNames {
		colSet[c] = true
	}

	if len(req.Dimensions) == 0 {
		return nil, shared.NewDomainError(shared.ErrCodeValidation, "at least one dimension required", nil)
	}
	dimCols := make([]string, 0, len(req.Dimensions))
	for _, d := range req.Dimensions {
		if d.Type != "column" || d.ColumnName == "" {
			return nil, shared.NewDomainError(shared.ErrCodeValidation, "dimensions must be of type column with column_name set", nil)
		}
		if !colSet[d.ColumnName] {
			return nil, shared.NewDomainError(shared.ErrCodeValidation, "dimension column not in table: "+d.ColumnName, nil)
		}
		dimCols = append(dimCols, d.ColumnName)
	}

	quotedTable := quoteIdentifierDQ(req.TableName)
	quotedDimCols := make([]string, len(dimCols))
	for i, c := range dimCols {
		quotedDimCols[i] = quoteIdentifierDQ(c)
	}

	var whereFragment string
	var whereArgs []any
	if req.Filter != nil && req.Filter.ColumnName != "" && colSet[req.Filter.ColumnName] {
		whereFragment = " WHERE " + quoteIdentifierDQ(req.Filter.ColumnName) + " >= ? AND " + quoteIdentifierDQ(req.Filter.ColumnName) + " <= ?"
		whereArgs = []any{req.Filter.Start, req.Filter.End}
	}

	selectList := strings.Join(quotedDimCols, ", ") + ", COUNT(*) AS count"
	groupList := strings.Join(quotedDimCols, ", ")
	sql := "SELECT " + selectList + " FROM " + quotedTable + whereFragment + " GROUP BY " + groupList

	var rows []map[string]any
	if len(whereArgs) > 0 {
		rows, err = s.quantDBAdapter.Query(ctx, ds, sql, whereArgs...)
	} else {
		rows, err = s.quantDBAdapter.Query(ctx, ds, sql)
	}
	if err != nil {
		return nil, fmt.Errorf("query dimension distribution: %w", err)
	}

	// Total rows: sum of all counts (or run a separate COUNT without GROUP BY for exact total)
	var totalRows int64
	for _, r := range rows {
		if n, ok := r["count"]; ok {
			switch v := n.(type) {
			case int64:
				totalRows += v
			case int:
				totalRows += int64(v)
			case int32:
				totalRows += int64(v)
			}
		}
	}

	out := &datastore.DimensionStatsResult{
		Dimensions: dimCols,
		Rows:       rows,
		TotalRows:  totalRows,
	}
	return out, nil
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

func quoteIdentifierDQ(name string) string {
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}
