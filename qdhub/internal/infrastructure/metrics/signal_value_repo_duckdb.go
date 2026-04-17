package metrics

import (
	"context"
	"fmt"
	"strings"
	"time"

	"qdhub/internal/domain/datastore"
	domain "qdhub/internal/domain/metrics"
)

type SignalValueRepoDuckDB struct {
	db datastore.QuantDB
}

func NewSignalValueRepoDuckDB(db datastore.QuantDB) *SignalValueRepoDuckDB {
	return &SignalValueRepoDuckDB{db: db}
}

func (r *SignalValueRepoDuckDB) Replace(ctx context.Context, metricID string, startDate, endDate string, values []domain.SignalValue) error {
	if _, err := r.db.Execute(ctx, `DELETE FROM signal_value WHERE metric_id = ? AND trade_date >= ? AND trade_date <= ?`, metricID, startDate, endDate); err != nil {
		return fmt.Errorf("delete signal_value range: %w", err)
	}
	if len(values) == 0 {
		return nil
	}
	rows := make([]map[string]any, 0, len(values))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, value := range values {
		row := map[string]any{
			"metric_id":  value.MetricID,
			"entity_id":  value.EntityID,
			"trade_date": value.TradeDate,
			"frequency":  value.Frequency,
			"version":    value.Version,
			"created_at": now,
		}
		if value.BoolValue != nil {
			row["bool_value"] = *value.BoolValue
		}
		if strings.TrimSpace(value.TextValue) != "" {
			row["text_value"] = value.TextValue
		}
		rows = append(rows, row)
	}
	if _, err := r.db.BulkInsert(ctx, "signal_value", rows); err != nil {
		return fmt.Errorf("insert signal_value rows: %w", err)
	}
	return nil
}

func (r *SignalValueRepoDuckDB) QuerySeries(ctx context.Context, req domain.SignalSeriesQuery) ([]domain.SignalValue, error) {
	if len(req.MetricIDs) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(req.MetricIDs)+len(req.EntityIDs)+4)
	sql := fmt.Sprintf(`SELECT metric_id, entity_id, trade_date, frequency, version, bool_value, text_value
FROM signal_value
WHERE metric_id IN (%s) AND trade_date >= ? AND trade_date <= ?`, placeholders(len(req.MetricIDs)))
	for _, metricID := range req.MetricIDs {
		args = append(args, metricID)
	}
	args = append(args, req.StartDate, req.EndDate)
	if strings.TrimSpace(req.Frequency) != "" {
		sql += ` AND frequency = ?`
		args = append(args, req.Frequency)
	}
	if len(req.EntityIDs) > 0 {
		sql += fmt.Sprintf(` AND entity_id IN (%s)`, placeholders(len(req.EntityIDs)))
		for _, entityID := range req.EntityIDs {
			args = append(args, entityID)
		}
	}
	sql += ` ORDER BY trade_date, entity_id, metric_id`
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query signal series: %w", err)
	}
	out := make([]domain.SignalValue, 0, len(rows))
	for _, row := range rows {
		out = append(out, domain.SignalValue{
			MetricID:  toString(row["metric_id"]),
			EntityID:  toString(row["entity_id"]),
			TradeDate: toString(row["trade_date"]),
			Frequency: toString(row["frequency"]),
			Version:   toInt(row["version"]),
			BoolValue: toBoolPtr(row["bool_value"]),
			TextValue: toString(row["text_value"]),
		})
	}
	return out, nil
}

func (r *SignalValueRepoDuckDB) LoadByMetric(ctx context.Context, metricID string, startDate, endDate string) ([]domain.SignalValue, error) {
	return r.QuerySeries(ctx, domain.SignalSeriesQuery{
		MetricIDs: []string{metricID},
		StartDate: startDate,
		EndDate:   endDate,
	})
}
