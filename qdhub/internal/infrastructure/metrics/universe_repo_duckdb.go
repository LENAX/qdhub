package metrics

import (
	"context"
	"fmt"
	"strings"
	"time"

	"qdhub/internal/domain/datastore"
	domain "qdhub/internal/domain/metrics"
)

type UniverseRepoDuckDB struct {
	db datastore.QuantDB
}

func NewUniverseRepoDuckDB(db datastore.QuantDB) *UniverseRepoDuckDB {
	return &UniverseRepoDuckDB{db: db}
}

func (r *UniverseRepoDuckDB) Replace(ctx context.Context, universeID string, startDate, endDate string, values []domain.UniverseMembership) error {
	if _, err := r.db.Execute(ctx, `DELETE FROM universe_membership WHERE universe_id = ? AND trade_date >= ? AND trade_date <= ?`, universeID, startDate, endDate); err != nil {
		return fmt.Errorf("delete universe_membership range: %w", err)
	}
	if len(values) == 0 {
		return nil
	}
	rows := make([]map[string]any, 0, len(values))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, value := range values {
		rows = append(rows, map[string]any{
			"universe_id": value.UniverseID,
			"entity_id":   value.EntityID,
			"trade_date":  value.TradeDate,
			"frequency":   value.Frequency,
			"version":     value.Version,
			"created_at":  now,
		})
	}
	if _, err := r.db.BulkInsert(ctx, "universe_membership", rows); err != nil {
		return fmt.Errorf("insert universe_membership rows: %w", err)
	}
	return nil
}

func (r *UniverseRepoDuckDB) QueryMembers(ctx context.Context, req domain.UniverseMembersQuery) ([]domain.UniverseMembership, error) {
	args := make([]any, 0, 4)
	sql := `SELECT universe_id, entity_id, trade_date, frequency, version
FROM universe_membership
WHERE universe_id = ?`
	args = append(args, req.UniverseID)
	if strings.TrimSpace(req.Frequency) != "" {
		sql += ` AND frequency = ?`
		args = append(args, req.Frequency)
	}
	if strings.TrimSpace(req.TradeDate) != "" {
		sql += ` AND trade_date = ?`
		args = append(args, req.TradeDate)
	} else {
		if strings.TrimSpace(req.StartDate) != "" {
			sql += ` AND trade_date >= ?`
			args = append(args, req.StartDate)
		}
		if strings.TrimSpace(req.EndDate) != "" {
			sql += ` AND trade_date <= ?`
			args = append(args, req.EndDate)
		}
	}
	sql += ` ORDER BY trade_date, entity_id`
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query universe_membership: %w", err)
	}
	out := make([]domain.UniverseMembership, 0, len(rows))
	for _, row := range rows {
		out = append(out, domain.UniverseMembership{
			UniverseID: toString(row["universe_id"]),
			EntityID:   toString(row["entity_id"]),
			TradeDate:  toString(row["trade_date"]),
			Frequency:  toString(row["frequency"]),
			Version:    toInt(row["version"]),
		})
	}
	return out, nil
}
