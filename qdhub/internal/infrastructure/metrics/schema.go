package metrics

import (
	"context"
	"fmt"

	"qdhub/internal/domain/datastore"
)

func EnsureSchema(ctx context.Context, db datastore.QuantDB) error {
	ddls := []string{
		`CREATE TABLE IF NOT EXISTS metric_def (
			metric_id TEXT PRIMARY KEY,
			display_name_cn TEXT NOT NULL,
			kind TEXT NOT NULL,
			category TEXT,
			expression TEXT NOT NULL,
			frequency TEXT NOT NULL,
			source_resolution TEXT,
			status TEXT NOT NULL,
			version INTEGER NOT NULL,
			depends_on_json TEXT,
			factor_spec_json TEXT,
			signal_spec_json TEXT,
			universe_spec_json TEXT,
			created_at TIMESTAMP,
			updated_at TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS factor_value (
			metric_id TEXT NOT NULL,
			entity_id TEXT NOT NULL,
			trade_date TEXT NOT NULL,
			frequency TEXT NOT NULL,
			version INTEGER NOT NULL,
			value DOUBLE NOT NULL,
			created_at TIMESTAMP,
			PRIMARY KEY(metric_id, entity_id, trade_date, frequency, version)
		)`,
		`CREATE TABLE IF NOT EXISTS signal_value (
			metric_id TEXT NOT NULL,
			entity_id TEXT NOT NULL,
			trade_date TEXT NOT NULL,
			frequency TEXT NOT NULL,
			version INTEGER NOT NULL,
			bool_value BOOLEAN,
			text_value TEXT,
			created_at TIMESTAMP,
			PRIMARY KEY(metric_id, entity_id, trade_date, frequency, version)
		)`,
		`CREATE TABLE IF NOT EXISTS universe_membership (
			universe_id TEXT NOT NULL,
			entity_id TEXT NOT NULL,
			trade_date TEXT NOT NULL,
			frequency TEXT NOT NULL,
			version INTEGER NOT NULL,
			created_at TIMESTAMP,
			PRIMARY KEY(universe_id, entity_id, trade_date, frequency, version)
		)`,
		`CREATE TABLE IF NOT EXISTS compute_job (
			job_id TEXT PRIMARY KEY,
			job_type TEXT NOT NULL,
			target_ids_json TEXT NOT NULL,
			range_type TEXT NOT NULL,
			start_time TEXT,
			end_time TEXT,
			trigger_reason TEXT,
			priority INTEGER,
			status TEXT NOT NULL,
			error_message TEXT,
			result_summary TEXT,
			created_at TIMESTAMP,
			started_at TIMESTAMP,
			finished_at TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_metric_def_kind_status ON metric_def(kind, status)`,
		`CREATE INDEX IF NOT EXISTS idx_factor_value_lookup ON factor_value(metric_id, trade_date, entity_id)`,
		`CREATE INDEX IF NOT EXISTS idx_signal_value_lookup ON signal_value(metric_id, trade_date, entity_id)`,
		`CREATE INDEX IF NOT EXISTS idx_universe_membership_lookup ON universe_membership(universe_id, trade_date, entity_id)`,
	}
	for _, ddl := range ddls {
		if _, err := db.Execute(ctx, ddl); err != nil {
			return fmt.Errorf("ensure metrics schema: %w", err)
		}
	}
	return nil
}
