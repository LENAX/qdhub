package dao

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/realtime"
	"qdhub/internal/domain/shared"
)

// RealtimeSourceDAO provides data access for realtime_sources.
type RealtimeSourceDAO struct {
	db *sqlx.DB
}

// NewRealtimeSourceDAO creates a new RealtimeSourceDAO.
func NewRealtimeSourceDAO(db *sqlx.DB) *RealtimeSourceDAO {
	return &RealtimeSourceDAO{db: db}
}

// TableName returns the table name.
func (d *RealtimeSourceDAO) TableName() string {
	return "realtime_sources"
}

// DB returns the underlying DB.
func (d *RealtimeSourceDAO) DB() *sqlx.DB {
	return d.db
}

// Create inserts a realtime source.
func (d *RealtimeSourceDAO) Create(tx *sqlx.Tx, entity *realtime.RealtimeSource) error {
	query := `INSERT INTO realtime_sources (id, name, type, config, priority, is_primary, health_check_on_startup, enabled, last_health_status, last_health_at, last_health_error, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	row := toRealtimeSourceRow(entity)
	args := []interface{}{row.ID, row.Name, row.Type, row.Config, row.Priority, row.IsPrimary, row.HealthCheckOnStartup, row.Enabled, row.LastHealthStatus, row.LastHealthAt, row.LastHealthError, row.CreatedAt, row.UpdatedAt}
	if tx != nil {
		_, err := tx.Exec(d.db.Rebind(query), args...)
		return err
	}
	_, err := d.db.Exec(d.db.Rebind(query), args...)
	return err
}

// GetByID retrieves by ID.
func (d *RealtimeSourceDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*realtime.RealtimeSource, error) {
	query := d.db.Rebind("SELECT * FROM realtime_sources WHERE id = ?")
	var row RealtimeSourceRow
	var err error
	if tx != nil {
		err = tx.Get(&row, query, id.String())
	} else {
		err = d.db.Get(&row, query, id.String())
	}
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return fromRealtimeSourceRow(&row), nil
}

// Update updates a realtime source.
func (d *RealtimeSourceDAO) Update(tx *sqlx.Tx, entity *realtime.RealtimeSource) error {
	query := d.db.Rebind(`UPDATE realtime_sources SET name=?, type=?, config=?, priority=?, is_primary=?, health_check_on_startup=?, enabled=?, last_health_status=?, last_health_at=?, last_health_error=?, updated_at=? WHERE id=?`)
	row := toRealtimeSourceRow(entity)
	args := []interface{}{row.Name, row.Type, row.Config, row.Priority, row.IsPrimary, row.HealthCheckOnStartup, row.Enabled, row.LastHealthStatus, row.LastHealthAt, row.LastHealthError, row.UpdatedAt, row.ID}
	if tx != nil {
		_, err := tx.Exec(query, args...)
		return err
	}
	_, err := d.db.Exec(query, args...)
	return err
}

// DeleteByID deletes by ID.
func (d *RealtimeSourceDAO) DeleteByID(tx *sqlx.Tx, id shared.ID) error {
	query := d.db.Rebind("DELETE FROM realtime_sources WHERE id = ?")
	if tx != nil {
		_, err := tx.Exec(query, id.String())
		return err
	}
	_, err := d.db.Exec(query, id.String())
	return err
}

// ListAll returns all realtime sources.
func (d *RealtimeSourceDAO) ListAll(tx *sqlx.Tx) ([]*realtime.RealtimeSource, error) {
	query := "SELECT * FROM realtime_sources ORDER BY priority ASC"
	var rows []RealtimeSourceRow
	var err error
	if tx != nil {
		err = tx.Select(&rows, d.db.Rebind(query))
	} else {
		err = d.db.Select(&rows, d.db.Rebind(query))
	}
	if err != nil {
		return nil, err
	}
	out := make([]*realtime.RealtimeSource, 0, len(rows))
	for i := range rows {
		out = append(out, fromRealtimeSourceRow(&rows[i]))
	}
	return out, nil
}

// ListEnabledForHealthCheck returns enabled sources with health_check_on_startup=1.
func (d *RealtimeSourceDAO) ListEnabledForHealthCheck(tx *sqlx.Tx) ([]*realtime.RealtimeSource, error) {
	query := d.db.Rebind("SELECT * FROM realtime_sources WHERE enabled = 1 AND health_check_on_startup = 1 ORDER BY priority ASC")
	var rows []RealtimeSourceRow
	var err error
	if tx != nil {
		err = tx.Select(&rows, query)
	} else {
		err = d.db.Select(&rows, query)
	}
	if err != nil {
		return nil, err
	}
	out := make([]*realtime.RealtimeSource, 0, len(rows))
	for i := range rows {
		out = append(out, fromRealtimeSourceRow(&rows[i]))
	}
	return out, nil
}

// GetOrderedByPurpose returns enabled sources for the given purpose, ordered by priority asc.
func (d *RealtimeSourceDAO) GetOrderedByPurpose(tx *sqlx.Tx, purpose string) ([]*realtime.RealtimeSource, error) {
	var types []string
	switch purpose {
	case realtime.PurposeTsRealtimeMktTick:
		types = []string{realtime.TypeTushareForward, realtime.TypeTushareWS}
	case realtime.PurposeRealtimeQuote:
		types = []string{realtime.TypeSina, realtime.TypeEastmoney}
	case realtime.PurposeRealtimeTick:
		types = []string{realtime.TypeEastmoney}
	default:
		return nil, fmt.Errorf("unknown purpose: %s", purpose)
	}
	if len(types) == 0 {
		return []*realtime.RealtimeSource{}, nil
	}
	// build IN clause
	query := d.db.Rebind("SELECT * FROM realtime_sources WHERE enabled = 1 AND type IN (?, ?) ORDER BY priority ASC")
	if purpose == realtime.PurposeRealtimeTick {
		query = d.db.Rebind("SELECT * FROM realtime_sources WHERE enabled = 1 AND type = ? ORDER BY priority ASC")
	}
	var rows []RealtimeSourceRow
	var err error
	if purpose == realtime.PurposeRealtimeTick {
		if tx != nil {
			err = tx.Select(&rows, query, realtime.TypeEastmoney)
		} else {
			err = d.db.Select(&rows, query, realtime.TypeEastmoney)
		}
	} else {
		if tx != nil {
			err = tx.Select(&rows, query, types[0], types[1])
		} else {
			err = d.db.Select(&rows, query, types[0], types[1])
		}
	}
	if err != nil {
		return nil, err
	}
	out := make([]*realtime.RealtimeSource, 0, len(rows))
	for i := range rows {
		out = append(out, fromRealtimeSourceRow(&rows[i]))
	}
	return out, nil
}

func toRealtimeSourceRow(e *realtime.RealtimeSource) *RealtimeSourceRow {
	r := &RealtimeSourceRow{
		ID:                   e.ID.String(),
		Name:                 e.Name,
		Type:                 e.Type,
		Config:               e.Config,
		Priority:             e.Priority,
		IsPrimary:            boolToIntRealtime(e.IsPrimary),
		HealthCheckOnStartup: boolToIntRealtime(e.HealthCheckOnStartup),
		Enabled:              boolToIntRealtime(e.Enabled),
		CreatedAt:            e.CreatedAt.ToTime(),
		UpdatedAt:            e.UpdatedAt.ToTime(),
	}
	if e.LastHealthStatus != "" {
		r.LastHealthStatus = sql.NullString{String: e.LastHealthStatus, Valid: true}
	}
	if !e.LastHealthAt.IsZero() {
		r.LastHealthAt = sql.NullTime{Time: e.LastHealthAt.ToTime(), Valid: true}
	}
	if e.LastHealthError != "" {
		r.LastHealthError = sql.NullString{String: e.LastHealthError, Valid: true}
	}
	return r
}

func fromRealtimeSourceRow(r *RealtimeSourceRow) *realtime.RealtimeSource {
	e := &realtime.RealtimeSource{
		ID:                   shared.ID(r.ID),
		Name:                 r.Name,
		Type:                 r.Type,
		Config:               r.Config,
		Priority:             r.Priority,
		IsPrimary:            r.IsPrimary != 0,
		HealthCheckOnStartup: r.HealthCheckOnStartup != 0,
		Enabled:              r.Enabled != 0,
		CreatedAt:            shared.Timestamp(r.CreatedAt),
		UpdatedAt:            shared.Timestamp(r.UpdatedAt),
	}
	if r.LastHealthStatus.Valid {
		e.LastHealthStatus = r.LastHealthStatus.String
	}
	if r.LastHealthAt.Valid {
		e.LastHealthAt = shared.Timestamp(r.LastHealthAt.Time)
	}
	if r.LastHealthError.Valid {
		e.LastHealthError = r.LastHealthError.String
	}
	return e
}

func boolToIntRealtime(b bool) int {
	if b {
		return 1
	}
	return 0
}

