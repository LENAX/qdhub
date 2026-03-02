package dao

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/shared"
)

// DataTypeMappingRuleDAO provides data access operations for DataTypeMappingRule.
type DataTypeMappingRuleDAO struct {
	*SQLBaseDAO[DataTypeMappingRuleRow]
}

// NewDataTypeMappingRuleDAO creates a new DataTypeMappingRuleDAO.
func NewDataTypeMappingRuleDAO(db *sqlx.DB) *DataTypeMappingRuleDAO {
	return &DataTypeMappingRuleDAO{
		SQLBaseDAO: NewSQLBaseDAO[DataTypeMappingRuleRow](db, "data_type_mapping_rules", "id"),
	}
}

// Create inserts a new mapping rule record.
func (d *DataTypeMappingRuleDAO) Create(tx *sqlx.Tx, entity *datastore.DataTypeMappingRule) error {
	query := `INSERT INTO data_type_mapping_rules (id, data_source_type, source_type, target_db_type, target_type, field_pattern, priority, is_default, created_at, updated_at)
		VALUES (:id, :data_source_type, :source_type, :target_db_type, :target_type, :field_pattern, :priority, :is_default, :created_at, :updated_at)`

	row := d.toRow(entity)
	var err error
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to create mapping rule: %w", err)
	}
	return nil
}

// GetByID retrieves a mapping rule by ID.
func (d *DataTypeMappingRuleDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*datastore.DataTypeMappingRule, error) {
	row, err := d.Get(tx, id.String())
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return d.toEntity(row), nil
}

// Update updates an existing mapping rule record.
func (d *DataTypeMappingRuleDAO) Update(tx *sqlx.Tx, entity *datastore.DataTypeMappingRule) error {
	query := `UPDATE data_type_mapping_rules SET
		data_source_type = :data_source_type, source_type = :source_type, target_db_type = :target_db_type,
		target_type = :target_type, field_pattern = :field_pattern, priority = :priority, is_default = :is_default, updated_at = :updated_at
		WHERE id = :id`

	row := d.toRow(entity)
	var err error
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to update mapping rule: %w", err)
	}
	return nil
}

// DeleteByID deletes a mapping rule by ID.
func (d *DataTypeMappingRuleDAO) DeleteByID(tx *sqlx.Tx, id shared.ID) error {
	return d.Delete(tx, id.String())
}

// ListAll retrieves all mapping rules.
func (d *DataTypeMappingRuleDAO) ListAll(tx *sqlx.Tx) ([]*datastore.DataTypeMappingRule, error) {
	rows, err := d.List(tx)
	if err != nil {
		return nil, err
	}

	entities := make([]*datastore.DataTypeMappingRule, 0, len(rows))
	for _, row := range rows {
		entities = append(entities, d.toEntity(row))
	}
	return entities, nil
}

// GetBySourceAndTarget retrieves rules by source and target types, ordered by priority descending.
func (d *DataTypeMappingRuleDAO) GetBySourceAndTarget(tx *sqlx.Tx, dataSourceType, targetDBType string) ([]*datastore.DataTypeMappingRule, error) {
	query := d.DB().Rebind(`SELECT * FROM data_type_mapping_rules WHERE data_source_type = ? AND target_db_type = ? ORDER BY priority DESC`)
	var rows []*DataTypeMappingRuleRow

	var err error
	if tx != nil {
		err = tx.Select(&rows, query, dataSourceType, targetDBType)
	} else {
		err = d.DB().Select(&rows, query, dataSourceType, targetDBType)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get mapping rules: %w", err)
	}

	entities := make([]*datastore.DataTypeMappingRule, 0, len(rows))
	for _, row := range rows {
		entities = append(entities, d.toEntity(row))
	}
	return entities, nil
}

// SaveBatch saves multiple rules in a batch.
func (d *DataTypeMappingRuleDAO) SaveBatch(tx *sqlx.Tx, rules []*datastore.DataTypeMappingRule) error {
	if len(rules) == 0 {
		return nil
	}

	query := `INSERT INTO data_type_mapping_rules (id, data_source_type, source_type, target_db_type, target_type, field_pattern, priority, is_default, created_at, updated_at)
		VALUES (:id, :data_source_type, :source_type, :target_db_type, :target_type, :field_pattern, :priority, :is_default, :created_at, :updated_at)
		ON CONFLICT (data_source_type, source_type, target_db_type, field_pattern) DO UPDATE SET
		target_type = excluded.target_type, priority = excluded.priority, is_default = excluded.is_default, updated_at = excluded.updated_at`

	for _, rule := range rules {
		row := d.toRow(rule)
		var err error
		if tx != nil {
			_, err = tx.NamedExec(query, row)
		} else {
			_, err = d.DB().NamedExec(query, row)
		}
		if err != nil {
			return fmt.Errorf("failed to save mapping rule batch: %w", err)
		}
	}
	return nil
}

// toRow converts domain entity to database row.
func (d *DataTypeMappingRuleDAO) toRow(entity *datastore.DataTypeMappingRule) *DataTypeMappingRuleRow {
	row := &DataTypeMappingRuleRow{
		ID:             entity.ID.String(),
		DataSourceType: entity.DataSourceType,
		SourceType:     entity.SourceType,
		TargetDBType:   entity.TargetDBType,
		TargetType:     entity.TargetType,
		Priority:       entity.Priority,
		IsDefault:      entity.IsDefault,
		CreatedAt:      entity.CreatedAt.ToTime(),
		UpdatedAt:      entity.UpdatedAt.ToTime(),
	}

	if entity.FieldPattern != nil {
		row.FieldPattern = sql.NullString{String: *entity.FieldPattern, Valid: true}
	}

	return row
}

// toEntity converts database row to domain entity.
func (d *DataTypeMappingRuleDAO) toEntity(row *DataTypeMappingRuleRow) *datastore.DataTypeMappingRule {
	entity := &datastore.DataTypeMappingRule{
		ID:             shared.ID(row.ID),
		DataSourceType: row.DataSourceType,
		SourceType:     row.SourceType,
		TargetDBType:   row.TargetDBType,
		TargetType:     row.TargetType,
		Priority:       row.Priority,
		IsDefault:      row.IsDefault,
		CreatedAt:      shared.Timestamp(row.CreatedAt),
		UpdatedAt:      shared.Timestamp(row.UpdatedAt),
	}

	if row.FieldPattern.Valid {
		entity.FieldPattern = &row.FieldPattern.String
	}

	return entity
}
