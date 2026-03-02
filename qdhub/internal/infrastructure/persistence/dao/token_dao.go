package dao

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
)

// TokenDAO provides data access operations for Token.
type TokenDAO struct {
	*SQLBaseDAO[TokenRow]
}

// NewTokenDAO creates a new TokenDAO.
func NewTokenDAO(db *sqlx.DB) *TokenDAO {
	return &TokenDAO{
		SQLBaseDAO: NewSQLBaseDAO[TokenRow](db, "tokens", "id"),
	}
}

// Create inserts a new token record.
func (d *TokenDAO) Create(tx *sqlx.Tx, entity *metadata.Token) error {
	query := `INSERT INTO tokens (id, data_source_id, token_value, expires_at, created_at)
		VALUES (:id, :data_source_id, :token_value, :expires_at, :created_at)`

	row := d.toRow(entity)
	var err error
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to create token: %w", err)
	}
	return nil
}

// GetByID retrieves a token by ID.
func (d *TokenDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*metadata.Token, error) {
	row, err := d.Get(tx, id.String())
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return d.toEntity(row), nil
}

// GetByDataSource retrieves a token by data source ID.
func (d *TokenDAO) GetByDataSource(tx *sqlx.Tx, dataSourceID shared.ID) (*metadata.Token, error) {
	query := d.DB().Rebind(`SELECT * FROM tokens WHERE data_source_id = ?`)
	var row TokenRow

	var err error
	if tx != nil {
		err = tx.Get(&row, query, dataSourceID.String())
	} else {
		err = d.DB().Get(&row, query, dataSourceID.String())
	}

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get token by data source: %w", err)
	}

	return d.toEntity(&row), nil
}

// Update updates an existing token record.
func (d *TokenDAO) Update(tx *sqlx.Tx, entity *metadata.Token) error {
	query := `UPDATE tokens SET
		token_value = :token_value, expires_at = :expires_at
		WHERE id = :id`

	row := d.toRow(entity)
	var err error
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to update token: %w", err)
	}
	return nil
}

// DeleteByID deletes a token by ID.
func (d *TokenDAO) DeleteByID(tx *sqlx.Tx, id shared.ID) error {
	return d.Delete(tx, id.String())
}

// DeleteByDataSource deletes a token by data source ID.
func (d *TokenDAO) DeleteByDataSource(tx *sqlx.Tx, dataSourceID shared.ID) error {
	query := d.DB().Rebind(`DELETE FROM tokens WHERE data_source_id = ?`)
	var err error
	if tx != nil {
		_, err = tx.Exec(query, dataSourceID.String())
	} else {
		_, err = d.DB().Exec(query, dataSourceID.String())
	}
	if err != nil {
		return fmt.Errorf("failed to delete token by data source: %w", err)
	}
	return nil
}

// toRow converts domain entity to database row.
func (d *TokenDAO) toRow(entity *metadata.Token) *TokenRow {
	row := &TokenRow{
		ID:           entity.ID.String(),
		DataSourceID: entity.DataSourceID.String(),
		TokenValue:   entity.TokenValue,
		CreatedAt:    entity.CreatedAt.ToTime(),
	}

	if entity.ExpiresAt != nil {
		row.ExpiresAt = sql.NullTime{Time: *entity.ExpiresAt, Valid: true}
	}

	return row
}

// toEntity converts database row to domain entity.
func (d *TokenDAO) toEntity(row *TokenRow) *metadata.Token {
	entity := &metadata.Token{
		ID:           shared.ID(row.ID),
		DataSourceID: shared.ID(row.DataSourceID),
		TokenValue:   row.TokenValue,
		CreatedAt:    shared.Timestamp(row.CreatedAt),
	}

	if row.ExpiresAt.Valid {
		entity.ExpiresAt = &row.ExpiresAt.Time
	}

	return entity
}
