package analysis

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	domainDatastore "qdhub/internal/domain/datastore"
	duckdbInfra "qdhub/internal/infrastructure/quantdb/duckdb"
)

func TestGetByDateAndType_UsesThsLimitTypeWhenLimitListDHasNoRows(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "readers_limit_stocks.duckdb")

	factory := duckdbInfra.NewFactory()
	t.Cleanup(func() { _ = factory.Close() })

	db, err := factory.Create(domainDatastore.QuantDBConfig{
		Type:        domainDatastore.DataStoreTypeDuckDB,
		StoragePath: dbPath,
	})
	if err != nil {
		t.Fatalf("create duckdb: %v", err)
	}

	ddl := []string{
		`CREATE TABLE stock_basic (ts_code VARCHAR, name VARCHAR, industry VARCHAR)`,
		`CREATE TABLE limit_list_d (
			trade_date VARCHAR, ts_code VARCHAR, "limit" VARCHAR, pct_chg DOUBLE,
			first_time VARCHAR, last_time VARCHAR, name VARCHAR, close DOUBLE,
			turnover_ratio DOUBLE, amount DOUBLE, float_mv DOUBLE, open_times INTEGER, industry VARCHAR
		)`,
		`CREATE TABLE limit_list_ths (
			trade_date VARCHAR, ts_code VARCHAR, name VARCHAR,
			first_lu_time VARCHAR, last_lu_time VARCHAR, lu_desc VARCHAR,
			price DOUBLE, pct_chg DOUBLE, turnover_rate DOUBLE, turnover DOUBLE,
			free_float DOUBLE, open_num INTEGER, limit_type VARCHAR
		)`,
	}
	for _, sql := range ddl {
		if _, err := db.Execute(ctx, sql); err != nil {
			t.Fatalf("create table failed: %v", err)
		}
	}

	tradeDate := time.Now().Format("20060102")
	if _, err := db.Execute(ctx, `
INSERT INTO limit_list_ths
	(trade_date, ts_code, name, first_lu_time, last_lu_time, lu_desc, price, pct_chg, turnover_rate, turnover, free_float, open_num, limit_type)
VALUES
	(?, '000001.SZ', '测试跌停股', '', '', '', 10.0, -1.2, 1.0, 1000.0, 10000.0, 0, '跌停池'),
	(?, '000002.SZ', '测试炸板股', '10:01:00', '14:30:00', '', 12.0, 3.5, 2.0, 2000.0, 20000.0, 2, '炸板池')
`, tradeDate, tradeDate); err != nil {
		t.Fatalf("insert ths rows failed: %v", err)
	}

	reader := NewReaders(db)

	downRows, err := reader.GetByDateAndType(ctx, tradeDate, "down")
	if err != nil {
		t.Fatalf("query down rows failed: %v", err)
	}
	if len(downRows) != 1 || downRows[0].TsCode != "000001.SZ" {
		t.Fatalf("down rows mismatch: got=%+v", downRows)
	}

	zRows, err := reader.GetByDateAndType(ctx, tradeDate, "z")
	if err != nil {
		t.Fatalf("query z rows failed: %v", err)
	}
	if len(zRows) != 1 || zRows[0].TsCode != "000002.SZ" {
		t.Fatalf("z rows mismatch: got=%+v", zRows)
	}
}

