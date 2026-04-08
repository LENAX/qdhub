package analysis

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"qdhub/internal/domain/analysis"
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

func TestGetMoneyFlowRank_UsesMoneyflowTable(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "readers_moneyflow_rank.duckdb")
	factory := duckdbInfra.NewFactory()
	t.Cleanup(func() { _ = factory.Close() })
	db, err := factory.Create(domainDatastore.QuantDBConfig{
		Type: domainDatastore.DataStoreTypeDuckDB, StoragePath: dbPath,
	})
	if err != nil {
		t.Fatalf("create duckdb: %v", err)
	}
	for _, sql := range []string{
		`CREATE TABLE stock_basic (ts_code VARCHAR, name VARCHAR)`,
		`CREATE TABLE moneyflow (
			trade_date VARCHAR, ts_code VARCHAR,
			buy_sm_amount DOUBLE, sell_sm_amount DOUBLE, buy_md_amount DOUBLE, sell_md_amount DOUBLE,
			buy_lg_amount DOUBLE, sell_lg_amount DOUBLE, buy_elg_amount DOUBLE, sell_elg_amount DOUBLE,
			net_mf_amount DOUBLE
		)`,
		`CREATE TABLE moneyflow_cnt_ths (trade_date VARCHAR, code VARCHAR, name VARCHAR, net_mf_amount DOUBLE)`,
	} {
		if _, err := db.Execute(ctx, sql); err != nil {
			t.Fatalf("ddl: %v", err)
		}
	}
	td := "20250301"
	if _, err := db.Execute(ctx, `INSERT INTO stock_basic VALUES ('000001.SZ', 'A'), ('000002.SZ', 'B')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, `
INSERT INTO moneyflow (trade_date, ts_code, buy_sm_amount, sell_sm_amount, buy_md_amount, sell_md_amount, buy_lg_amount, sell_lg_amount, buy_elg_amount, sell_elg_amount, net_mf_amount)
VALUES (?, '000001.SZ', 0,0,0,0,0,0,0,0, 100), (?, '000002.SZ', 0,0,0,0,0,0,0,0, 200)
`, td, td); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, `INSERT INTO moneyflow_cnt_ths VALUES (?, 'C001', '概念甲', 50)`, td); err != nil {
		t.Fatal(err)
	}
	r := NewReaders(db)
	res, err := r.GetMoneyFlowRank(ctx, analysis.MoneyFlowRankRequest{Scope: "all", TradeDate: td, Limit: 10, Offset: 0})
	if err != nil {
		t.Fatal(err)
	}
	if res.TradeDate != td || res.StockSource != "moneyflow" {
		t.Fatalf("expected trade_date=%s stock_source=moneyflow, got %+v %+v", td, res.TradeDate, res.StockSource)
	}
	if len(res.StockItems) != 2 || res.StockItems[0].Rank != 1 || res.StockItems[0].TsCode != "000002.SZ" || res.StockItems[0].NetMfAmount != 200 {
		t.Fatalf("stock rank: %+v", res.StockItems)
	}
	if len(res.ConceptItems) != 1 || res.ConceptItems[0].Rank != 1 || res.ConceptItems[0].ConceptName != "概念甲" {
		t.Fatalf("concept rank: %+v", res.ConceptItems)
	}
}

func TestGetMoneyFlowRank_FallbackMoneyflowThs(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "readers_mf_ths.duckdb")
	factory := duckdbInfra.NewFactory()
	t.Cleanup(func() { _ = factory.Close() })
	db, err := factory.Create(domainDatastore.QuantDBConfig{
		Type: domainDatastore.DataStoreTypeDuckDB, StoragePath: dbPath,
	})
	if err != nil {
		t.Fatalf("create duckdb: %v", err)
	}
	for _, sql := range []string{
		`CREATE TABLE moneyflow_ths (
			trade_date VARCHAR, ts_code VARCHAR, name VARCHAR,
			buy_sm_amount DOUBLE, buy_md_amount DOUBLE, buy_lg_amount DOUBLE, net_amount DOUBLE
		)`,
	} {
		if _, err := db.Execute(ctx, sql); err != nil {
			t.Fatalf("ddl: %v", err)
		}
	}
	td := "20250302"
	if _, err := db.Execute(ctx, `INSERT INTO moneyflow_ths VALUES (?, '000001.SZ', 'A', 1, 2, 3, 10)`, td); err != nil {
		t.Fatal(err)
	}
	r := NewReaders(db)
	res, err := r.GetMoneyFlowRank(ctx, analysis.MoneyFlowRankRequest{Scope: "stock", TradeDate: td, Limit: 5, Offset: 0})
	if err != nil {
		t.Fatal(err)
	}
	if res.StockSource != "moneyflow_ths" || len(res.StockItems) != 1 || res.StockItems[0].DataSource != "moneyflow_ths" {
		t.Fatalf("got %+v", res)
	}
}

func TestGetIndexOHLCV_Window(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "readers_idx.duckdb")
	factory := duckdbInfra.NewFactory()
	t.Cleanup(func() { _ = factory.Close() })
	db, err := factory.Create(domainDatastore.QuantDBConfig{
		Type: domainDatastore.DataStoreTypeDuckDB, StoragePath: dbPath,
	})
	if err != nil {
		t.Fatalf("create duckdb: %v", err)
	}
	if _, err := db.Execute(ctx, `CREATE TABLE index_daily (
		ts_code VARCHAR, trade_date VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, vol DOUBLE, amount DOUBLE
	)`); err != nil {
		t.Fatal(err)
	}
	code := "000001.SH"
	for _, d := range []string{"20250301", "20250302", "20250303", "20250304", "20250305"} {
		if _, err := db.Execute(ctx, `INSERT INTO index_daily VALUES (?, ?, 1,1,1,1, 100, 1000)`, code, d); err != nil {
			t.Fatal(err)
		}
	}
	r := NewReaders(db)
	res, err := r.GetIndexOHLCV(ctx, analysis.IndexOHLCVRequest{TsCode: code, Days: 3, EndDate: "20250305"})
	if err != nil {
		t.Fatal(err)
	}
	if res.EndDate != "20250305" || len(res.Items) != 3 {
		t.Fatalf("got end=%s n=%d items=%+v", res.EndDate, len(res.Items), res.Items)
	}
	if res.Items[0].TradeDate != "20250303" || res.Items[2].TradeDate != "20250305" {
		t.Fatalf("order: %+v", res.Items)
	}
}

func TestGetIndexOHLCV_RequiresTsCode(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "readers_idx2.duckdb")
	factory := duckdbInfra.NewFactory()
	t.Cleanup(func() { _ = factory.Close() })
	db, err := factory.Create(domainDatastore.QuantDBConfig{
		Type: domainDatastore.DataStoreTypeDuckDB, StoragePath: dbPath,
	})
	if err != nil {
		t.Fatalf("create duckdb: %v", err)
	}
	r := NewReaders(db)
	_, err = r.GetIndexOHLCV(ctx, analysis.IndexOHLCVRequest{TsCode: "", Days: 10})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestListIndexSectors(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "readers_index_classify.duckdb")
	factory := duckdbInfra.NewFactory()
	t.Cleanup(func() { _ = factory.Close() })
	db, err := factory.Create(domainDatastore.QuantDBConfig{
		Type: domainDatastore.DataStoreTypeDuckDB, StoragePath: dbPath,
	})
	if err != nil {
		t.Fatalf("create duckdb: %v", err)
	}
	if _, err := db.Execute(ctx, `CREATE TABLE index_classify (
		index_code VARCHAR, industry_name VARCHAR, parent_code VARCHAR, level VARCHAR,
		industry_code VARCHAR, is_pub VARCHAR, src VARCHAR
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, `INSERT INTO index_classify VALUES
		('801010.SI', '农林牧渔', '0', 'L1', '801010', 'Y', 'SW2021'),
		('801020.SI', '采掘', '0', 'L1', '801020', 'Y', 'SW2021')`); err != nil {
		t.Fatal(err)
	}
	r := NewReaders(db)
	list, err := r.ListIndexSectors(ctx, analysis.IndexSectorListRequest{Src: strPtr("SW2021"), Limit: 10, Offset: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].IndustryName == "" {
		t.Fatalf("got %+v", list)
	}
}

func strPtr(s string) *string { return &s }

func TestListIndexSectorMembers_IndexWeight(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "readers_index_weight.duckdb")
	factory := duckdbInfra.NewFactory()
	t.Cleanup(func() { _ = factory.Close() })
	db, err := factory.Create(domainDatastore.QuantDBConfig{
		Type: domainDatastore.DataStoreTypeDuckDB, StoragePath: dbPath,
	})
	if err != nil {
		t.Fatalf("create duckdb: %v", err)
	}
	for _, sql := range []string{
		`CREATE TABLE stock_basic (ts_code VARCHAR, name VARCHAR)`,
		`CREATE TABLE index_weight (index_code VARCHAR, con_code VARCHAR, trade_date VARCHAR, weight DOUBLE)`,
	} {
		if _, err := db.Execute(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Execute(ctx, `INSERT INTO stock_basic VALUES ('000001.SZ', '平安'), ('000002.SZ', '万科')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, `INSERT INTO index_weight VALUES
		('000300.SH', '000001.SZ', '20250301', 1.5),
		('000300.SH', '000002.SZ', '20250301', 2.0)`); err != nil {
		t.Fatal(err)
	}
	r := NewReaders(db)
	list, err := r.ListIndexSectorMembers(ctx, analysis.IndexSectorMemberRequest{IndexCode: "000300.SH", TradeDate: "20250301", Limit: 10, Offset: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].DataSource != "index_weight" || list[0].ConCode == "" {
		t.Fatalf("got %+v", list)
	}
}

func TestListIndexSectorMembers_FallbackMemberAll(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "readers_index_member_all.duckdb")
	factory := duckdbInfra.NewFactory()
	t.Cleanup(func() { _ = factory.Close() })
	db, err := factory.Create(domainDatastore.QuantDBConfig{
		Type: domainDatastore.DataStoreTypeDuckDB, StoragePath: dbPath,
	})
	if err != nil {
		t.Fatalf("create duckdb: %v", err)
	}
	if _, err := db.Execute(ctx, `CREATE TABLE index_member_all (
		l1_code VARCHAR, l1_name VARCHAR, l2_code VARCHAR, l2_name VARCHAR, l3_code VARCHAR, l3_name VARCHAR,
		ts_code VARCHAR, name VARCHAR, in_date VARCHAR, out_date VARCHAR, is_new VARCHAR
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, `INSERT INTO index_member_all VALUES
		('801010.SI', '农林牧渔', '801011.SI', '子类', '801012.SI', '三级', '600000.SH', '浦发', '20200101', '', 'Y')`); err != nil {
		t.Fatal(err)
	}
	r := NewReaders(db)
	list, err := r.ListIndexSectorMembers(ctx, analysis.IndexSectorMemberRequest{IndexCode: "801012.SI", Limit: 10, Offset: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].DataSource != "index_member_all" || list[0].ConCode != "600000.SH" {
		t.Fatalf("got %+v", list)
	}
}

func TestGetDailyWithAdjFactor_UsesIndexDailyWhenDailyEmpty(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "readers_kline_index.duckdb")
	factory := duckdbInfra.NewFactory()
	t.Cleanup(func() { _ = factory.Close() })
	db, err := factory.Create(domainDatastore.QuantDBConfig{
		Type: domainDatastore.DataStoreTypeDuckDB, StoragePath: dbPath,
	})
	if err != nil {
		t.Fatalf("create duckdb: %v", err)
	}
	for _, sql := range []string{
		`CREATE TABLE daily (ts_code VARCHAR, trade_date VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, vol DOUBLE, amount DOUBLE, pre_close DOUBLE, change DOUBLE, pct_chg DOUBLE)`,
		`CREATE TABLE adj_factor (ts_code VARCHAR, trade_date VARCHAR, adj_factor DOUBLE)`,
		`CREATE TABLE stock_basic (ts_code VARCHAR, name VARCHAR)`,
	} {
		if _, err := db.Execute(ctx, sql); err != nil {
			t.Fatalf("ddl: %v", err)
		}
	}
	if _, err := db.Execute(ctx, `CREATE TABLE index_daily (
		ts_code VARCHAR, trade_date VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
		vol DOUBLE, amount DOUBLE, pre_close DOUBLE, change DOUBLE, pct_chg DOUBLE
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, `INSERT INTO index_daily VALUES
		('000001.SH', '20250301', 3000, 3010, 2990, 3005, 1e9, 1e12, 3000, 5, 0.17)`); err != nil {
		t.Fatal(err)
	}
	r := NewReaders(db)
	rows, err := r.GetDailyWithAdjFactor(ctx, "000001.SH", "20250301", "20250301")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Close != 3005 || rows[0].AdjFactor != 1.0 {
		t.Fatalf("got %+v", rows)
	}
}
