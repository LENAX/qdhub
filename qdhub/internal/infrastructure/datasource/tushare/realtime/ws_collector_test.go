package realtime

import "testing"

func TestParseRows_ArrayRecordBackfillsCodeAndTSCode(t *testing.T) {
	c := &TushareWSTickCollector{TargetDBPath: "/tmp/test.duckdb"}
	msg := []byte(`{
		"status": true,
		"data": {
			"code": "000001.SZ",
			"record": ["000001.SZ","平安银行","2026-03-10 09:30:01",10.01,10.12,10.00,10.20,9.98,10.10,0,1000,100000,10]
		}
	}`)

	rows, err := c.parseRows(msg)
	if err != nil {
		t.Fatalf("parseRows unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	row := rows[0]
	if row["code"] != "000001.SZ" {
		t.Fatalf("expected code=000001.SZ, got %v", row["code"])
	}
	if row["ts_code"] != "000001.SZ" {
		t.Fatalf("expected ts_code=000001.SZ, got %v", row["ts_code"])
	}
	if row["target_db_path"] != "/tmp/test.duckdb" {
		t.Fatalf("expected target_db_path injected, got %v", row["target_db_path"])
	}
}

func TestNormalizeTushareTickRecord_MapBackfillsCodeFields(t *testing.T) {
	out := normalizeTushareTickRecord(map[string]interface{}{
		"price": 10.5,
	}, "600000.SH")
	if out["code"] != "600000.SH" {
		t.Fatalf("expected code backfilled, got %v", out["code"])
	}
	if out["ts_code"] != "600000.SH" {
		t.Fatalf("expected ts_code backfilled, got %v", out["ts_code"])
	}
}
