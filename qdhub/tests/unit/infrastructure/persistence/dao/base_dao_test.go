package dao_test

import (
	"testing"

	"qdhub/internal/infrastructure/persistence/dao"
)

func TestSQLBaseDAO_TableName(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	baseDAO := dao.NewSQLBaseDAO[struct{}](db.DB, "test_table", "id")

	tableName := baseDAO.TableName()
	if tableName != "test_table" {
		t.Errorf("TableName() = %s, want test_table", tableName)
	}
}

func TestSQLBaseDAO_DB(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	baseDAO := dao.NewSQLBaseDAO[struct{}](db.DB, "test_table", "id")

	retrievedDB := baseDAO.DB()
	if retrievedDB != db.DB {
		t.Error("DB() should return the same database connection")
	}
}

func TestExecWithTx(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	// Create a test table
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS test_exec (id TEXT PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test without transaction
	result, err := dao.ExecWithTx(db.DB, nil, `INSERT INTO test_exec (id, name) VALUES (?, ?)`, "1", "test1")
	if err != nil {
		t.Fatalf("ExecWithTx() without tx error = %v", err)
	}
	if result == nil {
		t.Error("ExecWithTx() should return result")
	}

	// Test with transaction
	tx, err := db.Beginx()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	result, err = dao.ExecWithTx(db.DB, tx, `INSERT INTO test_exec (id, name) VALUES (?, ?)`, "2", "test2")
	if err != nil {
		t.Fatalf("ExecWithTx() with tx error = %v", err)
	}
	if result == nil {
		t.Error("ExecWithTx() should return result")
	}
}

func TestGetWithTx(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	// Create a test table
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS test_get (id TEXT PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO test_get (id, name) VALUES (?, ?)`, "1", "test1")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	type TestRow struct {
		ID   string `db:"id"`
		Name string `db:"name"`
	}

	// Test without transaction
	var row TestRow
	err = dao.GetWithTx(db.DB, nil, &row, `SELECT * FROM test_get WHERE id = ?`, "1")
	if err != nil {
		t.Fatalf("GetWithTx() without tx error = %v", err)
	}
	if row.ID != "1" || row.Name != "test1" {
		t.Errorf("GetWithTx() row = %+v, want {ID: 1, Name: test1}", row)
	}

	// Test with transaction
	tx, err := db.Beginx()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	var row2 TestRow
	err = dao.GetWithTx(db.DB, tx, &row2, `SELECT * FROM test_get WHERE id = ?`, "1")
	if err != nil {
		t.Fatalf("GetWithTx() with tx error = %v", err)
	}
	if row2.ID != "1" || row2.Name != "test1" {
		t.Errorf("GetWithTx() row = %+v, want {ID: 1, Name: test1}", row2)
	}
}

func TestSelectWithTx(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	// Create a test table
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS test_select (id TEXT PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO test_select (id, name) VALUES (?, ?), (?, ?)`, "1", "test1", "2", "test2")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	type TestRow struct {
		ID   string `db:"id"`
		Name string `db:"name"`
	}

	// Test without transaction
	var rows []*TestRow
	err = dao.SelectWithTx(db.DB, nil, &rows, `SELECT * FROM test_select ORDER BY id`)
	if err != nil {
		t.Fatalf("SelectWithTx() without tx error = %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("SelectWithTx() returned %d rows, want 2", len(rows))
	}

	// Test with transaction
	tx, err := db.Beginx()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	var rows2 []*TestRow
	err = dao.SelectWithTx(db.DB, tx, &rows2, `SELECT * FROM test_select ORDER BY id`)
	if err != nil {
		t.Fatalf("SelectWithTx() with tx error = %v", err)
	}
	if len(rows2) != 2 {
		t.Errorf("SelectWithTx() returned %d rows, want 2", len(rows2))
	}
}

func TestSQLBaseDAO_Get(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	// Create a test table
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS test_base_get (id TEXT PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	type TestRow struct {
		ID   string `db:"id"`
		Name string `db:"name"`
	}

	baseDAO := dao.NewSQLBaseDAO[TestRow](db.DB, "test_base_get", "id")

	// Insert test data
	_, err = db.Exec(`INSERT INTO test_base_get (id, name) VALUES (?, ?)`, "1", "test1")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Test Get without transaction
	got, err := baseDAO.Get(nil, "1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got == nil {
		t.Fatal("Get() returned nil")
	}
	if got.ID != "1" || got.Name != "test1" {
		t.Errorf("Get() = %+v, want {ID: 1, Name: test1}", got)
	}

	// Test Get with transaction
	tx, err := db.Beginx()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	got2, err := baseDAO.Get(tx, "1")
	if err != nil {
		t.Fatalf("Get() with tx error = %v", err)
	}
	if got2 == nil {
		t.Fatal("Get() with tx returned nil")
	}
	if got2.ID != "1" || got2.Name != "test1" {
		t.Errorf("Get() with tx = %+v, want {ID: 1, Name: test1}", got2)
	}

	// Test Get with non-existent ID
	got3, err := baseDAO.Get(nil, "999")
	if err != nil {
		t.Fatalf("Get() with non-existent ID error = %v", err)
	}
	if got3 != nil {
		t.Error("Get() with non-existent ID should return nil")
	}
}

func TestSQLBaseDAO_Delete(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	// Create a test table
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS test_base_delete (id TEXT PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	type TestRow struct {
		ID   string `db:"id"`
		Name string `db:"name"`
	}

	baseDAO := dao.NewSQLBaseDAO[TestRow](db.DB, "test_base_delete", "id")

	// Insert test data
	_, err = db.Exec(`INSERT INTO test_base_delete (id, name) VALUES (?, ?)`, "1", "test1")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Test Delete without transaction
	err = baseDAO.Delete(nil, "1")
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Verify deletion
	got, err := baseDAO.Get(nil, "1")
	if err != nil {
		t.Fatalf("Get() after delete error = %v", err)
	}
	if got != nil {
		t.Error("Delete() should remove the record")
	}

	// Test Delete with transaction
	_, err = db.Exec(`INSERT INTO test_base_delete (id, name) VALUES (?, ?)`, "2", "test2")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	tx, err := db.Beginx()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	err = baseDAO.Delete(tx, "2")
	if err != nil {
		t.Fatalf("Delete() with tx error = %v", err)
	}

	// Verify deletion in transaction (before commit)
	got2, err := baseDAO.Get(tx, "2")
	if err != nil {
		t.Fatalf("Get() after delete in tx error = %v", err)
	}
	if got2 != nil {
		t.Error("Delete() with tx should remove the record")
	}
}

func TestSQLBaseDAO_List(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	// Create a test table
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS test_base_list (id TEXT PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	type TestRow struct {
		ID   string `db:"id"`
		Name string `db:"name"`
	}

	baseDAO := dao.NewSQLBaseDAO[TestRow](db.DB, "test_base_list", "id")

	// Insert test data
	_, err = db.Exec(`INSERT INTO test_base_list (id, name) VALUES (?, ?), (?, ?)`, "1", "test1", "2", "test2")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Test List without transaction
	list, err := baseDAO.List(nil)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(list) < 2 {
		t.Errorf("List() returned %d rows, want at least 2", len(list))
	}

	// Verify content
	found1, found2 := false, false
	for _, row := range list {
		if row.ID == "1" && row.Name == "test1" {
			found1 = true
		}
		if row.ID == "2" && row.Name == "test2" {
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Errorf("List() missing expected rows, found1=%v, found2=%v", found1, found2)
	}

	// Test List with transaction
	tx, err := db.Beginx()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	list2, err := baseDAO.List(tx)
	if err != nil {
		tx.Rollback()
		t.Fatalf("List() with tx error = %v", err)
	}
	if len(list2) < 2 {
		tx.Rollback()
		t.Errorf("List() with tx returned %d rows, want at least 2", len(list2))
	}
	tx.Rollback() // Ensure transaction is closed before creating new table

	// Test List with empty table (create a new table to avoid transaction issues)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS test_base_list_empty (id TEXT PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create empty table: %v", err)
	}

	baseDAOEmpty := dao.NewSQLBaseDAO[TestRow](db.DB, "test_base_list_empty", "id")
	list3, err := baseDAOEmpty.List(nil)
	if err != nil {
		t.Fatalf("List() with empty table error = %v", err)
	}
	if len(list3) != 0 {
		t.Errorf("List() with empty table returned %d rows, want 0", len(list3))
	}
}
