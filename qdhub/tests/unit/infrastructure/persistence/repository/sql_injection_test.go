package repository_test

import (
	"os"
	"testing"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
)

// ==================== SQL 注入防护测试 ====================
// 本文件专门测试 SQL 注入防护机制，包括：
// 1. IsSafeFieldName - 字段名安全检查
// 2. FieldWhitelist - 字段白名单验证
// 3. queryBuilder - 参数化查询构建
// 4. Repository 层对恶意输入的处理

// ==================== IsSafeFieldName 测试 ====================

func TestIsSafeFieldName_ValidFieldNames(t *testing.T) {
	validNames := []string{
		"id",
		"name",
		"created_at",
		"updated_at",
		"user_id",
		"data_source_id",
		"api_meta_id",
		"workflow_def_id",
		"_private",
		"field123",
		"UPPER_CASE",
		"mixedCase",
	}

	for _, name := range validNames {
		if !shared.IsSafeFieldName(name) {
			t.Errorf("IsSafeFieldName(%q) = false, want true", name)
		}
	}
}

func TestIsSafeFieldName_InvalidFieldNames(t *testing.T) {
	invalidNames := []struct {
		name   string
		reason string
	}{
		// SQL 注入攻击向量
		{"id; DROP TABLE users;--", "SQL 注入: 包含分号和注释"},
		{"id OR 1=1", "SQL 注入: 包含 OR 条件"},
		{"id' OR '1'='1", "SQL 注入: 单引号注入"},
		{"id\" OR \"1\"=\"1", "SQL 注入: 双引号注入"},
		{"id); DELETE FROM users;--", "SQL 注入: 括号闭合攻击"},
		{"id UNION SELECT * FROM users", "SQL 注入: UNION 攻击"},

		// 特殊字符
		{"field-name", "包含连字符"},
		{"field.name", "包含点号"},
		{"field name", "包含空格"},
		{"field\tname", "包含制表符"},
		{"field\nname", "包含换行符"},
		{"field`name`", "包含反引号"},
		{"field[0]", "包含方括号"},
		{"field()", "包含圆括号"},
		{"field$var", "包含美元符号"},
		{"field@name", "包含 @ 符号"},
		{"field#name", "包含井号"},
		{"field%name", "包含百分号"},
		{"field*", "包含星号"},
		{"field+name", "包含加号"},
		{"field=name", "包含等号"},
		{"field<>name", "包含比较运算符"},
		{"field!name", "包含感叹号"},

		// 数字开头
		{"123field", "数字开头"},
		{"0_field", "数字开头"},

		// 空值和过长字段名
		{"", "空字符串"},
		{string(make([]byte, 100)), "超长字段名 (>64 字符)"},

		// Unicode 字符
		{"字段名", "中文字符"},
		{"フィールド", "日文字符"},
		{"поле", "俄文字符"},
		{"field🔥", "表情符号"},
	}

	for _, tc := range invalidNames {
		if shared.IsSafeFieldName(tc.name) {
			t.Errorf("IsSafeFieldName(%q) = true, want false (reason: %s)", tc.name, tc.reason)
		}
	}
}

// ==================== FieldWhitelist 测试 ====================

func TestFieldWhitelist_IsAllowed(t *testing.T) {
	whitelist := shared.NewFieldWhitelist("sync_jobs", "id", "name", "status", "mode", "created_at")

	t.Run("Allowed fields", func(t *testing.T) {
		allowedFields := []string{"id", "name", "status", "mode", "created_at"}
		for _, field := range allowedFields {
			if !whitelist.IsAllowed(field) {
				t.Errorf("IsAllowed(%q) = false, want true", field)
			}
		}
	})

	t.Run("Disallowed fields", func(t *testing.T) {
		disallowedFields := []string{
			"password",
			"secret",
			"token",
			"unknown_field",
			"id; DROP TABLE",
			"",
		}
		for _, field := range disallowedFields {
			if whitelist.IsAllowed(field) {
				t.Errorf("IsAllowed(%q) = true, want false", field)
			}
		}
	})
}

func TestFieldWhitelist_ValidateConditions(t *testing.T) {
	whitelist := shared.NewFieldWhitelist("sync_jobs", "id", "name", "status")

	t.Run("Valid conditions", func(t *testing.T) {
		conditions := []shared.QueryCondition{
			shared.Eq("id", "123"),
			shared.Eq("name", "test"),
			shared.Eq("status", "enabled"),
		}
		if err := whitelist.ValidateConditions(conditions...); err != nil {
			t.Errorf("ValidateConditions() error = %v, want nil", err)
		}
	})

	t.Run("Invalid field in conditions", func(t *testing.T) {
		conditions := []shared.QueryCondition{
			shared.Eq("id", "123"),
			shared.Eq("password", "secret"), // 不在白名单中
		}
		if err := whitelist.ValidateConditions(conditions...); err == nil {
			t.Error("ValidateConditions() error = nil, want error for disallowed field")
		}
	})

	t.Run("SQL injection in field name", func(t *testing.T) {
		conditions := []shared.QueryCondition{
			shared.Eq("id; DROP TABLE users;--", "123"),
		}
		if err := whitelist.ValidateConditions(conditions...); err == nil {
			t.Error("ValidateConditions() should reject SQL injection in field name")
		}
	})
}

func TestFieldWhitelist_ValidateOrderBy(t *testing.T) {
	whitelist := shared.NewFieldWhitelist("sync_jobs", "id", "name", "created_at")

	t.Run("Valid order by", func(t *testing.T) {
		orderBy := []shared.OrderBy{
			shared.Asc("name"),
			shared.Desc("created_at"),
		}
		if err := whitelist.ValidateOrderBy(orderBy); err != nil {
			t.Errorf("ValidateOrderBy() error = %v, want nil", err)
		}
	})

	t.Run("Invalid field in order by", func(t *testing.T) {
		orderBy := []shared.OrderBy{
			shared.Asc("name"),
			shared.Desc("unknown_field"),
		}
		if err := whitelist.ValidateOrderBy(orderBy); err == nil {
			t.Error("ValidateOrderBy() error = nil, want error for disallowed field")
		}
	})

	t.Run("SQL injection in order by field", func(t *testing.T) {
		orderBy := []shared.OrderBy{
			{Field: "name; DROP TABLE users;--", Order: shared.SortAsc},
		}
		if err := whitelist.ValidateOrderBy(orderBy); err == nil {
			t.Error("ValidateOrderBy() should reject SQL injection in field name")
		}
	})
}

// ==================== Repository 层 SQL 注入测试 ====================

// setupSQLInjectionTestDB 创建用于 SQL 注入测试的临时数据库
func setupSQLInjectionTestDB(t *testing.T) (*persistence.DB, func()) {
	tmpfile, err := os.CreateTemp("", "test_sql_injection_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()

	dsn := tmpfile.Name()
	db, err := persistence.NewDB(dsn)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// 创建测试表
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS sync_jobs (
			id VARCHAR(64) PRIMARY KEY,
			name VARCHAR(128) NOT NULL,
			description TEXT,
			api_meta_id VARCHAR(64) NOT NULL,
			data_store_id VARCHAR(64) NOT NULL,
			workflow_def_id VARCHAR(64),
			mode VARCHAR(32) NOT NULL,
			cron_expression VARCHAR(128),
			params TEXT,
			param_rules TEXT,
			status VARCHAR(32) DEFAULT 'disabled',
			last_run_at TIMESTAMP,
			next_run_at TIMESTAMP,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		CREATE TABLE IF NOT EXISTS sync_executions (
			id VARCHAR(64) PRIMARY KEY,
			sync_job_id VARCHAR(64) NOT NULL,
			workflow_inst_id VARCHAR(64),
			status VARCHAR(32) NOT NULL,
			started_at TIMESTAMP NOT NULL,
			finished_at TIMESTAMP,
			record_count INTEGER DEFAULT 0,
			error_message TEXT,
			FOREIGN KEY (sync_job_id) REFERENCES sync_jobs(id) ON DELETE CASCADE
		);

		-- 创建一个敏感表，用于测试 SQL 注入是否能访问
		CREATE TABLE IF NOT EXISTS sensitive_data (
			id VARCHAR(64) PRIMARY KEY,
			secret_value TEXT
		);
		INSERT INTO sensitive_data (id, secret_value) VALUES ('1', 'TOP_SECRET_DATA');
	`)
	if err != nil {
		db.Close()
		t.Fatalf("Failed to create tables: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(dsn)
	}

	return db, cleanup
}

// TestRepository_SQLInjection_InFieldName 测试通过字段名进行 SQL 注入
func TestRepository_SQLInjection_InFieldName(t *testing.T) {
	db, cleanup := setupSQLInjectionTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	// 创建测试数据
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	if err := repo.Create(job); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// SQL 注入攻击向量 - 通过字段名
	injectionAttacks := []struct {
		name  string
		field string
		value interface{}
	}{
		{
			name:  "DROP TABLE 攻击",
			field: "id; DROP TABLE sync_jobs;--",
			value: "test",
		},
		{
			name:  "UNION SELECT 攻击",
			field: "id UNION SELECT * FROM sensitive_data--",
			value: "test",
		},
		{
			name:  "OR 1=1 攻击",
			field: "id OR 1=1--",
			value: "test",
		},
		{
			name:  "子查询攻击",
			field: "id IN (SELECT id FROM sensitive_data)",
			value: "test",
		},
		{
			name:  "括号闭合攻击",
			field: "id); DELETE FROM sync_jobs WHERE ('1'='1",
			value: "test",
		},
	}

	for _, tc := range injectionAttacks {
		t.Run(tc.name, func(t *testing.T) {
			// 使用恶意字段名查询 - 应该被过滤或返回空结果
			conditions := []shared.QueryCondition{
				{Field: tc.field, Operator: shared.OpEqual, Value: tc.value},
			}

			// FindBy 应该安全地处理恶意输入（跳过非法字段或返回空结果）
			results, err := repo.FindBy(conditions...)
			// 不应该产生严重错误（如 panic 或数据库错误）
			// 结果应该为空，因为字段被过滤
			if err != nil {
				// 某些数据库驱动可能会因为语法错误返回错误，这也是可接受的
				t.Logf("FindBy() returned error (acceptable): %v", err)
			}

			// 关键：确保数据仍然存在（没有被 DROP）
			allJobs, err := repo.List()
			if err != nil {
				t.Fatalf("List() error after injection attempt: %v", err)
			}
			if len(allJobs) == 0 {
				t.Error("Data was deleted - SQL injection may have succeeded!")
			}

			// 验证敏感数据未泄露（检查返回的数据不包含 sensitive_data）
			for _, result := range results {
				if result.Name == "TOP_SECRET_DATA" || result.Description == "TOP_SECRET_DATA" {
					t.Error("Sensitive data was leaked through SQL injection!")
				}
			}
		})
	}
}

// TestRepository_SQLInjection_InValue 测试通过值进行 SQL 注入
func TestRepository_SQLInjection_InValue(t *testing.T) {
	db, cleanup := setupSQLInjectionTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	// 创建测试数据
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	if err := repo.Create(job); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// SQL 注入攻击向量 - 通过值
	injectionAttacks := []struct {
		name  string
		value string
	}{
		{"单引号注入", "test' OR '1'='1"},
		{"双引号注入", "test\" OR \"1\"=\"1"},
		{"注释攻击", "test'--"},
		{"分号攻击", "test'; DROP TABLE sync_jobs;--"},
		{"UNION 攻击", "test' UNION SELECT * FROM sensitive_data--"},
		{"十六进制编码", "0x74657374"},
		{"URL 编码", "%27%20OR%20%271%27%3D%271"},
	}

	for _, tc := range injectionAttacks {
		t.Run(tc.name, func(t *testing.T) {
			// 使用合法字段名但恶意值进行查询
			results, err := repo.FindBy(shared.Eq("name", tc.value))

			// 参数化查询应该安全处理这些值
			// 查询应该正常执行，只是找不到匹配项
			if err != nil {
				t.Fatalf("FindBy() error = %v", err)
			}

			// 应该返回空结果（因为没有匹配的记录）
			if len(results) > 0 {
				t.Logf("Unexpected results found: %d", len(results))
			}

			// 关键：确保数据仍然存在
			allJobs, err := repo.List()
			if err != nil {
				t.Fatalf("List() error after injection attempt: %v", err)
			}
			if len(allJobs) == 0 {
				t.Error("Data was deleted - SQL injection may have succeeded!")
			}
		})
	}
}

// TestRepository_SQLInjection_InLikePattern 测试 LIKE 操作符中的 SQL 注入
func TestRepository_SQLInjection_InLikePattern(t *testing.T) {
	db, cleanup := setupSQLInjectionTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	// 创建测试数据
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	if err := repo.Create(job); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// LIKE 模式注入攻击
	likeInjections := []struct {
		name    string
		pattern string
	}{
		{"通配符泄露", "%"},
		{"下划线通配", "_____"},
		{"转义攻击", "%' OR '1'='1"},
		{"SQL 注入", "%'; DROP TABLE sync_jobs;--"},
	}

	for _, tc := range likeInjections {
		t.Run(tc.name, func(t *testing.T) {
			results, err := repo.FindBy(shared.Like("name", tc.pattern))

			if err != nil {
				t.Logf("FindBy(Like) returned error: %v", err)
			}

			// 验证数据仍然存在
			allJobs, err := repo.List()
			if err != nil {
				t.Fatalf("List() error after injection attempt: %v", err)
			}
			if len(allJobs) == 0 {
				t.Error("Data was deleted - SQL injection may have succeeded!")
			}

			// 验证 LIKE 通配符不会返回敏感数据
			for _, r := range results {
				if r.Name == "TOP_SECRET_DATA" {
					t.Error("LIKE pattern leaked sensitive data!")
				}
			}
		})
	}
}

// TestRepository_SQLInjection_InOrderBy 测试 ORDER BY 中的 SQL 注入
func TestRepository_SQLInjection_InOrderBy(t *testing.T) {
	db, cleanup := setupSQLInjectionTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	// 创建测试数据
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()

	for i := 0; i < 3; i++ {
		job := sync.NewSyncJob("Test Job", "Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
		if err := repo.Create(job); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
	}

	// ORDER BY 注入攻击
	orderByInjections := []struct {
		name  string
		field string
	}{
		{"DROP TABLE 攻击", "name; DROP TABLE sync_jobs;--"},
		{"CASE 注入", "CASE WHEN 1=1 THEN name ELSE id END"},
		{"子查询注入", "(SELECT secret_value FROM sensitive_data LIMIT 1)"},
		{"UNION 注入", "name UNION SELECT secret_value FROM sensitive_data"},
	}

	for _, tc := range orderByInjections {
		t.Run(tc.name, func(t *testing.T) {
			// 恶意 ORDER BY 字段应该被过滤
			orderBy := []shared.OrderBy{
				{Field: tc.field, Order: shared.SortAsc},
			}

			_, err := repo.FindByWithOrder(orderBy)
			if err != nil {
				t.Logf("FindByWithOrder() returned error (acceptable): %v", err)
			}

			// 验证数据仍然存在
			allJobs, err := repo.List()
			if err != nil {
				t.Fatalf("List() error after injection attempt: %v", err)
			}
			if len(allJobs) == 0 {
				t.Error("Data was deleted - SQL injection may have succeeded!")
			}
		})
	}
}

// TestRepository_SQLInjection_InINClause 测试 IN 子句中的 SQL 注入
func TestRepository_SQLInjection_InINClause(t *testing.T) {
	db, cleanup := setupSQLInjectionTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	// 创建测试数据
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()
	job := sync.NewSyncJob("Test Job", "Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
	if err := repo.Create(job); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// IN 子句注入攻击
	t.Run("IN clause with malicious values", func(t *testing.T) {
		maliciousValues := []string{
			"test",
			"'; DROP TABLE sync_jobs;--",
			"' OR '1'='1",
		}

		results, err := repo.FindBy(shared.In("name", maliciousValues))
		if err != nil {
			t.Logf("FindBy(In) returned error: %v", err)
		}

		// 验证数据仍然存在
		allJobs, err := repo.List()
		if err != nil {
			t.Fatalf("List() error after injection attempt: %v", err)
		}
		if len(allJobs) == 0 {
			t.Error("Data was deleted - SQL injection may have succeeded!")
		}

		// 验证没有返回超出预期的数据
		if len(results) > 1 {
			t.Logf("Unexpected number of results: %d", len(results))
		}
	})
}

// TestRepository_SQLInjection_Pagination 测试分页参数的 SQL 注入防护
func TestRepository_SQLInjection_Pagination(t *testing.T) {
	db, cleanup := setupSQLInjectionTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	// 创建测试数据
	apiMetaID := shared.NewID()
	dataStoreID := shared.NewID()
	workflowDefID := shared.NewID()

	for i := 0; i < 5; i++ {
		job := sync.NewSyncJob("Test Job", "Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
		repo.Create(job)
	}

	t.Run("Negative page number", func(t *testing.T) {
		pagination := shared.NewPagination(-1, 10)
		result, err := repo.ListWithPagination(pagination)
		if err != nil {
			t.Fatalf("ListWithPagination() error = %v", err)
		}
		// 应该被规范化为有效值
		if result.Page < 1 {
			t.Errorf("Page = %d, should be >= 1", result.Page)
		}
	})

	t.Run("Negative page size", func(t *testing.T) {
		pagination := shared.NewPagination(1, -10)
		result, err := repo.ListWithPagination(pagination)
		if err != nil {
			t.Fatalf("ListWithPagination() error = %v", err)
		}
		// 应该被规范化为有效值
		if result.PageSize < 1 {
			t.Errorf("PageSize = %d, should be >= 1", result.PageSize)
		}
	})

	t.Run("Very large page size", func(t *testing.T) {
		pagination := shared.NewPagination(1, 1000000)
		result, err := repo.ListWithPagination(pagination)
		if err != nil {
			t.Fatalf("ListWithPagination() error = %v", err)
		}
		// 应该被限制为最大值
		if result.PageSize > 100 {
			t.Errorf("PageSize = %d, should be <= 100", result.PageSize)
		}
	})
}

// ==================== 边界条件测试 ====================

func TestRepository_EdgeCases(t *testing.T) {
	db, cleanup := setupSQLInjectionTestDB(t)
	defer cleanup()

	repo := repository.NewSyncJobRepository(db)

	t.Run("Empty conditions", func(t *testing.T) {
		results, err := repo.FindBy()
		if err != nil {
			t.Fatalf("FindBy() with no conditions error = %v", err)
		}
		// 应该返回所有记录
		t.Logf("Results without conditions: %d", len(results))
	})

	t.Run("Empty IN clause", func(t *testing.T) {
		results, err := repo.FindBy(shared.In("name", []string{}))
		if err != nil {
			t.Fatalf("FindBy(In) with empty slice error = %v", err)
		}
		// 空 IN 子句应该返回空结果
		if len(results) != 0 {
			t.Errorf("Empty IN clause should return 0 results, got %d", len(results))
		}
	})

	t.Run("Nil value in condition", func(t *testing.T) {
		// 测试 IS NULL 条件
		results, err := repo.FindBy(shared.IsNull("cron_expression"))
		if err != nil {
			t.Fatalf("FindBy(IsNull) error = %v", err)
		}
		t.Logf("Results with NULL cron_expression: %d", len(results))
	})

	t.Run("Special characters in valid value", func(t *testing.T) {
		apiMetaID := shared.NewID()
		dataStoreID := shared.NewID()
		workflowDefID := shared.NewID()

		// 创建包含特殊字符的有效数据
		specialName := "Test Job with 'quotes' and \"double quotes\" and <brackets>"
		job := sync.NewSyncJob(specialName, "Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
		if err := repo.Create(job); err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		// 查询应该能正确处理特殊字符
		results, err := repo.FindBy(shared.Eq("name", specialName))
		if err != nil {
			t.Fatalf("FindBy() error = %v", err)
		}
		if len(results) != 1 {
			t.Errorf("FindBy() returned %d results, want 1", len(results))
		}
		if len(results) > 0 && results[0].Name != specialName {
			t.Errorf("Name = %q, want %q", results[0].Name, specialName)
		}
	})

	t.Run("Unicode in value", func(t *testing.T) {
		apiMetaID := shared.NewID()
		dataStoreID := shared.NewID()
		workflowDefID := shared.NewID()

		// 创建包含 Unicode 的数据
		unicodeName := "测试任务 🚀 テスト"
		job := sync.NewSyncJob(unicodeName, "Description", apiMetaID, dataStoreID, workflowDefID, sync.SyncModeBatch)
		if err := repo.Create(job); err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		// 查询应该能正确处理 Unicode
		results, err := repo.FindBy(shared.Eq("name", unicodeName))
		if err != nil {
			t.Fatalf("FindBy() error = %v", err)
		}
		if len(results) != 1 {
			t.Errorf("FindBy() returned %d results, want 1", len(results))
		}
	})
}

// ==================== ValidateFieldNamesSafe 测试 ====================

func TestValidateFieldNamesSafe(t *testing.T) {
	t.Run("All valid fields", func(t *testing.T) {
		err := shared.ValidateFieldNamesSafe("id", "name", "status", "created_at")
		if err != nil {
			t.Errorf("ValidateFieldNamesSafe() error = %v, want nil", err)
		}
	})

	t.Run("One invalid field", func(t *testing.T) {
		err := shared.ValidateFieldNamesSafe("id", "name; DROP TABLE", "status")
		if err == nil {
			t.Error("ValidateFieldNamesSafe() error = nil, want error for invalid field")
		}
	})

	t.Run("Empty fields list", func(t *testing.T) {
		err := shared.ValidateFieldNamesSafe()
		if err != nil {
			t.Errorf("ValidateFieldNamesSafe() with no fields error = %v, want nil", err)
		}
	})
}
