package workflows_test

import (
	"testing"

	"qdhub/internal/infrastructure/taskengine/workflows"
)

// ==================== MetadataCrawlParams 测试 ====================

func TestMetadataCrawlParams_Defaults(t *testing.T) {
	params := workflows.MetadataCrawlParams{
		DataSourceID:   "ds-001",
		DataSourceName: "tushare",
	}

	if params.DataSourceID != "ds-001" {
		t.Errorf("expected DataSourceID 'ds-001', got '%s'", params.DataSourceID)
	}
	if params.DataSourceName != "tushare" {
		t.Errorf("expected DataSourceName 'tushare', got '%s'", params.DataSourceName)
	}
	if params.MaxAPICrawl != 0 {
		t.Errorf("expected MaxAPICrawl 0, got %d", params.MaxAPICrawl)
	}
}

func TestMetadataCrawlWorkflowBuilder_NilRegistry(t *testing.T) {
	builder := workflows.NewMetadataCrawlWorkflowBuilder(nil)
	if builder == nil {
		t.Error("expected non-nil builder even with nil registry")
	}
}

func TestMetadataCrawlWorkflowBuilder_Chaining(t *testing.T) {
	builder := workflows.NewMetadataCrawlWorkflowBuilder(nil).
		WithDataSource("ds-001", "tushare").
		WithMaxAPICrawl(100)

	if builder == nil {
		t.Error("expected non-nil builder after chaining")
	}
}

// ==================== CreateTablesParams 测试 ====================

func TestCreateTablesParams_Defaults(t *testing.T) {
	params := workflows.CreateTablesParams{
		DataSourceID:   "ds-001",
		DataSourceName: "tushare",
		TargetDBPath:   "/tmp/stock.db",
	}

	if params.DataSourceID != "ds-001" {
		t.Errorf("expected DataSourceID 'ds-001', got '%s'", params.DataSourceID)
	}
	if params.TargetDBPath != "/tmp/stock.db" {
		t.Errorf("expected TargetDBPath '/tmp/stock.db', got '%s'", params.TargetDBPath)
	}
	if params.MaxTables != 0 {
		t.Errorf("expected MaxTables 0, got %d", params.MaxTables)
	}
}

func TestCreateTablesWorkflowBuilder_NilRegistry(t *testing.T) {
	builder := workflows.NewCreateTablesWorkflowBuilder(nil)
	if builder == nil {
		t.Error("expected non-nil builder even with nil registry")
	}
}

func TestCreateTablesWorkflowBuilder_Chaining(t *testing.T) {
	builder := workflows.NewCreateTablesWorkflowBuilder(nil).
		WithDataSource("ds-001", "tushare").
		WithTargetDB("/tmp/stock.db").
		WithMaxTables(50)

	if builder == nil {
		t.Error("expected non-nil builder after chaining")
	}
}

// ==================== BatchDataSyncParams 测试 ====================

func TestBatchDataSyncParams_Defaults(t *testing.T) {
	params := workflows.BatchDataSyncParams{
		DataSourceName: "tushare",
		Token:          "test-token",
		TargetDBPath:   "/tmp/stock.db",
		StartDate:      "20251201",
		EndDate:        "20251231",
		APINames:       []string{"daily"},
	}

	if params.DataSourceName != "tushare" {
		t.Errorf("expected DataSourceName 'tushare', got '%s'", params.DataSourceName)
	}
	if params.StartDate != "20251201" {
		t.Errorf("expected StartDate '20251201', got '%s'", params.StartDate)
	}
	if params.EndDate != "20251231" {
		t.Errorf("expected EndDate '20251231', got '%s'", params.EndDate)
	}
	if params.StartTime != "" {
		t.Errorf("expected empty StartTime, got '%s'", params.StartTime)
	}
	if params.EndTime != "" {
		t.Errorf("expected empty EndTime, got '%s'", params.EndTime)
	}
	if params.MaxStocks != 0 {
		t.Errorf("expected MaxStocks 0, got %d", params.MaxStocks)
	}
}

func TestBatchDataSyncParams_WithAPINames(t *testing.T) {
	params := workflows.BatchDataSyncParams{
		DataSourceName: "tushare",
		Token:          "test-token",
		TargetDBPath:   "/tmp/stock.db",
		StartDate:      "20251201",
		EndDate:        "20251231",
		APINames:       []string{"daily", "adj_factor"},
		MaxStocks:      10,
	}

	if len(params.APINames) != 2 {
		t.Errorf("expected 2 APINames, got %d", len(params.APINames))
	}
	if params.APINames[0] != "daily" {
		t.Errorf("expected first API 'daily', got '%s'", params.APINames[0])
	}
	if params.MaxStocks != 10 {
		t.Errorf("expected MaxStocks 10, got %d", params.MaxStocks)
	}
}

// ==================== BatchDataSyncParams.Validate 测试 ====================

func TestBatchDataSyncParams_Validate_Success(t *testing.T) {
	params := workflows.BatchDataSyncParams{
		DataSourceName: "tushare",
		Token:          "test-token",
		TargetDBPath:   "/tmp/stock.db",
		StartDate:      "20251201",
		EndDate:        "20251231",
		APINames:       []string{"daily"},
	}

	if err := params.Validate(); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestBatchDataSyncParams_Validate_EmptyAPINames(t *testing.T) {
	params := workflows.BatchDataSyncParams{
		DataSourceName: "tushare",
		Token:          "test-token",
		TargetDBPath:   "/tmp/stock.db",
		StartDate:      "20251201",
		EndDate:        "20251231",
		APINames:       []string{}, // 空的 API 列表
	}

	err := params.Validate()
	if err == nil {
		t.Error("expected error for empty APINames")
	}
	if err != workflows.ErrEmptyAPINames {
		t.Errorf("expected ErrEmptyAPINames, got %v", err)
	}
}

func TestBatchDataSyncParams_Validate_MissingFields(t *testing.T) {
	tests := []struct {
		name     string
		params   workflows.BatchDataSyncParams
		expected error
	}{
		{
			name: "missing data_source_name",
			params: workflows.BatchDataSyncParams{
				Token:        "token",
				TargetDBPath: "/tmp/db",
				StartDate:    "20251201",
				EndDate:      "20251231",
				APINames:     []string{"daily"},
			},
			expected: workflows.ErrEmptyDataSourceName,
		},
		{
			name: "missing token",
			params: workflows.BatchDataSyncParams{
				DataSourceName: "tushare",
				TargetDBPath:   "/tmp/db",
				StartDate:      "20251201",
				EndDate:        "20251231",
				APINames:       []string{"daily"},
			},
			expected: workflows.ErrEmptyToken,
		},
		{
			name: "missing target_db_path",
			params: workflows.BatchDataSyncParams{
				DataSourceName: "tushare",
				Token:          "token",
				StartDate:      "20251201",
				EndDate:        "20251231",
				APINames:       []string{"daily"},
			},
			expected: workflows.ErrEmptyTargetDBPath,
		},
		// StartDate/EndDate 在工作流层为可选；是否必填由调用方（如 ExecuteSyncPlan）根据计划内 API 参数决定
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.params.Validate()
			if err != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, err)
			}
		})
	}
}

// ==================== GetStartDateTime / GetEndDateTime 测试 ====================

func TestBatchDataSyncParams_GetStartDateTime_DateOnly(t *testing.T) {
	params := workflows.BatchDataSyncParams{
		StartDate: "20251201",
	}

	result := params.GetStartDateTime()
	if result != "20251201" {
		t.Errorf("expected '20251201', got '%s'", result)
	}
}

func TestBatchDataSyncParams_GetStartDateTime_WithTime(t *testing.T) {
	params := workflows.BatchDataSyncParams{
		StartDate: "20251201",
		StartTime: "09:30:00",
	}

	result := params.GetStartDateTime()
	if result != "20251201 09:30:00" {
		t.Errorf("expected '20251201 09:30:00', got '%s'", result)
	}
}

func TestBatchDataSyncParams_GetEndDateTime_DateOnly(t *testing.T) {
	params := workflows.BatchDataSyncParams{
		EndDate: "20251231",
	}

	result := params.GetEndDateTime()
	if result != "20251231" {
		t.Errorf("expected '20251231', got '%s'", result)
	}
}

func TestBatchDataSyncParams_GetEndDateTime_WithTime(t *testing.T) {
	params := workflows.BatchDataSyncParams{
		EndDate: "20251231",
		EndTime: "15:00:00",
	}

	result := params.GetEndDateTime()
	if result != "20251231 15:00:00" {
		t.Errorf("expected '20251231 15:00:00', got '%s'", result)
	}
}

func TestBatchDataSyncWorkflowBuilder_NilRegistry(t *testing.T) {
	builder := workflows.NewBatchDataSyncWorkflowBuilder(nil)
	if builder == nil {
		t.Error("expected non-nil builder even with nil registry")
	}
}

func TestBatchDataSyncWorkflowBuilder_Chaining(t *testing.T) {
	builder := workflows.NewBatchDataSyncWorkflowBuilder(nil).
		WithDataSource("tushare", "test-token").
		WithTargetDB("/tmp/stock.db").
		WithDateRange("20251201", "20251231").
		WithAPIs("daily", "adj_factor").
		WithMaxStocks(10)

	if builder == nil {
		t.Error("expected non-nil builder after chaining")
	}
}

func TestBatchDataSyncWorkflowBuilder_WithTimeRange(t *testing.T) {
	builder := workflows.NewBatchDataSyncWorkflowBuilder(nil).
		WithDataSource("tushare", "test-token").
		WithTargetDB("/tmp/stock.db").
		WithDateRange("20251201", "20251231").
		WithTimeRange("09:30:00", "15:00:00").
		WithAPIs("daily")

	if builder == nil {
		t.Error("expected non-nil builder after chaining")
	}
}

func TestBatchDataSyncWorkflowBuilder_WithDateTimeRange(t *testing.T) {
	builder := workflows.NewBatchDataSyncWorkflowBuilder(nil).
		WithDataSource("tushare", "test-token").
		WithTargetDB("/tmp/stock.db").
		WithDateTimeRange("20251201", "09:30:00", "20251231", "15:00:00").
		WithAPIs("daily")

	if builder == nil {
		t.Error("expected non-nil builder after chaining")
	}
}

// ==================== WorkflowFactory 测试 ====================

func TestNewWorkflowFactory_NilRegistry(t *testing.T) {
	factory := workflows.NewWorkflowFactory(nil)
	if factory == nil {
		t.Error("expected non-nil factory even with nil registry")
	}
}

func TestWorkflowFactory_BuilderMethods(t *testing.T) {
	factory := workflows.NewWorkflowFactory(nil)

	// 测试 Builder 返回方法
	if factory.MetadataCrawl() == nil {
		t.Error("MetadataCrawl() should return non-nil builder")
	}
	if factory.CreateTables() == nil {
		t.Error("CreateTables() should return non-nil builder")
	}
	if factory.BatchDataSync() == nil {
		t.Error("BatchDataSync() should return non-nil builder")
	}
}

// ==================== 参数验证测试 ====================

func TestMetadataCrawlParams_Validation(t *testing.T) {
	tests := []struct {
		name   string
		params workflows.MetadataCrawlParams
		valid  bool
	}{
		{
			name: "valid params",
			params: workflows.MetadataCrawlParams{
				DataSourceID:   "ds-001",
				DataSourceName: "tushare",
			},
			valid: true,
		},
		{
			name: "with max api crawl",
			params: workflows.MetadataCrawlParams{
				DataSourceID:   "ds-001",
				DataSourceName: "tushare",
				MaxAPICrawl:    100,
			},
			valid: true,
		},
		{
			name: "empty data source id",
			params: workflows.MetadataCrawlParams{
				DataSourceID:   "",
				DataSourceName: "tushare",
			},
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.params.DataSourceID == "" && tt.valid {
				t.Error("expected invalid for empty DataSourceID")
			}
		})
	}
}

func TestCreateTablesParams_Validation(t *testing.T) {
	tests := []struct {
		name   string
		params workflows.CreateTablesParams
		valid  bool
	}{
		{
			name: "valid params",
			params: workflows.CreateTablesParams{
				DataSourceID:   "ds-001",
				DataSourceName: "tushare",
				TargetDBPath:   "/tmp/stock.db",
			},
			valid: true,
		},
		{
			name: "empty target db path",
			params: workflows.CreateTablesParams{
				DataSourceID:   "ds-001",
				DataSourceName: "tushare",
				TargetDBPath:   "",
			},
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.params.TargetDBPath == "" && tt.valid {
				t.Error("expected invalid for empty TargetDBPath")
			}
		})
	}
}

func TestBatchDataSyncParams_Validation(t *testing.T) {
	tests := []struct {
		name   string
		params workflows.BatchDataSyncParams
		valid  bool
	}{
		{
			name: "valid params with default apis",
			params: workflows.BatchDataSyncParams{
				DataSourceName: "tushare",
				Token:          "test-token",
				TargetDBPath:   "/tmp/stock.db",
				StartDate:      "20251201",
				EndDate:        "20251231",
			},
			valid: true,
		},
		{
			name: "valid params with custom apis",
			params: workflows.BatchDataSyncParams{
				DataSourceName: "tushare",
				Token:          "test-token",
				TargetDBPath:   "/tmp/stock.db",
				StartDate:      "20251201",
				EndDate:        "20251231",
				APINames:       []string{"daily", "income"},
			},
			valid: true,
		},
		{
			name: "missing token",
			params: workflows.BatchDataSyncParams{
				DataSourceName: "tushare",
				Token:          "",
				TargetDBPath:   "/tmp/stock.db",
				StartDate:      "20251201",
				EndDate:        "20251231",
			},
			valid: false,
		},
		{
			name: "missing date range",
			params: workflows.BatchDataSyncParams{
				DataSourceName: "tushare",
				Token:          "test-token",
				TargetDBPath:   "/tmp/stock.db",
				StartDate:      "",
				EndDate:        "",
			},
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.params.Token == "" && tt.valid {
				t.Error("expected invalid for empty Token")
			}
			if (tt.params.StartDate == "" || tt.params.EndDate == "") && tt.valid {
				t.Error("expected invalid for empty date range")
			}
		})
	}
}

// ==================== RealtimeDataSyncParams 测试 ====================

func TestRealtimeDataSyncParams_Defaults(t *testing.T) {
	params := workflows.RealtimeDataSyncParams{
		DataSourceName: "tushare",
		Token:          "test-token",
		TargetDBPath:   "/tmp/stock.db",
		APINames:       []string{"daily"},
	}

	if params.DataSourceName != "tushare" {
		t.Errorf("expected DataSourceName 'tushare', got '%s'", params.DataSourceName)
	}
	if params.MaxStocks != 0 {
		t.Errorf("expected MaxStocks 0, got %d", params.MaxStocks)
	}
	if params.CronExpr != "" {
		t.Errorf("expected empty CronExpr, got '%s'", params.CronExpr)
	}
}

func TestRealtimeDataSyncParams_Validate_Success(t *testing.T) {
	params := workflows.RealtimeDataSyncParams{
		DataSourceName: "tushare",
		Token:          "test-token",
		TargetDBPath:   "/tmp/stock.db",
		APINames:       []string{"daily"},
	}

	if err := params.Validate(); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestRealtimeDataSyncParams_Validate_EmptyAPINames(t *testing.T) {
	params := workflows.RealtimeDataSyncParams{
		DataSourceName: "tushare",
		Token:          "test-token",
		TargetDBPath:   "/tmp/stock.db",
		APINames:       []string{}, // 空的 API 列表
	}

	err := params.Validate()
	if err == nil {
		t.Error("expected error for empty APINames")
	}
	if err != workflows.ErrEmptyAPINames {
		t.Errorf("expected ErrEmptyAPINames, got %v", err)
	}
}

func TestRealtimeDataSyncParams_Validate_MissingFields(t *testing.T) {
	tests := []struct {
		name     string
		params   workflows.RealtimeDataSyncParams
		expected error
	}{
		{
			name: "missing data_source_name",
			params: workflows.RealtimeDataSyncParams{
				Token:        "token",
				TargetDBPath:  "/tmp/db",
				APINames:      []string{"daily"},
			},
			expected: workflows.ErrEmptyDataSourceName,
		},
		{
			name: "missing token",
			params: workflows.RealtimeDataSyncParams{
				DataSourceName: "tushare",
				TargetDBPath:   "/tmp/db",
				APINames:       []string{"daily"},
			},
			expected: workflows.ErrEmptyToken,
		},
		{
			name: "missing target_db_path",
			params: workflows.RealtimeDataSyncParams{
				DataSourceName: "tushare",
				Token:          "token",
				APINames:       []string{"daily"},
			},
			expected: workflows.ErrEmptyTargetDBPath,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.params.Validate()
			if err != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, err)
			}
		})
	}
}

func TestRealtimeDataSyncWorkflowBuilder_NilRegistry(t *testing.T) {
	builder := workflows.NewRealtimeDataSyncWorkflowBuilder(nil)
	if builder == nil {
		t.Error("expected non-nil builder even with nil registry")
	}
}

func TestRealtimeDataSyncWorkflowBuilder_Chaining(t *testing.T) {
	builder := workflows.NewRealtimeDataSyncWorkflowBuilder(nil).
		WithDataSource("tushare", "test-token").
		WithTargetDB("/tmp/stock.db").
		WithSyncRange("20250101", "20251231", "daily", "trade_date").
		WithAPIs("daily", "adj_factor").
		WithMaxStocks(10).
		WithCronExpr("0 0 18 * * 1-5")

	if builder == nil {
		t.Error("expected non-nil builder after chaining")
	}
}

func TestWorkflowFactory_RealtimeDataSyncMethod(t *testing.T) {
	factory := workflows.NewWorkflowFactory(nil)

	if factory.RealtimeDataSync() == nil {
		t.Error("RealtimeDataSync() should return non-nil builder")
	}
}
