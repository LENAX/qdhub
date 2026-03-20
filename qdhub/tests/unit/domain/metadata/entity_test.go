package metadata_test

import (
	"encoding/json"
	"testing"
	"time"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
)

func TestNewDataSource(t *testing.T) {
	ds := metadata.NewDataSource("Tushare", "金融数据", "https://api.tushare.pro", "https://tushare.pro/doc")

	if ds.ID.IsEmpty() {
		t.Error("ID should not be empty")
	}
	if ds.Name != "Tushare" {
		t.Errorf("Name = %s, expected Tushare", ds.Name)
	}
	if ds.Status != shared.StatusActive {
		t.Errorf("Status = %s, expected active", ds.Status)
	}
	if ds.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}
}

func TestDataSource_ActivateDeactivate(t *testing.T) {
	ds := metadata.NewDataSource("Test", "", "", "")

	ds.Deactivate()
	if ds.Status != shared.StatusInactive {
		t.Errorf("Status after Deactivate = %s, expected inactive", ds.Status)
	}

	ds.Activate()
	if ds.Status != shared.StatusActive {
		t.Errorf("Status after Activate = %s, expected active", ds.Status)
	}
}

func TestDataSource_UpdateInfo(t *testing.T) {
	ds := metadata.NewDataSource("Old", "Old Desc", "http://old", "http://old/doc")
	originalUpdatedAt := ds.UpdatedAt

	time.Sleep(1 * time.Millisecond) // Ensure time difference
	ds.UpdateInfo("New", "New Desc", "http://new", "http://new/doc")

	if ds.Name != "New" {
		t.Errorf("Name = %s, expected New", ds.Name)
	}
	if ds.Description != "New Desc" {
		t.Errorf("Description = %s, expected New Desc", ds.Description)
	}
	if ds.UpdatedAt == originalUpdatedAt {
		t.Error("UpdatedAt should be updated")
	}
}

func TestDataSource_CommonDataAPIs(t *testing.T) {
	ds := metadata.NewDataSource("T", "", "", "")

	// Set and read
	ds.SetCommonDataAPIs([]string{"trade_cal", "stock_basic"})
	if len(ds.CommonDataAPIs) != 2 || ds.CommonDataAPIs[0] != "trade_cal" || ds.CommonDataAPIs[1] != "stock_basic" {
		t.Errorf("CommonDataAPIs = %v, expected [trade_cal stock_basic]", ds.CommonDataAPIs)
	}

	// Marshal
	jsonStr, err := ds.MarshalCommonDataAPIsJSON()
	if err != nil {
		t.Fatalf("MarshalCommonDataAPIsJSON() error = %v", err)
	}
	var decoded []string
	if err := json.Unmarshal([]byte(jsonStr), &decoded); err != nil {
		t.Fatalf("Unmarshal decoded: %v", err)
	}
	if len(decoded) != 2 || decoded[0] != "trade_cal" || decoded[1] != "stock_basic" {
		t.Errorf("decoded = %v, expected [trade_cal stock_basic]", decoded)
	}

	// Unmarshal empty
	if err := ds.UnmarshalCommonDataAPIsJSON(""); err != nil {
		t.Fatalf("UnmarshalCommonDataAPIsJSON('') error = %v", err)
	}
	if ds.CommonDataAPIs != nil {
		t.Errorf("CommonDataAPIs after empty unmarshal = %v, expected nil", ds.CommonDataAPIs)
	}

	// Unmarshal JSON
	if err := ds.UnmarshalCommonDataAPIsJSON(`["a","b"]`); err != nil {
		t.Fatalf("UnmarshalCommonDataAPIsJSON('[a,b]') error = %v", err)
	}
	if len(ds.CommonDataAPIs) != 2 || ds.CommonDataAPIs[0] != "a" || ds.CommonDataAPIs[1] != "b" {
		t.Errorf("CommonDataAPIs after unmarshal = %v, expected [a b]", ds.CommonDataAPIs)
	}

	// Unmarshal Python/手动单引号风格（与部分 GUI 导出一致）
	if err := ds.UnmarshalCommonDataAPIsJSON(`['stock_basic', 'trade_cal']`); err != nil {
		t.Fatalf("UnmarshalCommonDataAPIsJSON single-quote: %v", err)
	}
	if len(ds.CommonDataAPIs) != 2 || ds.CommonDataAPIs[0] != "stock_basic" || ds.CommonDataAPIs[1] != "trade_cal" {
		t.Errorf("CommonDataAPIs after single-quote unmarshal = %v, want [stock_basic trade_cal]", ds.CommonDataAPIs)
	}
}

func TestNewAPICategory(t *testing.T) {
	dsID := shared.NewID()
	cat := metadata.NewAPICategory(dsID, "股票", "股票数据", "/stock", nil, 1)

	if cat.ID.IsEmpty() {
		t.Error("ID should not be empty")
	}
	if cat.DataSourceID != dsID {
		t.Error("DataSourceID mismatch")
	}
	if cat.Name != "股票" {
		t.Errorf("Name = %s, expected 股票", cat.Name)
	}
	if cat.ParentID != nil {
		t.Error("ParentID should be nil")
	}
}

func TestNewAPIMetadata(t *testing.T) {
	dsID := shared.NewID()
	api := metadata.NewAPIMetadata(dsID, "daily", "日线行情", "获取日线数据", "/daily")

	if api.ID.IsEmpty() {
		t.Error("ID should not be empty")
	}
	if api.DataSourceID != dsID {
		t.Error("DataSourceID mismatch")
	}
	if api.Status != shared.StatusActive {
		t.Errorf("Status = %s, expected active", api.Status)
	}
	if len(api.RequestParams) != 0 {
		t.Error("RequestParams should be empty")
	}
	if len(api.ResponseFields) != 0 {
		t.Error("ResponseFields should be empty")
	}
}

func TestAPIMetadata_SetRequestParams(t *testing.T) {
	api := metadata.NewAPIMetadata(shared.NewID(), "test", "", "", "")
	originalUpdatedAt := api.UpdatedAt

	time.Sleep(1 * time.Millisecond)
	params := []metadata.ParamMeta{
		{Name: "ts_code", Type: "str", Required: true},
	}
	api.SetRequestParams(params)

	if len(api.RequestParams) != 1 {
		t.Errorf("RequestParams length = %d, expected 1", len(api.RequestParams))
	}
	if api.UpdatedAt == originalUpdatedAt {
		t.Error("UpdatedAt should be updated")
	}
}

func TestAPIMetadata_SetResponseFields(t *testing.T) {
	api := metadata.NewAPIMetadata(shared.NewID(), "test", "", "", "")

	fields := []metadata.FieldMeta{
		{Name: "ts_code", Type: "str", IsPrimary: true},
		{Name: "close", Type: "float"},
	}
	api.SetResponseFields(fields)

	if len(api.ResponseFields) != 2 {
		t.Errorf("ResponseFields length = %d, expected 2", len(api.ResponseFields))
	}
}

func TestAPIMetadata_SetRateLimit(t *testing.T) {
	api := metadata.NewAPIMetadata(shared.NewID(), "test", "", "", "")

	limit := &metadata.RateLimit{RequestsPerMinute: 200, PointsRequired: 2000}
	api.SetRateLimit(limit)

	if api.RateLimit == nil {
		t.Fatal("RateLimit should not be nil")
	}
	if api.RateLimit.RequestsPerMinute != 200 {
		t.Errorf("RequestsPerMinute = %d, expected 200", api.RateLimit.RequestsPerMinute)
	}
}

func TestAPIMetadata_JSONMarshaling(t *testing.T) {
	api := metadata.NewAPIMetadata(shared.NewID(), "test", "", "", "")
	api.SetRequestParams([]metadata.ParamMeta{
		{Name: "ts_code", Type: "str", Required: true, Description: "股票代码"},
	})
	api.SetResponseFields([]metadata.FieldMeta{
		{Name: "close", Type: "float", Description: "收盘价"},
	})
	api.SetRateLimit(&metadata.RateLimit{RequestsPerMinute: 100, PointsRequired: 1000})

	// Test RequestParams JSON
	paramsJSON, err := api.MarshalRequestParamsJSON()
	if err != nil {
		t.Fatalf("MarshalRequestParamsJSON error: %v", err)
	}

	api2 := metadata.NewAPIMetadata(shared.NewID(), "test2", "", "", "")
	if err := api2.UnmarshalRequestParamsJSON(paramsJSON); err != nil {
		t.Fatalf("UnmarshalRequestParamsJSON error: %v", err)
	}
	if len(api2.RequestParams) != 1 {
		t.Errorf("Unmarshaled RequestParams length = %d, expected 1", len(api2.RequestParams))
	}

	// Test ResponseFields JSON
	fieldsJSON, err := api.MarshalResponseFieldsJSON()
	if err != nil {
		t.Fatalf("MarshalResponseFieldsJSON error: %v", err)
	}

	if err := api2.UnmarshalResponseFieldsJSON(fieldsJSON); err != nil {
		t.Fatalf("UnmarshalResponseFieldsJSON error: %v", err)
	}
	if len(api2.ResponseFields) != 1 {
		t.Errorf("Unmarshaled ResponseFields length = %d, expected 1", len(api2.ResponseFields))
	}

	// Test RateLimit JSON
	limitJSON, err := api.MarshalRateLimitJSON()
	if err != nil {
		t.Fatalf("MarshalRateLimitJSON error: %v", err)
	}

	if err := api2.UnmarshalRateLimitJSON(limitJSON); err != nil {
		t.Fatalf("UnmarshalRateLimitJSON error: %v", err)
	}
	if api2.RateLimit == nil {
		t.Fatal("Unmarshaled RateLimit should not be nil")
	}

	// Test empty RateLimit
	emptyAPI := metadata.NewAPIMetadata(shared.NewID(), "empty", "", "", "")
	emptyJSON, _ := emptyAPI.MarshalRateLimitJSON()
	if emptyJSON != "" {
		t.Errorf("Empty RateLimit JSON = %s, expected empty string", emptyJSON)
	}

	if err := emptyAPI.UnmarshalRateLimitJSON(""); err != nil {
		t.Fatalf("UnmarshalRateLimitJSON empty error: %v", err)
	}
	if emptyAPI.RateLimit != nil {
		t.Error("RateLimit should be nil after unmarshaling empty string")
	}
}

func TestNewToken(t *testing.T) {
	dsID := shared.NewID()
	expiresAt := time.Now().Add(24 * time.Hour)

	token := metadata.NewToken(dsID, "encrypted_value", &expiresAt)

	if token.ID.IsEmpty() {
		t.Error("ID should not be empty")
	}
	if token.DataSourceID != dsID {
		t.Error("DataSourceID mismatch")
	}
	if token.TokenValue != "encrypted_value" {
		t.Errorf("TokenValue = %s, expected encrypted_value", token.TokenValue)
	}
}

func TestToken_IsExpired(t *testing.T) {
	dsID := shared.NewID()

	t.Run("not expired", func(t *testing.T) {
		future := time.Now().Add(24 * time.Hour)
		token := metadata.NewToken(dsID, "value", &future)

		if token.IsExpired() {
			t.Error("Token should not be expired")
		}
	})

	t.Run("expired", func(t *testing.T) {
		past := time.Now().Add(-24 * time.Hour)
		token := metadata.NewToken(dsID, "value", &past)

		if !token.IsExpired() {
			t.Error("Token should be expired")
		}
	})

	t.Run("nil expires at (never expires)", func(t *testing.T) {
		token := metadata.NewToken(dsID, "value", nil)

		if token.IsExpired() {
			t.Error("Token with nil ExpiresAt should never expire")
		}
	})
}

func TestValueObjects_JSON(t *testing.T) {
	t.Run("ParamMeta", func(t *testing.T) {
		defaultVal := "default"
		param := metadata.ParamMeta{
			Name:        "ts_code",
			Type:        "str",
			Required:    true,
			Default:     defaultVal,
			Description: "股票代码",
		}

		data, err := json.Marshal(param)
		if err != nil {
			t.Fatalf("Marshal error: %v", err)
		}

		var unmarshaled metadata.ParamMeta
		if err := json.Unmarshal(data, &unmarshaled); err != nil {
			t.Fatalf("Unmarshal error: %v", err)
		}

		if unmarshaled.Name != param.Name {
			t.Errorf("Name mismatch: %s != %s", unmarshaled.Name, param.Name)
		}
	})

	t.Run("FieldMeta", func(t *testing.T) {
		field := metadata.FieldMeta{
			Name:        "close",
			Type:        "float",
			Description: "收盘价",
			IsPrimary:   true,
			IsIndex:     true,
		}

		data, err := json.Marshal(field)
		if err != nil {
			t.Fatalf("Marshal error: %v", err)
		}

		var unmarshaled metadata.FieldMeta
		if err := json.Unmarshal(data, &unmarshaled); err != nil {
			t.Fatalf("Unmarshal error: %v", err)
		}

		if unmarshaled.IsPrimary != field.IsPrimary {
			t.Errorf("IsPrimary mismatch")
		}
	})

	t.Run("RateLimit", func(t *testing.T) {
		limit := metadata.RateLimit{
			RequestsPerMinute: 200,
			PointsRequired:    2000,
		}

		data, err := json.Marshal(limit)
		if err != nil {
			t.Fatalf("Marshal error: %v", err)
		}

		var unmarshaled metadata.RateLimit
		if err := json.Unmarshal(data, &unmarshaled); err != nil {
			t.Fatalf("Unmarshal error: %v", err)
		}

		if unmarshaled.RequestsPerMinute != limit.RequestsPerMinute {
			t.Errorf("RequestsPerMinute mismatch")
		}
	})
}

func TestDocumentType(t *testing.T) {
	if metadata.DocumentTypeHTML.String() != "html" {
		t.Errorf("DocumentTypeHTML.String() = %s, expected html", metadata.DocumentTypeHTML.String())
	}
	if metadata.DocumentTypeMarkdown.String() != "markdown" {
		t.Errorf("DocumentTypeMarkdown.String() = %s, expected markdown", metadata.DocumentTypeMarkdown.String())
	}
}
