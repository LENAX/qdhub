package metadata_test

import (
	"testing"
	"time"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
)

func TestMetadataValidator_ValidateAPIMetadata(t *testing.T) {
	validator := metadata.NewMetadataValidator()

	validAPIMetadata := func() *metadata.APIMetadata {
		return &metadata.APIMetadata{
			ID:           shared.NewID(),
			DataSourceID: shared.NewID(),
			Name:         "daily",
			DisplayName:  "日线行情",
			Description:  "获取日线行情数据",
			Endpoint:     "/daily",
			RequestParams: []metadata.ParamMeta{
				{Name: "ts_code", Type: "str", Required: true, Description: "股票代码"},
			},
			ResponseFields: []metadata.FieldMeta{
				{Name: "ts_code", Type: "str", Description: "股票代码"},
				{Name: "close", Type: "float", Description: "收盘价"},
			},
			RateLimit: &metadata.RateLimit{
				RequestsPerMinute: 200,
				PointsRequired:    2000,
			},
			Status: shared.StatusActive,
		}
	}

	tests := []struct {
		name    string
		modify  func(*metadata.APIMetadata)
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid API metadata",
			modify:  func(m *metadata.APIMetadata) {},
			wantErr: false,
		},
		{
			name:    "nil API metadata",
			modify:  func(m *metadata.APIMetadata) {},
			wantErr: true,
			errMsg:  "nil",
		},
		{
			name:    "empty ID",
			modify:  func(m *metadata.APIMetadata) { m.ID = "" },
			wantErr: true,
			errMsg:  "ID",
		},
		{
			name:    "empty DataSourceID",
			modify:  func(m *metadata.APIMetadata) { m.DataSourceID = "" },
			wantErr: true,
			errMsg:  "data source",
		},
		{
			name:    "empty name",
			modify:  func(m *metadata.APIMetadata) { m.Name = "" },
			wantErr: true,
			errMsg:  "name",
		},
		{
			name:    "empty endpoint",
			modify:  func(m *metadata.APIMetadata) { m.Endpoint = "" },
			wantErr: true,
			errMsg:  "endpoint",
		},
		{
			name:    "empty request param name",
			modify:  func(m *metadata.APIMetadata) { m.RequestParams[0].Name = "" },
			wantErr: true,
			errMsg:  "parameter",
		},
		{
			name:    "empty request param type",
			modify:  func(m *metadata.APIMetadata) { m.RequestParams[0].Type = "" },
			wantErr: true,
			errMsg:  "type",
		},
		{
			name:    "empty response fields",
			modify:  func(m *metadata.APIMetadata) { m.ResponseFields = []metadata.FieldMeta{} },
			wantErr: true,
			errMsg:  "response field",
		},
		{
			name:    "empty response field name",
			modify:  func(m *metadata.APIMetadata) { m.ResponseFields[0].Name = "" },
			wantErr: true,
			errMsg:  "field",
		},
		{
			name:    "empty response field type",
			modify:  func(m *metadata.APIMetadata) { m.ResponseFields[0].Type = "" },
			wantErr: true,
			errMsg:  "type",
		},
		{
			name:    "invalid rate limit",
			modify:  func(m *metadata.APIMetadata) { m.RateLimit.RequestsPerMinute = 0 },
			wantErr: true,
			errMsg:  "requests per minute",
		},
		{
			name:    "nil rate limit is ok",
			modify:  func(m *metadata.APIMetadata) { m.RateLimit = nil },
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var api *metadata.APIMetadata
			if tt.name == "nil API metadata" {
				api = nil
			} else {
				api = validAPIMetadata()
				tt.modify(api)
			}

			err := validator.ValidateAPIMetadata(api)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateAPIMetadata() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMetadataValidator_ValidateDataSource(t *testing.T) {
	validator := metadata.NewMetadataValidator()

	validDataSource := func() *metadata.DataSource {
		return &metadata.DataSource{
			ID:          shared.NewID(),
			Name:        "Tushare",
			Description: "Tushare 金融数据",
			BaseURL:     "https://api.tushare.pro",
			DocURL:      "https://tushare.pro/document",
			Status:      shared.StatusActive,
		}
	}

	tests := []struct {
		name    string
		modify  func(*metadata.DataSource)
		wantErr bool
	}{
		{
			name:    "valid data source",
			modify:  func(ds *metadata.DataSource) {},
			wantErr: false,
		},
		{
			name:    "nil data source",
			modify:  func(ds *metadata.DataSource) {},
			wantErr: true,
		},
		{
			name:    "empty ID",
			modify:  func(ds *metadata.DataSource) { ds.ID = "" },
			wantErr: true,
		},
		{
			name:    "empty name",
			modify:  func(ds *metadata.DataSource) { ds.Name = "" },
			wantErr: true,
		},
		{
			name:    "empty base URL",
			modify:  func(ds *metadata.DataSource) { ds.BaseURL = "" },
			wantErr: true,
		},
		{
			name:    "invalid base URL format",
			modify:  func(ds *metadata.DataSource) { ds.BaseURL = "ftp://invalid" },
			wantErr: true,
		},
		{
			name:    "invalid status",
			modify:  func(ds *metadata.DataSource) { ds.Status = "invalid" },
			wantErr: true,
		},
		{
			name:    "http base URL is valid",
			modify:  func(ds *metadata.DataSource) { ds.BaseURL = "http://api.tushare.pro" },
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ds *metadata.DataSource
			if tt.name == "nil data source" {
				ds = nil
			} else {
				ds = validDataSource()
				tt.modify(ds)
			}

			err := validator.ValidateDataSource(ds)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateDataSource() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMetadataValidator_ValidateToken(t *testing.T) {
	validator := metadata.NewMetadataValidator()

	futureTime := time.Now().Add(24 * time.Hour)
	pastTime := time.Now().Add(-24 * time.Hour)

	validToken := func() *metadata.Token {
		return &metadata.Token{
			ID:           shared.NewID(),
			DataSourceID: shared.NewID(),
			TokenValue:   "encrypted_token_value",
			ExpiresAt:    &futureTime,
		}
	}

	tests := []struct {
		name    string
		modify  func(*metadata.Token)
		wantErr bool
	}{
		{
			name:    "valid token",
			modify:  func(t *metadata.Token) {},
			wantErr: false,
		},
		{
			name:    "nil token",
			modify:  func(t *metadata.Token) {},
			wantErr: true,
		},
		{
			name:    "empty ID",
			modify:  func(t *metadata.Token) { t.ID = "" },
			wantErr: true,
		},
		{
			name:    "empty DataSourceID",
			modify:  func(t *metadata.Token) { t.DataSourceID = "" },
			wantErr: true,
		},
		{
			name:    "empty token value",
			modify:  func(t *metadata.Token) { t.TokenValue = "" },
			wantErr: true,
		},
		{
			name:    "expired token",
			modify:  func(t *metadata.Token) { t.ExpiresAt = &pastTime },
			wantErr: true,
		},
		{
			name:    "nil expires at is valid (never expires)",
			modify:  func(t *metadata.Token) { t.ExpiresAt = nil },
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var token *metadata.Token
			if tt.name == "nil token" {
				token = nil
			} else {
				token = validToken()
				tt.modify(token)
			}

			err := validator.ValidateToken(token)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateToken() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
