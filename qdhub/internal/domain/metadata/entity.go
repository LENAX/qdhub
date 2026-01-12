// Package metadata contains the metadata domain entities.
package metadata

import (
	"encoding/json"
	"time"

	"qdhub/internal/domain/shared"
)

// ==================== 聚合根 ====================

// DataSource represents a data source aggregate root.
// Responsibilities:
//   - Manage data source configuration
//   - Manage API categories, metadata, and tokens
type DataSource struct {
	ID          shared.ID
	Name        string
	Description string
	BaseURL     string
	DocURL      string
	Status      shared.Status
	CreatedAt   shared.Timestamp
	UpdatedAt   shared.Timestamp

	// Aggregated entities (lazy loaded)
	Categories []APICategory
	APIs       []APIMetadata
	Token      *Token
}

// NewDataSource creates a new DataSource aggregate.
func NewDataSource(name, description, baseURL, docURL string) *DataSource {
	now := shared.Now()
	return &DataSource{
		ID:          shared.NewID(),
		Name:        name,
		Description: description,
		BaseURL:     baseURL,
		DocURL:      docURL,
		Status:      shared.StatusActive,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// Activate activates the data source.
func (ds *DataSource) Activate() {
	ds.Status = shared.StatusActive
	ds.UpdatedAt = shared.Now()
}

// Deactivate deactivates the data source.
func (ds *DataSource) Deactivate() {
	ds.Status = shared.StatusInactive
	ds.UpdatedAt = shared.Now()
}

// UpdateInfo updates the data source information.
func (ds *DataSource) UpdateInfo(name, description, baseURL, docURL string) {
	ds.Name = name
	ds.Description = description
	ds.BaseURL = baseURL
	ds.DocURL = docURL
	ds.UpdatedAt = shared.Now()
}

// ==================== 聚合内实体 ====================

// APICategory represents an API category entity.
// Belongs to: DataSource aggregate
type APICategory struct {
	ID           shared.ID
	DataSourceID shared.ID
	Name         string
	Description  string
	ParentID     *shared.ID
	SortOrder    int
	DocPath      string
	CreatedAt    shared.Timestamp
}

// NewAPICategory creates a new APICategory.
func NewAPICategory(dataSourceID shared.ID, name, description, docPath string, parentID *shared.ID, sortOrder int) *APICategory {
	return &APICategory{
		ID:           shared.NewID(),
		DataSourceID: dataSourceID,
		Name:         name,
		Description:  description,
		ParentID:     parentID,
		SortOrder:    sortOrder,
		DocPath:      docPath,
		CreatedAt:    shared.Now(),
	}
}

// APIMetadata represents an API metadata entity.
// Belongs to: DataSource aggregate
type APIMetadata struct {
	ID             shared.ID
	DataSourceID   shared.ID
	CategoryID     *shared.ID
	Name           string
	DisplayName    string
	Description    string
	Endpoint       string
	RequestParams  []ParamMeta
	ResponseFields []FieldMeta
	RateLimit      *RateLimit
	Permission     string
	Status         shared.Status
	CreatedAt      shared.Timestamp
	UpdatedAt      shared.Timestamp
}

// NewAPIMetadata creates a new APIMetadata.
func NewAPIMetadata(dataSourceID shared.ID, name, displayName, description, endpoint string) *APIMetadata {
	now := shared.Now()
	return &APIMetadata{
		ID:             shared.NewID(),
		DataSourceID:   dataSourceID,
		Name:           name,
		DisplayName:    displayName,
		Description:    description,
		Endpoint:       endpoint,
		RequestParams:  []ParamMeta{},
		ResponseFields: []FieldMeta{},
		Status:         shared.StatusActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

// SetRequestParams sets the request parameters.
func (api *APIMetadata) SetRequestParams(params []ParamMeta) {
	api.RequestParams = params
	api.UpdatedAt = shared.Now()
}

// SetResponseFields sets the response fields.
func (api *APIMetadata) SetResponseFields(fields []FieldMeta) {
	api.ResponseFields = fields
	api.UpdatedAt = shared.Now()
}

// SetRateLimit sets the rate limit.
func (api *APIMetadata) SetRateLimit(limit *RateLimit) {
	api.RateLimit = limit
	api.UpdatedAt = shared.Now()
}

// MarshalRequestParamsJSON marshals request params to JSON string.
func (api *APIMetadata) MarshalRequestParamsJSON() (string, error) {
	data, err := json.Marshal(api.RequestParams)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalRequestParamsJSON unmarshals request params from JSON string.
func (api *APIMetadata) UnmarshalRequestParamsJSON(jsonStr string) error {
	return json.Unmarshal([]byte(jsonStr), &api.RequestParams)
}

// MarshalResponseFieldsJSON marshals response fields to JSON string.
func (api *APIMetadata) MarshalResponseFieldsJSON() (string, error) {
	data, err := json.Marshal(api.ResponseFields)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalResponseFieldsJSON unmarshals response fields from JSON string.
func (api *APIMetadata) UnmarshalResponseFieldsJSON(jsonStr string) error {
	return json.Unmarshal([]byte(jsonStr), &api.ResponseFields)
}

// MarshalRateLimitJSON marshals rate limit to JSON string.
func (api *APIMetadata) MarshalRateLimitJSON() (string, error) {
	if api.RateLimit == nil {
		return "", nil
	}
	data, err := json.Marshal(api.RateLimit)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalRateLimitJSON unmarshals rate limit from JSON string.
func (api *APIMetadata) UnmarshalRateLimitJSON(jsonStr string) error {
	if jsonStr == "" {
		api.RateLimit = nil
		return nil
	}
	var limit RateLimit
	if err := json.Unmarshal([]byte(jsonStr), &limit); err != nil {
		return err
	}
	api.RateLimit = &limit
	return nil
}

// Token represents a token entity.
// Belongs to: DataSource aggregate
type Token struct {
	ID           shared.ID
	DataSourceID shared.ID
	TokenValue   string // encrypted
	ExpiresAt    *time.Time
	CreatedAt    shared.Timestamp
}

// NewToken creates a new Token.
func NewToken(dataSourceID shared.ID, tokenValue string, expiresAt *time.Time) *Token {
	return &Token{
		ID:           shared.NewID(),
		DataSourceID: dataSourceID,
		TokenValue:   tokenValue,
		ExpiresAt:    expiresAt,
		CreatedAt:    shared.Now(),
	}
}

// IsExpired checks if the token is expired.
func (t *Token) IsExpired() bool {
	if t.ExpiresAt == nil {
		return false
	}
	return time.Now().After(*t.ExpiresAt)
}

// ==================== 值对象 ====================

// ParamMeta represents parameter metadata (value object).
type ParamMeta struct {
	Name        string  `json:"name"`
	Type        string  `json:"type"`
	Required    bool    `json:"required"`
	Default     *string `json:"default,omitempty"`
	Description string  `json:"description"`
}

// FieldMeta represents field metadata (value object).
type FieldMeta struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
	IsPrimary   bool   `json:"is_primary"`
	IsIndex     bool   `json:"is_index"`
}

// RateLimit represents rate limit information (value object).
type RateLimit struct {
	RequestsPerMinute int `json:"requests_per_minute"`
	PointsRequired    int `json:"points_required"`
}

// ==================== 枚举类型 ====================

// DocumentType represents the document type.
type DocumentType string

const (
	DocumentTypeHTML     DocumentType = "html"
	DocumentTypeMarkdown DocumentType = "markdown"
)

// String returns the string representation of the document type.
func (dt DocumentType) String() string {
	return string(dt)
}

// ==================== 独立实体 ====================

// DataTypeMappingRule represents data type mapping rule (independent entity).
// Belongs to: Metadata domain
// Responsibilities:
//   - Manage mapping rules from data source field types to target database types
type DataTypeMappingRule struct {
	ID             shared.ID
	DataSourceType string
	SourceType     string
	TargetDBType   string
	TargetType     string
	FieldPattern   *string
	Priority       int
	IsDefault      bool
	CreatedAt      shared.Timestamp
	UpdatedAt      shared.Timestamp
}

// NewDataTypeMappingRule creates a new DataTypeMappingRule.
func NewDataTypeMappingRule(dataSourceType, sourceType, targetDBType, targetType string, priority int, isDefault bool) *DataTypeMappingRule {
	now := shared.Now()
	return &DataTypeMappingRule{
		ID:             shared.NewID(),
		DataSourceType: dataSourceType,
		SourceType:     sourceType,
		TargetDBType:   targetDBType,
		TargetType:     targetType,
		Priority:       priority,
		IsDefault:      isDefault,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

// SetFieldPattern sets the field pattern for the mapping rule.
func (rule *DataTypeMappingRule) SetFieldPattern(pattern string) {
	rule.FieldPattern = &pattern
	rule.UpdatedAt = shared.Now()
}
