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
	ID          shared.ID        `json:"id"`
	Name        string           `json:"name"`
	Description string           `json:"description"`
	BaseURL     string           `json:"base_url"`
	DocURL      string           `json:"doc_url"`
	Status      shared.Status    `json:"status"`
	CreatedAt   shared.Timestamp `json:"created_at"`
	UpdatedAt   shared.Timestamp `json:"updated_at"`

	// CommonDataAPIs: API names treated as common data (e.g. trade_cal, stock_basic for tushare).
	// Used for cache-first / DataStore-first reuse across workflows; persisted in data_sources.common_data_apis.
	CommonDataAPIs []string `json:"common_data_apis,omitempty"`

	// Aggregated entities (lazy loaded)
	Categories []APICategory  `json:"categories,omitempty"`
	APIs       []APIMetadata  `json:"apis,omitempty"`
	Token      *Token         `json:"token,omitempty"`
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

// SetCommonDataAPIs sets the list of API names treated as common data (reused across workflows).
func (ds *DataSource) SetCommonDataAPIs(apis []string) {
	ds.CommonDataAPIs = apis
	ds.UpdatedAt = shared.Now()
}

// MarshalCommonDataAPIsJSON marshals CommonDataAPIs to JSON string.
func (ds *DataSource) MarshalCommonDataAPIsJSON() (string, error) {
	if len(ds.CommonDataAPIs) == 0 {
		return "", nil
	}
	data, err := json.Marshal(ds.CommonDataAPIs)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalCommonDataAPIsJSON unmarshals CommonDataAPIs from JSON string.
func (ds *DataSource) UnmarshalCommonDataAPIsJSON(jsonStr string) error {
	if jsonStr == "" {
		ds.CommonDataAPIs = nil
		return nil
	}
	return json.Unmarshal([]byte(jsonStr), &ds.CommonDataAPIs)
}

// ==================== 聚合内实体 ====================

// APICategory represents an API category entity.
// Belongs to: DataSource aggregate
type APICategory struct {
	ID           shared.ID        `json:"id"`
	DataSourceID shared.ID        `json:"data_source_id"`
	Name         string           `json:"name"`
	Description  string           `json:"description"`
	ParentID     *shared.ID       `json:"parent_id,omitempty"`
	SortOrder    int              `json:"sort_order"`
	DocPath      string           `json:"doc_path"`
	CreatedAt    shared.Timestamp `json:"created_at"`
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
	ID                shared.ID        `json:"id"`
	DataSourceID      shared.ID        `json:"data_source_id"`
	CategoryID        *shared.ID      `json:"category_id,omitempty"`
	Name              string           `json:"name"`
	DisplayName       string           `json:"display_name"`
	Description       string           `json:"description"`
	Endpoint          string           `json:"endpoint"`
	RequestParams     []ParamMeta      `json:"request_params,omitempty"`
	ResponseFields    []FieldMeta      `json:"response_fields,omitempty"`
	RateLimit         *RateLimit       `json:"rate_limit,omitempty"`
	Permission        string           `json:"permission"`
	Status            shared.Status    `json:"status"`
	CreatedAt         shared.Timestamp `json:"created_at"`
	UpdatedAt         shared.Timestamp `json:"updated_at"`
	ParamDependencies []ParamDependency `json:"param_dependencies,omitempty"` // 参数依赖规则（用于自动解析 API 依赖）
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

// MarshalParamDependenciesJSON marshals param dependencies to JSON string.
func (api *APIMetadata) MarshalParamDependenciesJSON() (string, error) {
	if len(api.ParamDependencies) == 0 {
		return "", nil
	}
	data, err := json.Marshal(api.ParamDependencies)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalParamDependenciesJSON unmarshals param dependencies from JSON string.
func (api *APIMetadata) UnmarshalParamDependenciesJSON(jsonStr string) error {
	if jsonStr == "" {
		api.ParamDependencies = nil
		return nil
	}
	return json.Unmarshal([]byte(jsonStr), &api.ParamDependencies)
}

// GetSourceAPIs returns all source APIs from param dependencies.
// Used by DependencyResolver to build dependency graph.
func (api *APIMetadata) GetSourceAPIs() []string {
	seen := make(map[string]bool)
	var sources []string
	for _, dep := range api.ParamDependencies {
		if dep.SourceAPI != "" && !seen[dep.SourceAPI] {
			seen[dep.SourceAPI] = true
			sources = append(sources, dep.SourceAPI)
		}
	}
	return sources
}

// Token represents a token entity.
// Belongs to: DataSource aggregate
type Token struct {
	ID           shared.ID        `json:"id"`
	DataSourceID shared.ID        `json:"data_source_id"`
	TokenValue   string           `json:"-"` // encrypted, never serialize
	ExpiresAt    *time.Time       `json:"expires_at,omitempty"`
	CreatedAt    shared.Timestamp `json:"created_at"`
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
	Name        string `json:"name"`
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Default     any    `json:"default,omitempty"`
	Description string `json:"description"`
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

// ParamDependency 参数依赖规则（值对象）
// 定义 API 参数从哪个上游 API 的字段获取值
// 用于 DependencyResolver 自动解析 API 之间的依赖关系
type ParamDependency struct {
	ParamName   string `json:"param_name"`             // 参数名，如 "ts_code"
	SourceAPI   string `json:"source_api"`             // 来源 API，如 "stock_basic"
	SourceField string `json:"source_field"`           // 来源字段，如 "ts_code"
	IsList      bool   `json:"is_list"`                // 是否是列表（需要拆分子任务）
	FilterField string `json:"filter_field,omitempty"` // 过滤字段（可选），如 "is_open"
	FilterValue any    `json:"filter_value,omitempty"` // 过滤值（可选），如 1
}

// ==================== API 同步策略实体 ====================

// SyncParamType 同步参数类型
type SyncParamType string

const (
	// SyncParamNone 无必填参数，直接查询即可
	SyncParamNone SyncParamType = "none"
	// SyncParamTradeDate 支持 trade_date 参数，按日期查询全市场
	SyncParamTradeDate SyncParamType = "trade_date"
	// SyncParamTsCode 必须提供 ts_code，需要按股票代码拆分任务
	SyncParamTsCode SyncParamType = "ts_code"
)

// APISyncStrategy API 同步策略实体
// 定义每个 API 的同步方式，用于工作流构建
// Belongs to: DataSource aggregate (通过 data_source_id + api_name 关联)
type APISyncStrategy struct {
	ID               shared.ID             `json:"id"`
	DataSourceID     shared.ID             `json:"data_source_id"`
	APIName          string                `json:"api_name"`
	PreferredParam   SyncParamType         `json:"preferred_param"`
	SupportDateRange bool                  `json:"support_date_range"`
	RequiredParams   []string              `json:"required_params,omitempty"`
	Dependencies     []string              `json:"dependencies,omitempty"`
	// FixedParams 为该 API 请求固定追加的参数（例如 fields），通常由管理员在策略里配置。
	FixedParams      map[string]any        `json:"fixed_params,omitempty"`
	// FixedParamKeys 中的 key 将始终以 FixedParams 为准，上游调用方即便传了同名参数也会被忽略。
	FixedParamKeys   []string              `json:"fixed_param_keys,omitempty"`
	// 实时同步专用：ts_code 分片大小（0 表示非实时或不分片）
	RealtimeTsCodeChunkSize int `json:"realtime_ts_code_chunk_size,omitempty"`
	// 实时同步专用：comma_separated / single
	RealtimeTsCodeFormat string `json:"realtime_ts_code_format,omitempty"`
	// 需要迭代的参数及值列表（如 src: ["sina"]），存 DB 为 JSON
	IterateParams    map[string][]string   `json:"iterate_params,omitempty"`
	Description      string                `json:"description"`
	CreatedAt        shared.Timestamp      `json:"created_at"`
	UpdatedAt        shared.Timestamp      `json:"updated_at"`
}

// NewAPISyncStrategy 创建新的 API 同步策略
func NewAPISyncStrategy(dataSourceID shared.ID, apiName string, preferredParam SyncParamType) *APISyncStrategy {
	now := shared.Now()
	return &APISyncStrategy{
		ID:               shared.NewID(),
		DataSourceID:     dataSourceID,
		APIName:          apiName,
		PreferredParam:   preferredParam,
		SupportDateRange: false,
		RequiredParams:   []string{},
		Dependencies:     []string{},
		FixedParams:      map[string]any{},
		FixedParamKeys:   []string{},
		CreatedAt:        now,
		UpdatedAt:        now,
	}
}

// SetSupportDateRange 设置是否支持日期范围查询
func (s *APISyncStrategy) SetSupportDateRange(support bool) *APISyncStrategy {
	s.SupportDateRange = support
	s.UpdatedAt = shared.Now()
	return s
}

// SetRequiredParams 设置必需参数
func (s *APISyncStrategy) SetRequiredParams(params []string) *APISyncStrategy {
	s.RequiredParams = params
	s.UpdatedAt = shared.Now()
	return s
}

// SetDependencies 设置依赖的上游任务
func (s *APISyncStrategy) SetDependencies(deps []string) *APISyncStrategy {
	s.Dependencies = deps
	s.UpdatedAt = shared.Now()
	return s
}

// SetDescription 设置策略说明
func (s *APISyncStrategy) SetDescription(desc string) *APISyncStrategy {
	s.Description = desc
	s.UpdatedAt = shared.Now()
	return s
}

// IsDirectSync 是否可以直接同步（无需拆分任务）
func (s *APISyncStrategy) IsDirectSync() bool {
	return s.PreferredParam == SyncParamNone || s.PreferredParam == SyncParamTradeDate
}

// NeedsStockSplit 是否需要按股票代码拆分任务
func (s *APISyncStrategy) NeedsStockSplit() bool {
	return s.PreferredParam == SyncParamTsCode
}

// MarshalIterateParamsJSON 序列化 IterateParams 为 JSON 字符串
func (s *APISyncStrategy) MarshalIterateParamsJSON() (string, error) {
	if len(s.IterateParams) == 0 {
		return "", nil
	}
	data, err := json.Marshal(s.IterateParams)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalIterateParamsJSON 从 JSON 字符串反序列化 IterateParams
func (s *APISyncStrategy) UnmarshalIterateParamsJSON(jsonStr string) error {
	if jsonStr == "" {
		s.IterateParams = nil
		return nil
	}
	return json.Unmarshal([]byte(jsonStr), &s.IterateParams)
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

