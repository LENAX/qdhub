# QDHub 设计文档

> 版本：v1.3  
> 更新时间：2026-01-12

---

## 1. 项目概述

### 1.1 项目定位

QDHub 是一个基于 Task Engine 的量化数据管理系统，提供元数据管理、自动建表、数据同步、分析和可视化功能。

### 1.2 核心价值

| 痛点 | 解决方案 |
|------|----------|
| 数据源 API 繁多，手动对接成本高 | 元数据自动爬取解析，统一管理 |
| 表结构设计重复劳动 | 根据元数据自动建表 |
| 数据同步调度复杂 | 工作流驱动，自动调度 |
| 并发控制、失败重试难 | Task Engine 统一管理 |

### 1.3 技术选型

| 组件 | 技术 | 说明 |
|------|------|------|
| HTTP API | Gin | 轻量高性能 |
| 系统数据库 | SQLite / PostgreSQL | 开发用 SQLite，生产用 PostgreSQL |
| 数据库交互 | sqlx | 原生 SQL + 结构体映射 |
| 工作流引擎 | task-engine | 自研工作流框架 |
| 量化数据库 | DuckDB | 主力 OLAP，后续扩展 ClickHouse |
| 数据源 | Tushare / AKShare | 主力 Tushare，后续扩展 |
| 前端 | Vue + NaiveUI | 已有成品 |

---

## 2. 系统架构

### 2.1 分层架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        Presentation Layer                        │
│                    (Gin HTTP API / Vue Frontend)                 │
└─────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                       Application Layer                          │
│              (Use Cases / Application Services)                  │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌──────────────┐  │
│  │MetadataApp │ │DataStoreApp│ │  SyncApp   │ │ WorkflowApp  │  │
│  └────────────┘ └────────────┘ └────────────┘ └──────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Domain Layer                             │
│                    (Entities / Aggregates / Services)            │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌──────────────┐  │
│  │ DataSource │ │QuantData-  │ │  SyncJob   │ │ WorkflowDef  │  │
│  │  (聚合根)  │ │   Store    │ │  (聚合根)  │ │  (聚合根)    │  │
│  │            │ │  (聚合根)  │ │            │ │              │  │
│  └────────────┘ └────────────┘ └────────────┘ └──────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Infrastructure Layer                        │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    Repository Layer                       │   │
│  │  ┌─────────────────────────────────────────────────────┐ │   │
│  │  │              Generic Repository<T>                   │ │   │
│  │  │         (通用 CRUD: Create/Get/Update/Delete)        │ │   │
│  │  └─────────────────────────────────────────────────────┘ │   │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────┐ │   │
│  │  │ MetadataRepo│ │DataStoreRepo│ │ SyncRepo   │ │WfRepo  │ │   │
│  │  │(聚合根操作) │ │(聚合根操作) │ │(聚合根操作)│ │        │ │   │
│  │  └────────────┘ └────────────┘ └────────────┘ └────────┘ │   │
│  │  ┌─────────────────────────────────────────────────────┐ │   │
│  │  │                     DAO Layer                        │ │   │
│  │  │  DataSourceDAO | APICategoryDAO | APIMetadataDAO ... │ │   │
│  │  │              (单表操作，无业务逻辑)                   │ │   │
│  │  └─────────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────────┘   │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────────────────┐ │
│  │  DataSource  │ │   QuantDB    │ │     Task Engine          │ │
│  │   Adapters   │ │   Adapters   │ │     Integration          │ │
│  └──────────────┘ └──────────────┘ └──────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 模块划分

```
qdhub/
├── cmd/                          # 入口
│   └── server/
│       └── main.go
├── internal/
│   ├── domain/                   # 领域层
│   │   ├── metadata/             # 元数据聚合
│   │   │   ├── entity.go         # 实体定义
│   │   │   ├── repository.go     # 仓储接口（聚合根级别）
│   │   │   └── service.go        # 领域服务
│   │   ├── datastore/            # 量化数据存储聚合
│   │   │   ├── entity.go
│   │   │   ├── repository.go
│   │   │   └── service.go
│   │   ├── sync/                 # 同步任务聚合
│   │   │   ├── entity.go
│   │   │   ├── repository.go
│   │   │   └── service.go
│   │   ├── workflow/             # 工作流管理聚合
│   │   │   ├── entity.go
│   │   │   ├── repository.go
│   │   │   └── service.go
│   │   └── shared/               # 共享内核
│   │       ├── types.go          # 通用类型定义
│   │       └── repository.go     # 通用仓储接口
│   ├── application/              # 应用层
│   │   ├── metadata_app.go
│   │   ├── datastore_app.go
│   │   ├── sync_app.go
│   │   └── workflow_app.go
│   ├── infrastructure/           # 基础设施层
│   │   ├── persistence/          # 持久化
│   │   │   ├── dao/              # DAO 层（单表操作）
│   │   │   │   ├── base_dao.go   # 通用 DAO
│   │   │   │   ├── datasource_dao.go          # Metadata 领域
│   │   │   │   ├── api_category_dao.go
│   │   │   │   ├── api_metadata_dao.go
│   │   │   │   ├── token_dao.go
│   │   │   │   ├── quant_datastore_dao.go     # DataStore 领域
│   │   │   │   ├── table_schema_dao.go
│   │   │   │   ├── data_type_mapping_rule_dao.go
│   │   │   │   ├── sync_job_dao.go            # Sync 领域
│   │   │   │   ├── sync_execution_dao.go
│   │   │   │   ├── workflow_definition_dao.go # Workflow 领域
│   │   │   │   └── workflow_instance_dao.go
│   │   │   ├── repository/       # Repository 实现（聚合根操作）
│   │   │   │   ├── generic_repo.go
│   │   │   │   ├── metadata_repo.go
│   │   │   │   ├── datastore_repo.go
│   │   │   │   ├── sync_repo.go
│   │   │   │   └── workflow_repo.go
│   │   │   ├── sqlite/           # SQLite 具体实现
│   │   │   └── postgres/         # PostgreSQL 具体实现
│   │   ├── datasource/           # 数据源适配器
│   │   │   ├── tushare/
│   │   │   ├── akshare/
│   │   │   └── parser/           # 文档解析器
│   │   │       ├── factory.go    # 解析器工厂
│   │   │       ├── html_parser.go
│   │   │       └── markdown_parser.go
│   │   ├── quantdb/              # 量化数据库适配器
│   │   │   ├── duckdb/
│   │   │   └── clickhouse/
│   │   └── taskengine/           # Task Engine 集成
│   │       ├── jobs/             # Job Functions
│   │       ├── handlers/         # Task Handlers
│   │       └── definitions/      # Workflow 定义
│   └── interfaces/               # 接口层
│       ├── http/
│       │   ├── handler/
│       │   ├── middleware/
│       │   └── router.go
│       └── dto/                  # 数据传输对象
├── pkg/                          # 公共包
│   ├── config/
│   ├── crypto/                   # 加密工具
│   └── typemap/                  # 类型映射
├── configs/                      # 配置文件
└── migrations/                   # 数据库迁移
```

---

## 3. 领域模型

### 3.0 聚合根概览

| 领域 | 聚合根 | 聚合内实体 | 值对象 | 独立实体 |
|------|--------|-----------|--------|----------|
| **Metadata** | `DataSource` | APICategory, APIMetadata, Token | ParamMeta, FieldMeta, RateLimit | - |
| **QuantDataStore** | `QuantDataStore` | TableSchema | ColumnDef, IndexDef | DataTypeMappingRule |
| **Sync** | `SyncJob` | SyncExecution | ParamRule | - |
| **Workflow** | `WorkflowDefinition` | WorkflowInstance, TaskInstance | - | - |

> **独立实体**：不属于任何聚合根，有独立的生命周期和仓储，但归属于特定领域管理。

> **聚合根职责**：聚合根是聚合的入口点，外部只能通过聚合根访问聚合内的实体。聚合根负责维护聚合内的一致性。

---

### 3.1 元数据领域 (Metadata)

> **聚合根**：`DataSource`  
> **一致性边界**：一个数据源及其下的所有 API 目录、API 元数据、访问令牌

```go
// ==================== 聚合根 ====================

// DataSource 数据源（聚合根）
// 职责：管理数据源配置、API 目录、API 元数据、访问令牌
// 一致性规则：
//   - 删除数据源时，级联删除其下所有 Category、APIMetadata、Token
//   - Token 必须关联有效的 DataSource
type DataSource struct {
    ID          string    `db:"id"`
    Name        string    `db:"name"`
    Description string    `db:"description"`
    BaseURL     string    `db:"base_url"`
    DocURL      string    `db:"doc_url"`
    Status      Status    `db:"status"`
    CreatedAt   time.Time `db:"created_at"`
    UpdatedAt   time.Time `db:"updated_at"`
    
    // 聚合内实体（按需加载）
    Categories  []APICategory  // API 目录列表
    APIs        []APIMetadata  // API 元数据列表
    Token       *Token         // 访问令牌
}

// ==================== 聚合内实体 ====================

// APICategory API 目录分类（聚合内实体）
// 归属：DataSource 聚合
type APICategory struct {
    ID           string  `db:"id"`
    DataSourceID string  `db:"data_source_id"` // 外键，关联聚合根
    Name         string  `db:"name"`
    Description  string  `db:"description"`
    ParentID     *string `db:"parent_id"`
    SortOrder    int     `db:"sort_order"`
    DocPath      string  `db:"doc_path"`
}

// APIMetadata API 元数据（聚合内实体）
// 归属：DataSource 聚合
// 说明：虽然是核心业务实体，但其生命周期由 DataSource 管理
type APIMetadata struct {
    ID             string      `db:"id"`
    DataSourceID   string      `db:"data_source_id"` // 外键，关联聚合根
    CategoryID     string      `db:"category_id"`
    Name           string      `db:"name"`
    DisplayName    string      `db:"display_name"`
    Description    string      `db:"description"`
    Endpoint       string      `db:"endpoint"`
    RequestParams  []ParamMeta // 值对象列表
    ResponseFields []FieldMeta // 值对象列表
    RateLimit      *RateLimit  // 值对象
    Permission     string      `db:"permission"`
    Status         Status      `db:"status"`
    CreatedAt      time.Time   `db:"created_at"`
    UpdatedAt      time.Time   `db:"updated_at"`
}

// Token 数据源访问令牌（聚合内实体）
// 归属：DataSource 聚合
type Token struct {
    ID           string     `db:"id"`
    DataSourceID string     `db:"data_source_id"` // 外键，关联聚合根
    TokenValue   string     `db:"token_value"`    // 加密存储
    ExpiresAt    *time.Time `db:"expires_at"`
    CreatedAt    time.Time  `db:"created_at"`
}

// ==================== 值对象 ====================

// ParamMeta 参数元数据（值对象）
type ParamMeta struct {
    Name        string  `json:"name"`
    Type        string  `json:"type"`
    Required    bool    `json:"required"`
    Default     *string `json:"default,omitempty"`
    Description string  `json:"description"`
}

// FieldMeta 字段元数据（值对象）
type FieldMeta struct {
    Name        string `json:"name"`
    Type        string `json:"type"`
    Description string `json:"description"`
    IsPrimary   bool   `json:"is_primary"`
    IsIndex     bool   `json:"is_index"`
}

// RateLimit 频率限制（值对象）
type RateLimit struct {
    RequestsPerMinute int `json:"requests_per_minute"`
    PointsRequired    int `json:"points_required"`
}

// ==================== 领域接口 ====================

// DocumentType 文档类型
type DocumentType string
const (
    DocumentTypeHTML     DocumentType = "html"
    DocumentTypeMarkdown DocumentType = "markdown"
)

// DocumentParser 文档解析器接口（领域层定义，基础设施层实现）
// 职责：解析不同格式的数据源文档，提取 API 目录和元数据
type DocumentParser interface {
    // ParseCatalog 解析目录结构
    // 返回：分类列表、API 详情页 URL 列表
    ParseCatalog(content string) ([]APICategory, []string, error)
    
    // ParseAPIDetail 解析 API 详情
    ParseAPIDetail(content string) (*APIMetadata, error)
    
    // SupportedType 返回支持的文档类型
    SupportedType() DocumentType
}

// DocumentParserFactory 文档解析器工厂接口
type DocumentParserFactory interface {
    // GetParser 根据文档类型获取对应的解析器
    GetParser(docType DocumentType) (DocumentParser, error)
    
    // RegisterParser 注册解析器
    RegisterParser(parser DocumentParser)
}
```

---

### 3.2 量化数据存储领域 (QuantDataStore)

> **聚合根**：`QuantDataStore`  
> **一致性边界**：一个数据存储及其下的所有表结构定义

```go
// ==================== 聚合根 ====================

// QuantDataStore 量化数据存储配置（聚合根）
// 职责：管理数据存储连接配置、表结构定义
// 一致性规则：
//   - 删除数据存储时，级联删除其下所有 TableSchema
//   - TableSchema 的 DataStoreID 必须引用有效的 QuantDataStore
type QuantDataStore struct {
    ID          string        `db:"id"`
    Name        string        `db:"name"`
    Type        DataStoreType `db:"type"`
    DSN         string        `db:"dsn"`          // 加密存储
    StoragePath string        `db:"storage_path"` // 仅文件型数据库
    Status      Status        `db:"status"`
    CreatedAt   time.Time     `db:"created_at"`
    UpdatedAt   time.Time     `db:"updated_at"`
    
    // 聚合内实体（按需加载）
    Schemas     []TableSchema // 表结构列表
}

// ==================== 聚合内实体 ====================

// TableSchema 表结构定义（聚合内实体）
// 归属：QuantDataStore 聚合
type TableSchema struct {
    ID           string       `db:"id"`
    DataStoreID  string       `db:"data_store_id"` // 外键，关联聚合根
    APIMetaID    string       `db:"api_meta_id"`   // 跨聚合引用
    TableName    string       `db:"table_name"`
    Columns      []ColumnDef  // 值对象列表
    PrimaryKeys  []string     // 主键列名
    Indexes      []IndexDef   // 值对象列表
    Status       SchemaStatus `db:"status"`
    CreatedAt    time.Time    `db:"created_at"`
    ErrorMessage *string      `db:"error_message"`
}

// ==================== 值对象 ====================

// ColumnDef 列定义（值对象）
type ColumnDef struct {
    Name       string  `json:"name"`
    SourceType string  `json:"source_type"`
    TargetType string  `json:"target_type"`
    Nullable   bool    `json:"nullable"`
    Default    *string `json:"default,omitempty"`
    Comment    string  `json:"comment"`
}

// IndexDef 索引定义（值对象）
type IndexDef struct {
    Name    string   `json:"name"`
    Columns []string `json:"columns"`
    Unique  bool     `json:"unique"`
}

// ==================== 枚举类型 ====================

// DataStoreType 数据存储类型
type DataStoreType string
const (
    DataStoreTypeDuckDB     DataStoreType = "duckdb"
    DataStoreTypeClickHouse DataStoreType = "clickhouse"
    DataStoreTypePostgreSQL DataStoreType = "postgres"
)

// SchemaStatus 表结构状态
type SchemaStatus string
const (
    SchemaStatusPending SchemaStatus = "pending"
    SchemaStatusCreated SchemaStatus = "created"
    SchemaStatusFailed  SchemaStatus = "failed"
)

// ==================== 独立实体 ====================

// DataTypeMappingRule 数据类型映射规则（独立实体）
// 说明：不属于任何聚合根，有独立的生命周期
// 职责：管理数据源字段类型到目标数据库类型的映射规则
// 使用场景：
//   - 根据 API 元数据生成 TableSchema 时，查询匹配的映射规则
//   - 支持按字段名模式（正则）进行精确类型映射
//   - 优先级高的规则优先匹配
type DataTypeMappingRule struct {
    ID             string        `db:"id"`
    DataSourceType string        `db:"data_source_type"` // tushare, akshare 等
    SourceType     string        `db:"source_type"`      // 源数据类型：str, float, int, date...
    TargetDBType   DataStoreType `db:"target_db_type"`   // 目标数据库类型
    TargetType     string        `db:"target_type"`      // 目标数据类型：VARCHAR, DOUBLE...
    FieldPattern   *string       `db:"field_pattern"`    // 可选，字段名正则模式
    Priority       int           `db:"priority"`         // 优先级，越高越优先匹配
    IsDefault      bool          `db:"is_default"`       // 是否系统默认规则
    CreatedAt      time.Time     `db:"created_at"`
    UpdatedAt      time.Time     `db:"updated_at"`
}
```

---

### 3.3 同步任务领域 (Sync)

> **聚合根**：`SyncJob`  
> **一致性边界**：一个同步任务及其所有执行记录

```go
// ==================== 聚合根 ====================

// SyncJob 同步任务（聚合根）
// 职责：管理同步任务配置、执行记录、调度策略
// 一致性规则：
//   - SyncExecution 必须关联有效的 SyncJob
//   - 任务运行中时不允许删除
//   - CronExpression 变更时需同步更新 Task Engine 调度器
type SyncJob struct {
    ID             string         `db:"id"`
    Name           string         `db:"name"`
    Description    string         `db:"description"`
    APIMetaID      string         `db:"api_meta_id"`      // 跨聚合引用
    DataStoreID    string         `db:"data_store_id"`    // 跨聚合引用
    WorkflowDefID  string         `db:"workflow_def_id"`  // 关联的工作流定义
    Mode           SyncMode       `db:"mode"`
    CronExpression *string        `db:"cron_expression"`  // Cron 表达式，如 "0 0 9 * * *"
    Params         map[string]any // 固定参数
    ParamRules     []ParamRule    // 值对象列表
    Status         JobStatus      `db:"status"`
    LastRunAt      *time.Time     `db:"last_run_at"`
    NextRunAt      *time.Time     `db:"next_run_at"`
    CreatedAt      time.Time      `db:"created_at"`
    UpdatedAt      time.Time      `db:"updated_at"`
    
    // 聚合内实体（按需加载）
    Executions     []SyncExecution // 执行历史
}

// ==================== 聚合内实体 ====================

// SyncExecution 同步执行记录（聚合内实体）
// 归属：SyncJob 聚合
type SyncExecution struct {
    ID               string     `db:"id"`
    SyncJobID        string     `db:"sync_job_id"` // 外键，关联聚合根
    WorkflowInstID   string     `db:"workflow_inst_id"` // 跨聚合引用
    Status           ExecStatus `db:"status"`
    StartedAt        time.Time  `db:"started_at"`
    FinishedAt       *time.Time `db:"finished_at"`
    RecordCount      int64      `db:"record_count"`
    ErrorMessage     *string    `db:"error_message"`
}

// ==================== 值对象 ====================

// ParamRule 参数规则（值对象）
type ParamRule struct {
    ParamName  string `json:"param_name"`
    RuleType   string `json:"rule_type"`
    RuleConfig any    `json:"rule_config"`
}

// ==================== 枚举类型 ====================

// SyncMode 同步模式
type SyncMode string
const (
    SyncModeBatch    SyncMode = "batch"
    SyncModeRealtime SyncMode = "realtime"
)

// JobStatus 任务状态
type JobStatus string
const (
    JobStatusEnabled  JobStatus = "enabled"
    JobStatusDisabled JobStatus = "disabled"
    JobStatusRunning  JobStatus = "running"
)

// ExecStatus 执行状态
type ExecStatus string
const (
    ExecStatusPending   ExecStatus = "pending"
    ExecStatusRunning   ExecStatus = "running"
    ExecStatusSuccess   ExecStatus = "success"
    ExecStatusFailed    ExecStatus = "failed"
    ExecStatusCancelled ExecStatus = "cancelled"
)
```

### 3.4 工作流管理领域 (Workflow)

> **聚合根**：`WorkflowDefinition`  
> **一致性边界**：一个工作流定义及其所有实例、任务实例

```go
// ==================== 聚合根 ====================

// WorkflowDefinition 工作流定义（聚合根）
// 职责：管理工作流定义、实例、任务实例
// 一致性规则：
//   - WorkflowInstance 必须关联有效的 WorkflowDefinition
//   - 禁用的工作流定义不能创建新实例
//   - 删除定义时需检查是否有运行中的实例
type WorkflowDefinition struct {
    ID             string      `db:"id"`
    Name           string      `db:"name"`
    Description    string      `db:"description"`
    Category       WfCategory  `db:"category"`
    DefinitionYAML string      `db:"definition_yaml"` // YAML 定义
    Version        int         `db:"version"`
    Status         WfDefStatus `db:"status"`
    IsSystem       bool        `db:"is_system"`       // 系统内置工作流
    CreatedAt      time.Time   `db:"created_at"`
    UpdatedAt      time.Time   `db:"updated_at"`
    
    // 聚合内实体（按需加载）
    Instances      []WorkflowInstance // 实例列表
}

// ==================== 聚合内实体 ====================

// WorkflowInstance 工作流实例（聚合内实体）
// 归属：WorkflowDefinition 聚合
type WorkflowInstance struct {
    ID               string         `db:"id"`
    WorkflowDefID    string         `db:"workflow_def_id"`    // 外键，关联聚合根
    EngineInstanceID string         `db:"engine_instance_id"` // Task Engine 实例 ID
    TriggerType      TriggerType    `db:"trigger_type"`
    TriggerParams    map[string]any // 触发参数
    Status           WfInstStatus   `db:"status"`
    Progress         float64        `db:"progress"`
    StartedAt        time.Time      `db:"started_at"`
    FinishedAt       *time.Time     `db:"finished_at"`
    ErrorMessage     *string        `db:"error_message"`
    
    // 聚合内实体
    TaskInstances    []TaskInstance // 任务实例列表
}

// TaskInstance 任务实例（聚合内实体）
// 归属：WorkflowDefinition 聚合（通过 WorkflowInstance）
// 说明：简化视图，详细信息从 Task Engine 获取
type TaskInstance struct {
    ID             string     `db:"id"`
    WorkflowInstID string     `db:"workflow_inst_id"` // 外键，关联 WorkflowInstance
    TaskName       string     `db:"task_name"`
    Status         TaskStatus `db:"status"`
    StartedAt      *time.Time `db:"started_at"`
    FinishedAt     *time.Time `db:"finished_at"`
    RetryCount     int        `db:"retry_count"`
    ErrorMessage   *string    `db:"error_message"`
}

// ==================== 枚举类型 ====================

// WfCategory 工作流分类
type WfCategory string
const (
    WfCategoryMetadata WfCategory = "metadata"
    WfCategorySync     WfCategory = "sync"
    WfCategoryCustom   WfCategory = "custom"
)

// WfDefStatus 工作流定义状态
type WfDefStatus string
const (
    WfDefStatusEnabled  WfDefStatus = "enabled"
    WfDefStatusDisabled WfDefStatus = "disabled"
)

// TriggerType 触发类型
type TriggerType string
const (
    TriggerTypeManual TriggerType = "manual"
    TriggerTypeCron   TriggerType = "cron"
    TriggerTypeEvent  TriggerType = "event"
)

// WfInstStatus 工作流实例状态
type WfInstStatus string
const (
    WfInstStatusPending   WfInstStatus = "pending"
    WfInstStatusRunning   WfInstStatus = "running"
    WfInstStatusPaused    WfInstStatus = "paused"
    WfInstStatusSuccess   WfInstStatus = "success"
    WfInstStatusFailed    WfInstStatus = "failed"
    WfInstStatusCancelled WfInstStatus = "cancelled"
)

// TaskStatus 任务状态
type TaskStatus string
const (
    TaskStatusPending  TaskStatus = "pending"
    TaskStatusRunning  TaskStatus = "running"
    TaskStatusSuccess  TaskStatus = "success"
    TaskStatusFailed   TaskStatus = "failed"
    TaskStatusSkipped  TaskStatus = "skipped"
)
```

---

## 4. 领域服务详细设计

### 4.1 元数据领域服务 (MetadataService)

```go
// MetadataService 元数据领域服务接口
type MetadataService interface {
    // ==================== 数据源管理 ====================
    
    // RegisterDataSource 注册新数据源
    // - 验证数据源配置有效性
    // - 创建数据源记录
    RegisterDataSource(ctx context.Context, ds *DataSource) error
    
    // GetDataSource 获取数据源详情
    GetDataSource(ctx context.Context, id string) (*DataSource, error)
    
    // ListDataSources 列出所有数据源
    ListDataSources(ctx context.Context) ([]DataSource, error)
    
    // ==================== 元数据刷新 ====================
    
    // RefreshMetadata 刷新数据源元数据
    // - 触发元数据刷新工作流
    // - 返回工作流实例 ID 用于追踪进度
    // 核心流程：
    //   1. 验证数据源存在且 Token 有效
    //   2. 创建元数据刷新工作流实例
    //   3. 提交工作流到 Task Engine
    //   4. 返回工作流实例 ID
    RefreshMetadata(ctx context.Context, dataSourceID string) (workflowInstID string, err error)
    
    // GetRefreshStatus 获取刷新进度
    GetRefreshStatus(ctx context.Context, workflowInstID string) (*RefreshProgress, error)
    
    // ==================== API 目录管理 ====================
    
    // ListCategories 列出 API 目录（树形结构）
    // - 返回层级化的目录结构
    ListCategories(ctx context.Context, dataSourceID string) ([]CategoryTree, error)
    
    // ==================== API 元数据查询 ====================
    
    // GetAPIMetadata 获取 API 详情
    // - 包含请求参数、返回字段等完整信息
    GetAPIMetadata(ctx context.Context, id string) (*APIMetadata, error)
    
    // ListAPIs 列出数据源的所有 API
    // - 支持按分类筛选
    // - 支持关键词搜索
    ListAPIs(ctx context.Context, query APIQuery) ([]APIMetadata, error)
    
    // SearchAPIs 搜索 API
    // - 按名称、描述模糊搜索
    SearchAPIs(ctx context.Context, keyword string) ([]APIMetadata, error)
    
    // ==================== Token 管理 ====================
    
    // SetToken 设置数据源 Token
    // - Token 加密后存储
    // - 验证 Token 有效性（可选）
    SetToken(ctx context.Context, dataSourceID string, token string) error
    
    // ValidateToken 验证 Token 有效性
    // - 调用数据源 API 验证
    ValidateToken(ctx context.Context, dataSourceID string) (bool, error)
    
    // GetTokenStatus 获取 Token 状态（不返回实际值）
    GetTokenStatus(ctx context.Context, dataSourceID string) (*TokenStatus, error)
}

// RefreshProgress 刷新进度
type RefreshProgress struct {
    WorkflowInstID string
    Status         WfInstStatus
    Progress       float64
    CurrentStep    string
    TotalAPIs      int
    ProcessedAPIs  int
    ErrorMessage   *string
}

// CategoryTree 目录树
type CategoryTree struct {
    Category APICategory
    Children []CategoryTree
}

// APIQuery API 查询条件
type APIQuery struct {
    DataSourceID *string
    CategoryID   *string
    Keyword      *string
    Status       *Status
    Limit        int
    Offset       int
}

// TokenStatus Token 状态
type TokenStatus struct {
    HasToken  bool
    IsValid   bool
    ExpiresAt *time.Time
}
```

### 4.2 量化数据存储领域服务 (QuantDataStoreService)

```go
// QuantDataStoreService 量化数据存储领域服务接口
type QuantDataStoreService interface {
    // ==================== 数据存储配置管理 ====================
    
    // CreateDataStore 创建数据存储配置
    // - 验证连接参数有效性
    // - 测试连接可用性
    // - 创建配置记录
    CreateDataStore(ctx context.Context, ds *QuantDataStore) error
    
    // GetDataStore 获取数据存储详情
    GetDataStore(ctx context.Context, id string) (*QuantDataStore, error)
    
    // ListDataStores 列出所有数据存储
    ListDataStores(ctx context.Context) ([]QuantDataStore, error)
    
    // TestConnection 测试数据存储连接
    TestConnection(ctx context.Context, id string) error
    
    // DeleteDataStore 删除数据存储
    // - 检查是否有关联的同步任务
    // - 级联删除表结构定义
    DeleteDataStore(ctx context.Context, id string) error
    
    // ==================== 表结构生成 ====================
    
    // GenerateSchema 根据 API 元数据生成表结构
    // 核心流程：
    //   1. 获取 API 元数据的 ResponseFields
    //   2. 根据目标数据库类型进行类型映射
    //   3. 识别主键和索引字段
    //   4. 生成 TableSchema
    GenerateSchema(ctx context.Context, apiMetaID, dataStoreID string) (*TableSchema, error)
    
    // GenerateSchemaBatch 批量生成表结构
    // - 根据数据源批量生成所有 API 的表结构
    GenerateSchemaBatch(ctx context.Context, dataSourceID, dataStoreID string) ([]TableSchema, error)
    
    // GetSchema 获取表结构定义
    GetSchema(ctx context.Context, schemaID string) (*TableSchema, error)
    
    // ListSchemas 列出数据存储的所有表结构
    ListSchemas(ctx context.Context, dataStoreID string) ([]TableSchema, error)
    
    // ==================== 建表操作 ====================
    
    // CreateTable 执行建表
    // 核心流程：
    //   1. 获取 TableSchema 定义
    //   2. 获取 QuantDataStore 连接
    //   3. 生成 DDL SQL
    //   4. 执行建表
    //   5. 更新 Schema 状态
    CreateTable(ctx context.Context, schemaID string) error
    
    // CreateTableBatch 批量建表
    // - 按依赖顺序执行建表
    CreateTableBatch(ctx context.Context, schemaIDs []string) ([]CreateTableResult, error)
    
    // DropTable 删除表
    DropTable(ctx context.Context, schemaID string) error
    
    // GetTableStats 获取表统计信息
    // - 行数、大小、最后更新时间等
    GetTableStats(ctx context.Context, schemaID string) (*TableStats, error)
}

// CreateTableResult 批量建表结果
type CreateTableResult struct {
    SchemaID string
    Success  bool
    Error    *string
}

// TableStats 表统计信息
type TableStats struct {
    RowCount       int64
    SizeBytes      int64
    LastUpdatedAt  *time.Time
}
```

### 4.3 同步任务领域服务 (SyncService)

```go
// SyncService 同步任务领域服务接口
type SyncService interface {
    // ==================== 同步任务管理 ====================
    
    // CreateSyncJob 创建同步任务
    // 核心流程：
    //   1. 验证 API 和数据存储存在
    //   2. 验证参数规则合法性
    //   3. 创建 SyncJob 记录
    //   4. 如果有 Cron 表达式，注册定时任务
    CreateSyncJob(ctx context.Context, job *SyncJob) error
    
    // GetSyncJob 获取同步任务详情
    GetSyncJob(ctx context.Context, id string) (*SyncJob, error)
    
    // ListSyncJobs 列出同步任务
    ListSyncJobs(ctx context.Context, query SyncJobQuery) ([]SyncJob, error)
    
    // UpdateSyncJob 更新同步任务
    // - 更新参数、调度规则等
    // - 如果调度规则变更，需要更新定时任务
    UpdateSyncJob(ctx context.Context, job *SyncJob) error
    
    // DeleteSyncJob 删除同步任务
    // - 取消关联的定时任务
    // - 保留历史执行记录
    DeleteSyncJob(ctx context.Context, id string) error
    
    // ==================== 任务控制 ====================
    
    // EnableJob 启用同步任务
    // - 注册定时任务（如果有 Cron 表达式）
    EnableJob(ctx context.Context, id string) error
    
    // DisableJob 禁用同步任务
    // - 取消定时任务
    // - 不影响正在运行的执行
    DisableJob(ctx context.Context, id string) error
    
    // ==================== 同步执行 ====================
    
    // TriggerSync 手动触发同步
    // 核心流程：
    //   1. 获取 SyncJob 配置
    //   2. 创建 SyncExecution 记录
    //   3. 构建同步工作流
    //   4. 提交工作流到 Task Engine
    //   5. 返回 SyncExecution
    TriggerSync(ctx context.Context, jobID string) (*SyncExecution, error)
    
    // TriggerSyncWithParams 使用自定义参数触发同步
    // - 覆盖任务默认参数
    TriggerSyncWithParams(ctx context.Context, jobID string, params map[string]any) (*SyncExecution, error)
    
    // ==================== 执行监控 ====================
    
    // GetExecution 获取执行详情
    GetExecution(ctx context.Context, execID string) (*SyncExecution, error)
    
    // GetExecutionProgress 获取执行进度
    // - 从 Task Engine 获取实时进度
    GetExecutionProgress(ctx context.Context, execID string) (*ExecutionProgress, error)
    
    // ListExecutions 列出执行历史
    ListExecutions(ctx context.Context, jobID string, limit int) ([]SyncExecution, error)
    
    // CancelExecution 取消执行
    CancelExecution(ctx context.Context, execID string) error
    
    // ==================== 参数规则 ====================
    
    // GenerateParams 根据规则生成参数列表
    // - 用于预览将要执行的参数组合
    GenerateParams(ctx context.Context, jobID string) ([]map[string]any, error)
    
    // ValidateParamRules 验证参数规则
    ValidateParamRules(ctx context.Context, rules []ParamRule) error
}

// SyncJobQuery 同步任务查询条件
type SyncJobQuery struct {
    APIMetaID   *string
    DataStoreID *string
    Mode        *SyncMode
    Status      *JobStatus
    Limit       int
    Offset      int
}

// ExecutionProgress 执行进度
type ExecutionProgress struct {
    ExecID       string
    Status       ExecStatus
    Progress     float64      // 0-100
    CurrentTask  string
    TotalTasks   int
    CompletedTasks int
    RecordCount  int64
    ErrorMessage *string
}
```

### 4.4 工作流管理领域服务 (WorkflowService)

```go
// WorkflowService 工作流管理领域服务接口
type WorkflowService interface {
    // ==================== 工作流定义管理 ====================
    
    // CreateDefinition 创建工作流定义
    // - 解析并验证 YAML 定义
    // - 验证引用的 Job Function 存在
    CreateDefinition(ctx context.Context, def *WorkflowDefinition) error
    
    // GetDefinition 获取工作流定义
    GetDefinition(ctx context.Context, id string) (*WorkflowDefinition, error)
    
    // ListDefinitions 列出工作流定义
    ListDefinitions(ctx context.Context, category *WfCategory) ([]WorkflowDefinition, error)
    
    // UpdateDefinition 更新工作流定义
    // - 创建新版本而不是覆盖
    UpdateDefinition(ctx context.Context, def *WorkflowDefinition) error
    
    // DeleteDefinition 删除工作流定义
    // - 仅允许删除无运行实例的定义
    DeleteDefinition(ctx context.Context, id string) error
    
    // EnableDefinition 启用工作流定义
    EnableDefinition(ctx context.Context, id string) error
    
    // DisableDefinition 禁用工作流定义
    DisableDefinition(ctx context.Context, id string) error
    
    // ==================== 工作流执行 ====================
    
    // ExecuteWorkflow 执行工作流
    // 核心流程：
    //   1. 获取工作流定义
    //   2. 解析 YAML 构建 Workflow 对象
    //   3. 创建 WorkflowInstance 记录
    //   4. 提交到 Task Engine
    //   5. 返回实例 ID
    ExecuteWorkflow(ctx context.Context, defID string, params map[string]any) (instID string, err error)
    
    // ==================== 实例管理 ====================
    
    // GetInstance 获取工作流实例
    GetInstance(ctx context.Context, id string) (*WorkflowInstance, error)
    
    // GetInstanceWithTasks 获取实例及其任务列表
    GetInstanceWithTasks(ctx context.Context, id string) (*WorkflowInstance, error)
    
    // ListInstances 列出工作流实例
    ListInstances(ctx context.Context, query InstanceQuery) ([]WorkflowInstance, error)
    
    // GetInstanceProgress 获取实例进度
    // - 从 Task Engine 获取实时状态
    GetInstanceProgress(ctx context.Context, id string) (*InstanceProgress, error)
    
    // ==================== 实例控制 ====================
    
    // PauseInstance 暂停实例
    // - 调用 Task Engine 暂停
    PauseInstance(ctx context.Context, id string) error
    
    // ResumeInstance 恢复实例
    // - 调用 Task Engine 恢复
    ResumeInstance(ctx context.Context, id string) error
    
    // CancelInstance 取消实例
    // - 调用 Task Engine 取消
    CancelInstance(ctx context.Context, id string) error
    
    // RetryInstance 重试失败的实例
    // - 从失败点继续执行
    RetryInstance(ctx context.Context, id string) error
    
    // ==================== 系统工作流 ====================
    
    // GetSystemWorkflows 获取系统内置工作流
    GetSystemWorkflows(ctx context.Context) ([]WorkflowDefinition, error)
    
    // InitSystemWorkflows 初始化系统工作流
    // - 应用启动时调用
    InitSystemWorkflows(ctx context.Context) error
}

// InstanceQuery 实例查询条件
type InstanceQuery struct {
    WorkflowDefID *string
    Status        *WfInstStatus
    TriggerType   *TriggerType
    StartTimeFrom *time.Time
    StartTimeTo   *time.Time
    Limit         int
    Offset        int
}

// InstanceProgress 实例进度
type InstanceProgress struct {
    InstanceID     string
    Status         WfInstStatus
    Progress       float64
    TotalTasks     int
    CompletedTasks int
    RunningTasks   int
    FailedTasks    int
    CurrentTasks   []string
    ErrorMessage   *string
}
```

---

## 5. 数据访问层设计

### 5.1 通用 Repository 接口

```go
// ==================== 通用 Repository ====================

// Entity 实体基础接口
type Entity interface {
    GetID() string
    SetID(id string)
    TableName() string
}

// Repository 通用仓储接口
type Repository[T Entity] interface {
    // Create 创建实体
    Create(ctx context.Context, entity *T) error
    
    // GetByID 根据 ID 获取实体
    GetByID(ctx context.Context, id string) (*T, error)
    
    // Update 更新实体
    Update(ctx context.Context, entity *T) error
    
    // Delete 删除实体
    Delete(ctx context.Context, id string) error
    
    // Exists 检查实体是否存在
    Exists(ctx context.Context, id string) (bool, error)
    
    // Count 统计数量
    Count(ctx context.Context, conditions map[string]any) (int64, error)
    
    // List 列表查询
    List(ctx context.Context, conditions map[string]any, limit, offset int) ([]T, error)
}
```

### 5.2 DAO 层设计

```go
// ==================== 通用 DAO ====================

// BaseDAO 通用 DAO 实现
type BaseDAO[T any] struct {
    db        *sqlx.DB
    tableName string
}

// NewBaseDAO 创建通用 DAO
func NewBaseDAO[T any](db *sqlx.DB, tableName string) *BaseDAO[T] {
    return &BaseDAO[T]{db: db, tableName: tableName}
}

// Insert 插入记录
func (d *BaseDAO[T]) Insert(ctx context.Context, entity *T) error {
    // 使用反射生成 INSERT SQL
}

// GetByID 根据 ID 查询
func (d *BaseDAO[T]) GetByID(ctx context.Context, id string) (*T, error) {
    query := fmt.Sprintf("SELECT * FROM %s WHERE id = $1", d.tableName)
    var entity T
    err := d.db.GetContext(ctx, &entity, query, id)
    return &entity, err
}

// Update 更新记录
func (d *BaseDAO[T]) Update(ctx context.Context, entity *T) error {
    // 使用反射生成 UPDATE SQL
}

// Delete 删除记录
func (d *BaseDAO[T]) Delete(ctx context.Context, id string) error {
    query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", d.tableName)
    _, err := d.db.ExecContext(ctx, query, id)
    return err
}

// FindBy 条件查询
func (d *BaseDAO[T]) FindBy(ctx context.Context, conditions map[string]any, limit, offset int) ([]T, error) {
    // 构建条件查询 SQL
}

// ==================== 具体 DAO 示例 ====================

// DataSourceDAO 数据源 DAO
type DataSourceDAO struct {
    *BaseDAO[DataSource]
}

func NewDataSourceDAO(db *sqlx.DB) *DataSourceDAO {
    return &DataSourceDAO{
        BaseDAO: NewBaseDAO[DataSource](db, "data_sources"),
    }
}

// FindByStatus 按状态查询（业务特定方法）
func (d *DataSourceDAO) FindByStatus(ctx context.Context, status Status) ([]DataSource, error) {
    query := "SELECT * FROM data_sources WHERE status = $1"
    var results []DataSource
    err := d.db.SelectContext(ctx, &results, query, status)
    return results, err
}

// APIMetadataDAO API 元数据 DAO
type APIMetadataDAO struct {
    *BaseDAO[APIMetadata]
}

func NewAPIMetadataDAO(db *sqlx.DB) *APIMetadataDAO {
    return &APIMetadataDAO{
        BaseDAO: NewBaseDAO[APIMetadata](db, "api_metadata"),
    }
}

// FindByDataSource 按数据源查询
func (d *APIMetadataDAO) FindByDataSource(ctx context.Context, dataSourceID string) ([]APIMetadata, error) {
    query := "SELECT * FROM api_metadata WHERE data_source_id = $1"
    var results []APIMetadata
    err := d.db.SelectContext(ctx, &results, query, dataSourceID)
    return results, err
}

// FindByCategory 按分类查询
func (d *APIMetadataDAO) FindByCategory(ctx context.Context, categoryID string) ([]APIMetadata, error) {
    query := "SELECT * FROM api_metadata WHERE category_id = $1"
    var results []APIMetadata
    err := d.db.SelectContext(ctx, &results, query, categoryID)
    return results, err
}

// Search 关键词搜索
func (d *APIMetadataDAO) Search(ctx context.Context, keyword string) ([]APIMetadata, error) {
    query := `SELECT * FROM api_metadata 
              WHERE name ILIKE $1 OR display_name ILIKE $1 OR description ILIKE $1`
    var results []APIMetadata
    err := d.db.SelectContext(ctx, &results, query, "%"+keyword+"%")
    return results, err
}
```

### 5.3 Repository 实现（聚合根级别）

```go
// ==================== 元数据 Repository ====================

// MetadataRepository 元数据仓储接口
type MetadataRepository interface {
    Repository[APIMetadata]
    
    // ===== 数据源相关 =====
    SaveDataSource(ctx context.Context, ds *DataSource) error
    GetDataSource(ctx context.Context, id string) (*DataSource, error)
    ListDataSources(ctx context.Context) ([]DataSource, error)
    
    // ===== 目录相关 =====
    SaveCategories(ctx context.Context, categories []APICategory) error
    GetCategoriesByDataSource(ctx context.Context, dataSourceID string) ([]APICategory, error)
    
    // ===== API 元数据相关（聚合根操作）=====
    SaveAPIMetadataBatch(ctx context.Context, apis []APIMetadata) error
    GetAPIsByDataSource(ctx context.Context, dataSourceID string) ([]APIMetadata, error)
    GetAPIsByCategory(ctx context.Context, categoryID string) ([]APIMetadata, error)
    SearchAPIs(ctx context.Context, keyword string) ([]APIMetadata, error)
    
    // ===== Token 相关 =====
    SaveToken(ctx context.Context, token *Token) error
    GetToken(ctx context.Context, dataSourceID string) (*Token, error)
}

// MetadataRepositoryImpl 元数据仓储实现
type MetadataRepositoryImpl struct {
    dataSourceDAO   *DataSourceDAO
    categoryDAO     *APICategoryDAO
    apiMetadataDAO  *APIMetadataDAO
    tokenDAO        *TokenDAO
    db              *sqlx.DB
}

// SaveDataSource 保存数据源
func (r *MetadataRepositoryImpl) SaveDataSource(ctx context.Context, ds *DataSource) error {
    if ds.ID == "" {
        ds.ID = uuid.New().String()
        return r.dataSourceDAO.Insert(ctx, ds)
    }
    return r.dataSourceDAO.Update(ctx, ds)
}

// SaveAPIMetadataBatch 批量保存 API 元数据（事务操作）
func (r *MetadataRepositoryImpl) SaveAPIMetadataBatch(ctx context.Context, apis []APIMetadata) error {
    tx, err := r.db.BeginTxx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()
    
    for _, api := range apis {
        if api.ID == "" {
            api.ID = uuid.New().String()
        }
        // 使用 UPSERT
        _, err := tx.NamedExecContext(ctx, `
            INSERT INTO api_metadata (id, data_source_id, category_id, name, display_name, ...)
            VALUES (:id, :data_source_id, :category_id, :name, :display_name, ...)
            ON CONFLICT (data_source_id, name) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                description = EXCLUDED.description,
                ...
        `, api)
        if err != nil {
            return err
        }
    }
    
    return tx.Commit()
}

// ==================== 量化数据存储 Repository ====================

// QuantDataStoreRepository 量化数据存储仓储接口
type QuantDataStoreRepository interface {
    Repository[QuantDataStore]
    
    // ===== 聚合根操作 =====
    // GetWithSchemas 获取数据存储及其所有表结构
    GetWithSchemas(ctx context.Context, id string) (*QuantDataStore, error)
    
    // ===== 表结构操作 =====
    SaveSchema(ctx context.Context, schema *TableSchema) error
    GetSchema(ctx context.Context, schemaID string) (*TableSchema, error)
    GetSchemasByDataStore(ctx context.Context, dataStoreID string) ([]TableSchema, error)
    GetSchemaByAPIAndStore(ctx context.Context, apiMetaID, dataStoreID string) (*TableSchema, error)
    UpdateSchemaStatus(ctx context.Context, schemaID string, status SchemaStatus, errMsg *string) error
}

// ==================== 同步任务 Repository ====================

// SyncRepository 同步任务仓储接口
type SyncRepository interface {
    Repository[SyncJob]
    
    // ===== 聚合根操作 =====
    // GetWithExecutions 获取任务及其执行历史
    GetWithExecutions(ctx context.Context, id string, execLimit int) (*SyncJob, error)
    
    // ===== 任务查询 =====
    ListByStatus(ctx context.Context, status JobStatus) ([]SyncJob, error)
    ListByDataStore(ctx context.Context, dataStoreID string) ([]SyncJob, error)
    GetEnabledCronJobs(ctx context.Context) ([]SyncJob, error)
    
    // ===== 执行记录操作 =====
    SaveExecution(ctx context.Context, exec *SyncExecution) error
    GetExecution(ctx context.Context, execID string) (*SyncExecution, error)
    ListExecutions(ctx context.Context, jobID string, limit int) ([]SyncExecution, error)
    UpdateExecutionStatus(ctx context.Context, execID string, status ExecStatus, recordCount int64, errMsg *string) error
}

// ==================== 工作流 Repository ====================

// WorkflowRepository 工作流仓储接口
type WorkflowRepository interface {
    Repository[WorkflowDefinition]
    
    // ===== 定义操作 =====
    GetByName(ctx context.Context, name string) (*WorkflowDefinition, error)
    ListByCategory(ctx context.Context, category WfCategory) ([]WorkflowDefinition, error)
    ListSystemWorkflows(ctx context.Context) ([]WorkflowDefinition, error)
    
    // ===== 实例操作 =====
    SaveInstance(ctx context.Context, inst *WorkflowInstance) error
    GetInstance(ctx context.Context, instID string) (*WorkflowInstance, error)
    GetInstanceWithTasks(ctx context.Context, instID string) (*WorkflowInstance, error)
    ListInstances(ctx context.Context, query InstanceQuery) ([]WorkflowInstance, error)
    UpdateInstanceStatus(ctx context.Context, instID string, status WfInstStatus, progress float64, errMsg *string) error
    
    // ===== 任务实例操作 =====
    SaveTaskInstances(ctx context.Context, tasks []TaskInstance) error
    UpdateTaskStatus(ctx context.Context, taskID string, status TaskStatus, errMsg *string) error
}
```

---

## 6. 数据类型映射

### 6.1 数据源类型映射表

从数据源 API 返回的字段类型，映射到目标数据库类型。

#### Tushare 类型映射

| Tushare 类型 | Go 类型 | DuckDB 类型 | ClickHouse 类型 | PostgreSQL 类型 |
|-------------|---------|-------------|-----------------|-----------------|
| `str` | `string` | `VARCHAR` | `String` | `VARCHAR` |
| `float` | `float64` | `DOUBLE` | `Float64` | `DOUBLE PRECISION` |
| `int` | `int64` | `BIGINT` | `Int64` | `BIGINT` |
| `date` | `time.Time` | `DATE` | `Date` | `DATE` |
| `datetime` | `time.Time` | `TIMESTAMP` | `DateTime` | `TIMESTAMP` |

#### 字段特殊处理规则

| 字段名模式 | 类型推断 | 说明 |
|-----------|---------|------|
| `*_code`, `ts_code` | `VARCHAR(16)` | 股票代码，固定长度 |
| `*_date`, `trade_date` | `DATE` | 日期类型 |
| `*_time`, `*_datetime` | `TIMESTAMP` | 时间戳类型 |
| `vol`, `amount`, `*_vol` | `DECIMAL(20,2)` | 大数值类型 |
| `pct_*`, `*_pct`, `*_rate` | `DECIMAL(10,4)` | 百分比/比率 |
| `*_price`, `open`, `high`, `low`, `close` | `DECIMAL(10,2)` | 价格类型 |

### 6.2 类型映射实现

```go
// ==================== 类型映射器 ====================

// TypeMapper 类型映射器接口
type TypeMapper interface {
    // MapFieldType 映射单个字段类型
    MapFieldType(field FieldMeta, targetDB DataStoreType) string
    
    // MapAllFields 映射所有字段类型
    MapAllFields(fields []FieldMeta, targetDB DataStoreType) []ColumnDef
}

// TushareTypeMapper Tushare 类型映射器
type TushareTypeMapper struct{}

// 基础类型映射表
var tushareTypeMap = map[string]map[DataStoreType]string{
    "str": {
        DataStoreTypeDuckDB:     "VARCHAR",
        DataStoreTypeClickHouse: "String",
        DataStoreTypePostgreSQL: "VARCHAR",
    },
    "float": {
        DataStoreTypeDuckDB:     "DOUBLE",
        DataStoreTypeClickHouse: "Float64",
        DataStoreTypePostgreSQL: "DOUBLE PRECISION",
    },
    "int": {
        DataStoreTypeDuckDB:     "BIGINT",
        DataStoreTypeClickHouse: "Int64",
        DataStoreTypePostgreSQL: "BIGINT",
    },
    "date": {
        DataStoreTypeDuckDB:     "DATE",
        DataStoreTypeClickHouse: "Date",
        DataStoreTypePostgreSQL: "DATE",
    },
    "datetime": {
        DataStoreTypeDuckDB:     "TIMESTAMP",
        DataStoreTypeClickHouse: "DateTime",
        DataStoreTypePostgreSQL: "TIMESTAMP",
    },
}

// 特殊字段规则
var fieldPatternRules = []struct {
    Pattern   string                      // 正则模式
    TypeMap   map[DataStoreType]string    // 类型映射
}{
    {
        Pattern: `^(ts_code|.*_code)$`,
        TypeMap: map[DataStoreType]string{
            DataStoreTypeDuckDB:     "VARCHAR(16)",
            DataStoreTypeClickHouse: "FixedString(16)",
            DataStoreTypePostgreSQL: "VARCHAR(16)",
        },
    },
    {
        Pattern: `^(.*_date|trade_date|ann_date|end_date)$`,
        TypeMap: map[DataStoreType]string{
            DataStoreTypeDuckDB:     "DATE",
            DataStoreTypeClickHouse: "Date",
            DataStoreTypePostgreSQL: "DATE",
        },
    },
    {
        Pattern: `^(vol|amount|.*_vol|.*_amount)$`,
        TypeMap: map[DataStoreType]string{
            DataStoreTypeDuckDB:     "DECIMAL(20,2)",
            DataStoreTypeClickHouse: "Decimal(20,2)",
            DataStoreTypePostgreSQL: "DECIMAL(20,2)",
        },
    },
    {
        Pattern: `^(pct_.*|.*_pct|.*_rate|.*_ratio)$`,
        TypeMap: map[DataStoreType]string{
            DataStoreTypeDuckDB:     "DECIMAL(10,4)",
            DataStoreTypeClickHouse: "Decimal(10,4)",
            DataStoreTypePostgreSQL: "DECIMAL(10,4)",
        },
    },
    {
        Pattern: `^(open|high|low|close|pre_close|.*_price)$`,
        TypeMap: map[DataStoreType]string{
            DataStoreTypeDuckDB:     "DECIMAL(10,2)",
            DataStoreTypeClickHouse: "Decimal(10,2)",
            DataStoreTypePostgreSQL: "DECIMAL(10,2)",
        },
    },
}

// MapFieldType 映射单个字段类型
func (m *TushareTypeMapper) MapFieldType(field FieldMeta, targetDB DataStoreType) string {
    // 1. 先检查特殊字段规则
    for _, rule := range fieldPatternRules {
        matched, _ := regexp.MatchString(rule.Pattern, field.Name)
        if matched {
            if t, ok := rule.TypeMap[targetDB]; ok {
                return t
            }
        }
    }
    
    // 2. 使用基础类型映射
    if typeMap, ok := tushareTypeMap[field.Type]; ok {
        if t, ok := typeMap[targetDB]; ok {
            return t
        }
    }
    
    // 3. 默认返回 VARCHAR
    return "VARCHAR"
}

// MapAllFields 映射所有字段
func (m *TushareTypeMapper) MapAllFields(fields []FieldMeta, targetDB DataStoreType) []ColumnDef {
    columns := make([]ColumnDef, len(fields))
    for i, field := range fields {
        columns[i] = ColumnDef{
            Name:       field.Name,
            SourceType: field.Type,
            TargetType: m.MapFieldType(field, targetDB),
            Nullable:   !field.IsPrimary,
            Comment:    field.Description,
        }
    }
    return columns
}
```

### 6.3 Go 与数据库类型映射

| Go 类型 | DuckDB 类型 | ClickHouse 类型 | PostgreSQL 类型 | SQLite 类型 |
|---------|-------------|-----------------|-----------------|-------------|
| `string` | `VARCHAR` | `String` | `VARCHAR` / `TEXT` | `TEXT` |
| `int` | `INTEGER` | `Int32` | `INTEGER` | `INTEGER` |
| `int64` | `BIGINT` | `Int64` | `BIGINT` | `INTEGER` |
| `float64` | `DOUBLE` | `Float64` | `DOUBLE PRECISION` | `REAL` |
| `bool` | `BOOLEAN` | `Bool` | `BOOLEAN` | `INTEGER` |
| `time.Time` | `TIMESTAMP` | `DateTime` | `TIMESTAMP` | `TEXT` |
| `[]byte` | `BLOB` | `String` | `BYTEA` | `BLOB` |
| `map[string]any` | - | - | `JSONB` | `TEXT` |

---

## 7. API 设计

### 7.1 RESTful API 概览

#### 元数据管理

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/datasources/:id/refresh` | 刷新数据源元数据 |
| GET | `/api/v1/datasources` | 列出所有数据源 |
| GET | `/api/v1/datasources/:id/categories` | 获取 API 目录 |
| GET | `/api/v1/datasources/:id/apis` | 列出数据源的所有 API |
| GET | `/api/v1/apis/:id` | 获取 API 详情 |
| POST | `/api/v1/datasources/:id/token` | 设置数据源 Token |

#### 数据存储管理

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/datastores` | 创建量化数据存储配置 |
| GET | `/api/v1/datastores` | 列出所有数据存储 |
| GET | `/api/v1/datastores/:id` | 获取数据存储详情 |
| POST | `/api/v1/datastores/:id/test` | 测试连接 |
| POST | `/api/v1/datastores/:id/schemas/generate` | 生成表结构 |
| POST | `/api/v1/datastores/:id/schemas/:schemaId/create` | 执行建表 |
| GET | `/api/v1/datastores/:id/schemas` | 列出已创建的表 |

#### 同步管理

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/sync-jobs` | 创建同步任务 |
| GET | `/api/v1/sync-jobs` | 列出同步任务 |
| GET | `/api/v1/sync-jobs/:id` | 获取任务详情 |
| PUT | `/api/v1/sync-jobs/:id` | 更新任务配置 |
| POST | `/api/v1/sync-jobs/:id/trigger` | 手动触发同步 |
| POST | `/api/v1/sync-jobs/:id/enable` | 启用任务 |
| POST | `/api/v1/sync-jobs/:id/disable` | 禁用任务 |
| GET | `/api/v1/sync-jobs/:id/executions` | 获取执行历史 |

#### 工作流管理

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/workflows` | 创建工作流定义 |
| GET | `/api/v1/workflows` | 列出工作流定义 |
| GET | `/api/v1/workflows/:id` | 获取工作流定义 |
| PUT | `/api/v1/workflows/:id` | 更新工作流定义 |
| DELETE | `/api/v1/workflows/:id` | 删除工作流定义 |
| POST | `/api/v1/workflows/:id/execute` | 执行工作流 |
| POST | `/api/v1/workflows/:id/enable` | 启用工作流 |
| POST | `/api/v1/workflows/:id/disable` | 禁用工作流 |
| GET | `/api/v1/instances` | 列出工作流实例 |
| GET | `/api/v1/instances/:id` | 获取实例详情 |
| GET | `/api/v1/instances/:id/tasks` | 获取实例任务列表 |
| GET | `/api/v1/instances/:id/progress` | 获取实例进度 |
| POST | `/api/v1/instances/:id/pause` | 暂停实例 |
| POST | `/api/v1/instances/:id/resume` | 恢复实例 |
| POST | `/api/v1/instances/:id/cancel` | 取消实例 |
| POST | `/api/v1/instances/:id/retry` | 重试实例 |

---

## 8. 工作流设计

### 8.1 元数据刷新工作流

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  爬取API目录  │────▶│  解析目录结构 │────▶│  保存目录数据 │
└──────────────┘     └──────────────┘     └──────────────┘
                                                  │
                                                  ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ 保存API元数据 │◀────│  解析API详情  │◀────│ 爬取API详情  │ (模板任务)
└──────────────┘     └──────────────┘     └──────────────┘
```

### 8.2 批量数据同步工作流

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  加载同步配置 │────▶│  生成参数列表 │────▶│  拉取数据    │ (模板任务)
└──────────────┘     └──────────────┘     └──────────────┘
                                                  │
                                                  ▼
                     ┌──────────────┐     ┌──────────────┐
                     │  更新同步状态 │◀────│  写入数据库   │
                     └──────────────┘     └──────────────┘
```

### 8.3 系统内置工作流

| 工作流名称 | 分类 | 说明 |
|-----------|------|------|
| `metadata_refresh` | metadata | 元数据刷新 |
| `batch_sync` | sync | 批量数据同步 |
| `realtime_sync` | sync | 实时数据同步 |
| `schema_generate` | datastore | 批量生成表结构 |
| `table_create` | datastore | 批量建表 |

---

## 9. 数据库设计

### 9.1 系统数据库表（SQLite/PostgreSQL）

```sql
-- 数据源表
CREATE TABLE data_sources (
    id          VARCHAR(64) PRIMARY KEY,
    name        VARCHAR(128) NOT NULL,
    description TEXT,
    base_url    VARCHAR(512),
    doc_url     VARCHAR(512),
    status      VARCHAR(32) DEFAULT 'active',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- API 目录表
CREATE TABLE api_categories (
    id             VARCHAR(64) PRIMARY KEY,
    data_source_id VARCHAR(64) NOT NULL REFERENCES data_sources(id),
    name           VARCHAR(128) NOT NULL,
    description    TEXT,
    parent_id      VARCHAR(64) REFERENCES api_categories(id),
    sort_order     INT DEFAULT 0,
    doc_path       VARCHAR(512),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- API 元数据表
CREATE TABLE api_metadata (
    id              VARCHAR(64) PRIMARY KEY,
    data_source_id  VARCHAR(64) NOT NULL REFERENCES data_sources(id),
    category_id     VARCHAR(64) REFERENCES api_categories(id),
    name            VARCHAR(128) NOT NULL,
    display_name    VARCHAR(256),
    description     TEXT,
    endpoint        VARCHAR(512),
    request_params  JSONB,
    response_fields JSONB,
    rate_limit      JSONB,
    permission      VARCHAR(64),
    status          VARCHAR(32) DEFAULT 'active',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(data_source_id, name)
);

-- Token 表
CREATE TABLE tokens (
    id             VARCHAR(64) PRIMARY KEY,
    data_source_id VARCHAR(64) NOT NULL REFERENCES data_sources(id),
    token_value    TEXT NOT NULL,
    expires_at     TIMESTAMP,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 量化数据存储配置表
CREATE TABLE quant_data_stores (
    id           VARCHAR(64) PRIMARY KEY,
    name         VARCHAR(128) NOT NULL,
    type         VARCHAR(32) NOT NULL,
    dsn          TEXT,
    storage_path VARCHAR(512),
    status       VARCHAR(32) DEFAULT 'active',
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 表结构定义表
CREATE TABLE table_schemas (
    id             VARCHAR(64) PRIMARY KEY,
    data_store_id  VARCHAR(64) NOT NULL REFERENCES quant_data_stores(id),
    api_meta_id    VARCHAR(64) NOT NULL REFERENCES api_metadata(id),
    table_name     VARCHAR(128) NOT NULL,
    columns        JSONB NOT NULL,
    primary_keys   JSONB,
    indexes        JSONB,
    status         VARCHAR(32) DEFAULT 'pending',
    error_message  TEXT,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(data_store_id, table_name)
);

-- 数据类型映射规则表
CREATE TABLE data_type_mapping_rules (
    id               VARCHAR(64) PRIMARY KEY,
    data_source_type VARCHAR(32) NOT NULL,          -- tushare, akshare 等
    source_type      VARCHAR(64) NOT NULL,          -- 源数据类型
    target_db_type   VARCHAR(32) NOT NULL,          -- 目标数据库类型
    target_type      VARCHAR(64) NOT NULL,          -- 目标数据类型
    field_pattern    VARCHAR(256),                  -- 字段名正则模式（可选）
    priority         INT DEFAULT 0,                 -- 优先级，越高越优先
    is_default       BOOLEAN DEFAULT FALSE,         -- 是否系统默认规则
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(data_source_type, source_type, target_db_type, field_pattern)
);
CREATE INDEX idx_mapping_rules_lookup ON data_type_mapping_rules(data_source_type, target_db_type, priority DESC);

-- 同步任务表
CREATE TABLE sync_jobs (
    id              VARCHAR(64) PRIMARY KEY,
    name            VARCHAR(128) NOT NULL,
    description     TEXT,
    api_meta_id     VARCHAR(64) NOT NULL REFERENCES api_metadata(id),
    data_store_id   VARCHAR(64) NOT NULL REFERENCES quant_data_stores(id),
    workflow_def_id VARCHAR(64) REFERENCES workflow_definitions(id),  -- 关联工作流定义
    mode            VARCHAR(32) NOT NULL,
    cron_expression VARCHAR(128),                   -- Cron 表达式
    params          JSONB,
    param_rules     JSONB,
    status          VARCHAR(32) DEFAULT 'disabled',
    last_run_at     TIMESTAMP,
    next_run_at     TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 同步执行记录表
CREATE TABLE sync_executions (
    id               VARCHAR(64) PRIMARY KEY,
    sync_job_id      VARCHAR(64) NOT NULL REFERENCES sync_jobs(id),
    workflow_inst_id VARCHAR(64),
    status           VARCHAR(32) NOT NULL,
    started_at       TIMESTAMP NOT NULL,
    finished_at      TIMESTAMP,
    record_count     BIGINT DEFAULT 0,
    error_message    TEXT
);
CREATE INDEX idx_sync_executions_job_id ON sync_executions(sync_job_id);
CREATE INDEX idx_sync_executions_status ON sync_executions(status);

-- 工作流定义表
CREATE TABLE workflow_definitions (
    id              VARCHAR(64) PRIMARY KEY,
    name            VARCHAR(128) NOT NULL UNIQUE,
    description     TEXT,
    category        VARCHAR(32) NOT NULL,
    definition_yaml TEXT NOT NULL,
    version         INT DEFAULT 1,
    status          VARCHAR(32) DEFAULT 'enabled',
    is_system       BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 工作流实例表
CREATE TABLE workflow_instances (
    id                  VARCHAR(64) PRIMARY KEY,
    workflow_def_id     VARCHAR(64) NOT NULL REFERENCES workflow_definitions(id),
    engine_instance_id  VARCHAR(64),
    trigger_type        VARCHAR(32) NOT NULL,
    trigger_params      JSONB,
    status              VARCHAR(32) NOT NULL,
    progress            DECIMAL(5,2) DEFAULT 0,
    started_at          TIMESTAMP NOT NULL,
    finished_at         TIMESTAMP,
    error_message       TEXT
);
CREATE INDEX idx_workflow_instances_def_id ON workflow_instances(workflow_def_id);
CREATE INDEX idx_workflow_instances_status ON workflow_instances(status);

-- 任务实例表
CREATE TABLE task_instances (
    id                VARCHAR(64) PRIMARY KEY,
    workflow_inst_id  VARCHAR(64) NOT NULL REFERENCES workflow_instances(id),
    task_name         VARCHAR(128) NOT NULL,
    status            VARCHAR(32) NOT NULL,
    started_at        TIMESTAMP,
    finished_at       TIMESTAMP,
    retry_count       INT DEFAULT 0,
    error_message     TEXT
);
CREATE INDEX idx_task_instances_wf_inst_id ON task_instances(workflow_inst_id);
```

---

## 10. 开发路线图

### Phase 1: MVP 核心功能（2-3 周）

| 优先级 | 功能 | 说明 |
|--------|------|------|
| P0 | 项目骨架搭建 | 目录结构、依赖注入、配置管理 |
| P0 | 通用 DAO/Repository | 实现通用 CRUD |
| P0 | Tushare 数据源适配器 | 实现 API 调用客户端 |
| P0 | 元数据爬取解析 | 爬取 Tushare 文档，解析 API 信息 |
| P0 | DuckDB 适配器 | 实现表操作和数据写入 |
| P0 | 批量同步工作流 | 实现核心数据同步流程 |

### Phase 2: 管理功能（2 周）

| 优先级 | 功能 | 说明 |
|--------|------|------|
| P1 | HTTP API 完整实现 | 所有管理接口 |
| P1 | 工作流管理 | 定义、实例、任务管理 |
| P1 | 同步任务调度 | Cron 定时触发 |
| P1 | 执行状态监控 | 实时查询工作流状态 |
| P1 | Token 加密管理 | 安全存储数据源凭证 |

### Phase 3: 扩展功能（按需）

| 优先级 | 功能 | 说明 |
|--------|------|------|
| P2 | 实时同步模式 | 支持实时数据推送 |
| P2 | AKShare 数据源 | 扩展第二个数据源 |
| P2 | PostgreSQL 系统库 | 生产环境数据库 |
| P2 | ClickHouse 量化库 | 大数据量场景 |
| P3 | 数据可视化 API | 查询接口供前端使用 |

---

## 附录

### A. 依赖接口定义

```go
// ==================== 元数据领域接口 ====================

// Crawler 数据源爬虫接口（基础设施层实现）
type Crawler interface {
    // FetchCatalogPage 获取目录页面内容
    FetchCatalogPage(datasourceID string) (content string, docType DocumentType, error)
    // FetchAPIDetailPage 获取 API 详情页面内容
    FetchAPIDetailPage(apiURL string) (content string, docType DocumentType, error)
}

// DocumentParser 文档解析器接口（基础设施层实现）
// 说明：已在 3.1 元数据领域中定义，此处为快速参考
type DocumentParser interface {
    ParseCatalog(content string) ([]APICategory, []string, error)
    ParseAPIDetail(content string) (*APIMetadata, error)
    SupportedType() DocumentType
}

// DocumentParserFactory 文档解析器工厂接口
type DocumentParserFactory interface {
    GetParser(docType DocumentType) (DocumentParser, error)
    RegisterParser(parser DocumentParser)
}

// APIClient 数据源 API 客户端接口（基础设施层实现）
type APIClient interface {
    SetToken(token string)
    Query(apiName string, params map[string]interface{}) ([]map[string]interface{}, error)
}

// ==================== 量化数据存储领域接口 ====================

// QuantDB 量化数据库接口（基础设施层实现）
type QuantDB interface {
    CreateTable(schema *TableSchema) error
    DropTable(tableName string) error
    BulkInsert(tableName string, data []map[string]interface{}) (int64, error)
    Query(sql string, args ...interface{}) ([]map[string]interface{}, error)
}

// TypeMapper 类型映射器接口（领域层定义）
// 职责：根据 DataTypeMappingRule 进行字段类型映射
type TypeMapper interface {
    // MapFieldType 映射单个字段类型
    // 参数：字段元数据、数据源类型、目标数据库类型
    // 返回：目标数据库的列类型
    MapFieldType(field FieldMeta, dataSourceType string, targetDB DataStoreType) string
    
    // MapAllFields 映射所有字段，生成列定义列表
    MapAllFields(fields []FieldMeta, dataSourceType string, targetDB DataStoreType) []ColumnDef
}

// TypeMappingRuleRepository 类型映射规则仓储接口
type TypeMappingRuleRepository interface {
    // GetBySourceAndTarget 根据数据源类型和目标数据库类型获取规则列表
    // 返回按优先级降序排列的规则
    GetBySourceAndTarget(ctx context.Context, dataSourceType string, targetDB DataStoreType) ([]DataTypeMappingRule, error)
    
    // GetMatchingRule 获取匹配的规则
    // 参数：数据源类型、源字段类型、目标数据库类型、字段名
    GetMatchingRule(ctx context.Context, dataSourceType, sourceType string, targetDB DataStoreType, fieldName string) (*DataTypeMappingRule, error)
    
    // SaveBatch 批量保存规则
    SaveBatch(ctx context.Context, rules []DataTypeMappingRule) error
    
    // InitDefaultRules 初始化默认规则
    InitDefaultRules(ctx context.Context) error
}
```

### B. 配置文件示例

```yaml
# configs/config.yaml
server:
  host: "0.0.0.0"
  port: 8080

database:
  driver: "sqlite"  # sqlite | postgres
  dsn: "./data/qdhub.db"

task_engine:
  worker_count: 10
  task_timeout: 60

datasources:
  tushare:
    enabled: true
    base_url: "http://api.tushare.pro"
    doc_url: "https://tushare.pro/document/2"
```
