# 内建Workflow集成分析报告

## 概述

本报告分析 `metadata_impl.go`、`datastore_impl.go`、`sync_impl.go` 三个应用服务实现文件中，哪些方法需要使用内建workflow来实现功能，并需要改为异步方法。

## 内建Workflow说明

系统提供4个内建workflow：

1. **MetadataCrawlWorkflow** (`metadata_crawl`)
   - 功能：从数据源爬取API文档并保存元数据
   - 包含：获取目录、解析目录、保存分类、为每个API生成爬取子任务、保存元数据
   - 支持SAGA事务，失败时自动回滚

2. **CreateTablesWorkflow** (`create_tables`)
   - 功能：为多个API批量创建表
   - 支持模板任务，动态生成建表子任务
   - 支持SAGA事务，建表失败时自动删除已创建的表

3. **BatchDataSyncWorkflow** (`batch_data_sync`)
   - 功能：批量同步历史数据，支持指定日期范围
   - 包含：获取交易日历、获取股票基础信息、为每个API生成同步子任务
   - 支持SAGA事务，同步失败时按sync_batch_id回滚数据

4. **RealtimeDataSyncWorkflow** (`realtime_data_sync`)
   - 功能：增量实时同步最新数据，支持断点续传和定时调度
   - 包含：获取同步检查点、获取最新交易日、增量同步、更新检查点
   - 支持SAGA事务，同步失败时按sync_batch_id回滚数据

## 方法分析

### 1. metadata_impl.go

#### ✅ 需要使用Workflow的方法

**ParseAndImportMetadata** (第145行)
- **当前实现**：同步方法，仅解析传入的文档内容并保存分类，功能不完整
- **应该使用**：`MetadataCrawlWorkflow` (`metadata_crawl`)
- **原因**：
  - 当前实现只做了部分工作（解析文档内容），没有完整实现元数据爬取流程
  - MetadataCrawlWorkflow提供了完整的爬取流程：获取目录、解析、保存分类、爬取每个API详情、保存元数据
  - 这是一个耗时操作，应该异步执行
- **改造方案**：
  - 改为异步方法，返回workflow instance ID
  - 通过 `WorkflowApplicationService.ExecuteBuiltInWorkflowByName("metadata_crawl", ...)` 执行
  - 参数映射：
    - `data_source_id`: `req.DataSourceID`
    - `data_source_name`: 从DataSource获取
    - `max_api_crawl`: 可选参数，从请求中获取或使用默认值0（不限制）
- **接口变更**：
  ```go
  // 原接口（同步）
  ParseAndImportMetadata(ctx context.Context, req ParseMetadataRequest) (*ParseMetadataResult, error)
  
  // 新接口（异步）
  ParseAndImportMetadata(ctx context.Context, req ParseMetadataRequest) (shared.ID, error) // 返回workflow instance ID
  ```

#### ❌ 不需要使用Workflow的方法

以下方法都是简单的CRUD操作或查询操作，不需要使用workflow：
- `CreateDataSource` - 创建数据源，简单操作
- `GetDataSource` - 查询操作
- `UpdateDataSource` - 更新操作
- `DeleteDataSource` - 删除操作
- `ListDataSources` - 查询操作
- `CreateAPIMetadata` - 创建操作
- `GetAPIMetadata` - 查询操作
- `UpdateAPIMetadata` - 更新操作
- `DeleteAPIMetadata` - 删除操作
- `ListAPIMetadataByDataSource` - 查询操作
- `SaveToken` - 保存操作
- `GetToken` - 查询操作
- `DeleteToken` - 删除操作

### 2. datastore_impl.go

#### ✅ 需要使用Workflow的方法

**CreateTable** (第274行)
- **当前实现**：同步方法，直接执行DDL创建单个表
- **应该使用**：`CreateTablesWorkflow` (`create_tables`)
- **原因**：
  - CreateTablesWorkflow支持为多个API批量建表，使用模板任务动态生成子任务
  - 支持SAGA事务，建表失败时自动回滚
  - 虽然当前方法只创建单个表，但使用workflow可以提供更好的错误处理和事务支持
  - 未来可能需要支持批量建表场景
- **改造方案**：
  - 改为异步方法，返回workflow instance ID
  - 通过 `WorkflowApplicationService.ExecuteBuiltInWorkflowByName("create_tables", ...)` 执行
  - 参数映射：
    - `data_source_id`: 从schema关联的APIMetadata获取DataSourceID
    - `data_source_name`: 从DataSource获取
    - `target_db_path`: 从DataStore获取StoragePath或DSN
    - `max_tables`: 1（单个表）或从请求中获取
- **接口变更**：
  ```go
  // 原接口（同步）
  CreateTable(ctx context.Context, schemaID shared.ID) error
  
  // 新接口（异步）
  CreateTable(ctx context.Context, schemaID shared.ID) (shared.ID, error) // 返回workflow instance ID
  ```

**GenerateTableSchema** (第193行) - **可选**
- **当前实现**：生成schema，如果AutoCreate=true则调用CreateTable
- **是否需要改造**：取决于业务需求
- **方案1（推荐）**：保持同步，但AutoCreate时改为异步调用CreateTable
  - GenerateTableSchema保持同步（生成schema是快速操作）
  - 如果AutoCreate=true，调用异步的CreateTable，返回workflow instance ID
- **方案2**：整个方法改为异步
  - 如果AutoCreate=true，使用CreateTablesWorkflow同时生成schema和建表
  - 但这样会改变方法语义，不推荐

#### ❌ 不需要使用Workflow的方法

以下方法都是简单的CRUD操作、查询操作或快速操作，不需要使用workflow：
- `CreateDataStore` - 创建操作
- `GetDataStore` - 查询操作
- `UpdateDataStore` - 更新操作
- `DeleteDataStore` - 删除操作
- `ListDataStores` - 查询操作
- `TestConnection` - 快速测试操作
- `GenerateTableSchema` - 生成schema（快速操作，除非需要异步建表）
- `DropTable` - 删除操作（快速操作）
- `GetTableSchema` - 查询操作
- `GetTableSchemaByAPI` - 查询操作
- `ListTableSchemas` - 查询操作
- `UpdateTableSchema` - 更新操作
- `SyncSchemaStatus` - 同步状态（快速操作）
- `CreateMappingRule` - 创建操作
- `GetMappingRules` - 查询操作

### 3. sync_impl.go

#### ✅ 需要使用Workflow的方法

**ExecuteSyncJob** (第186行)
- **当前实现**：已经使用workflow，但通过SyncJob的WorkflowDefID执行自定义workflow
- **应该使用**：根据SyncMode选择对应的内建workflow
  - `SyncModeBatch` → `BatchDataSyncWorkflow` (`batch_data_sync`)
  - `SyncModeRealtime` → `RealtimeDataSyncWorkflow` (`realtime_data_sync`)
- **原因**：
  - 当前实现依赖用户手动创建workflow definition，使用不便
  - 内建workflow提供了完整的同步流程，包括错误处理、事务支持等
  - 根据SyncMode自动选择对应的workflow，简化使用
- **改造方案**：
  - 保持异步（已经是异步）
  - 根据job.Mode选择对应的内建workflow：
    ```go
    var workflowName string
    switch job.Mode {
    case sync.SyncModeBatch:
        workflowName = "batch_data_sync"
    case sync.SyncModeRealtime:
        workflowName = "realtime_data_sync"
    default:
        return "", fmt.Errorf("unsupported sync mode: %s", job.Mode)
    }
    ```
  - 参数映射：
    - BatchDataSyncWorkflow参数：
      - `data_source_name`: 从APIMetadata关联的DataSource获取
      - `token`: 从DataSource的Token获取
      - `target_db_path`: 从DataStore获取StoragePath或DSN
      - `start_date`, `end_date`: 从job.Params获取
      - `start_time`, `end_time`: 从job.Params获取（可选）
      - `api_names`: 从job.Params获取或从APIMetadata获取单个API名称
      - `max_stocks`: 从job.Params获取（可选）
    - RealtimeDataSyncWorkflow参数：
      - `data_source_name`: 从APIMetadata关联的DataSource获取
      - `token`: 从DataSource的Token获取
      - `target_db_path`: 从DataStore获取StoragePath或DSN
      - `checkpoint_table`: 从job.Params获取或使用默认值
      - `api_names`: 从job.Params获取或从APIMetadata获取单个API名称
      - `max_stocks`: 从job.Params获取（可选）
      - `cron_expr`: 从job.CronExpression获取（可选）
  - 不再使用job.WorkflowDefID，改为使用内建workflow
- **接口变更**：
  ```go
  // 接口保持不变（已经是异步）
  ExecuteSyncJob(ctx context.Context, jobID shared.ID) (shared.ID, error)
  ```

#### ❌ 不需要使用Workflow的方法

以下方法都是管理操作、查询操作或回调处理，不需要使用workflow：
- `CreateSyncJob` - 创建操作
- `GetSyncJob` - 查询操作
- `UpdateSyncJob` - 更新操作
- `DeleteSyncJob` - 删除操作
- `ListSyncJobs` - 查询操作
- `GetSyncExecution` - 查询操作
- `ListSyncExecutions` - 查询操作
- `CancelExecution` - 取消操作（调用Task Engine API）
- `EnableJob` - 启用操作
- `DisableJob` - 禁用操作
- `UpdateSchedule` - 更新调度
- `HandleExecutionCallback` - 回调处理

## 总结

### 需要改造的方法

| 文件 | 方法 | 使用Workflow | 改造类型 |
|------|------|-------------|---------|
| `metadata_impl.go` | `ParseAndImportMetadata` | `metadata_crawl` | 同步→异步 |
| `datastore_impl.go` | `CreateTable` | `create_tables` | 同步→异步 |
| `sync_impl.go` | `ExecuteSyncJob` | `batch_data_sync` / `realtime_data_sync` | 已异步，改用内建workflow |

### 改造要点

1. **异步化**：
   - `ParseAndImportMetadata` 和 `CreateTable` 需要从同步改为异步
   - 返回workflow instance ID而不是直接返回结果
   - 客户端需要通过workflow instance API查询执行状态和结果

2. **参数映射**：
   - 需要将应用服务的请求参数映射到workflow的参数格式
   - 需要从数据库获取关联数据（如DataSource、DataStore等）

3. **错误处理**：
   - 参数验证失败：返回400错误
   - Workflow执行失败：返回workflow instance ID，客户端通过instance API查询错误详情
   - 参数替换失败（存在未替换的占位符）：返回400错误，包含未替换的占位符列表

4. **向后兼容**：
   - 考虑是否需要保留同步版本的API（如 `ParseAndImportMetadataSync`）
   - 或者通过配置控制是否使用workflow

5. **状态查询**：
   - 提供workflow instance查询API，让客户端可以查询执行状态
   - 提供回调机制，workflow完成后通知应用服务

## 实施建议

### 阶段1：基础改造
1. 改造 `ParseAndImportMetadata` 使用 `metadata_crawl` workflow
2. 改造 `CreateTable` 使用 `create_tables` workflow
3. 改造 `ExecuteSyncJob` 根据SyncMode使用对应的内建workflow

### 阶段2：完善功能
1. 添加workflow instance查询API
2. 添加回调处理机制
3. 完善错误处理和参数验证

### 阶段3：优化体验
1. 提供同步版本的API（内部使用workflow，但等待完成）
2. 添加进度查询API
3. 添加取消执行API
