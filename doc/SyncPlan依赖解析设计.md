# SyncPlan 字段级依赖解析设计文档

## 1. 背景与问题

### 1.1 现状分析

当前 Tushare 数据同步存在以下问题：

- `batch_data_sync.go` 中 `APISyncConfig` 需要手动配置依赖和参数映射
- `sync_jobs.go` 中 `GenerateDataSyncSubTasks` 硬编码 `param_key` 逻辑
- 现有 `SyncJob` 是单 API 粒度的独立聚合根，无法表达多 API 之间的依赖关系
- 用户必须手动指定完整的依赖链，无法根据选择的 API 自动推断

### 1.2 需求

- 依赖 `stock_basic` 的任务，参数 `ts_code` 来源于 `stock_basic.ts_code`
- 依赖 `trade_cal` 的任务，参数 `trade_date` 来源于 `trade_cal.cal_date`
- 用户选择部分 API 时，系统自动补充缺失的基础依赖 API
- 依赖规则可复用于其他数据源（JoinQuant、Wind 等）
- **SyncPlan 作为唯一聚合根**，SyncTask 记录单个 API 的同步参数

---

## 2. 架构设计

### 2.1 核心设计决策

1. **SyncPlan 为唯一聚合根**：管理多 API 同步计划的完整生命周期
2. **SyncTask 为聚合内实体**：记录单个 API 的同步参数配置（替代原 SyncJob 的参数部分）
3. **SyncExecution 保留**：记录每次执行的状态和结果
4. **ExecuteSyncPlan 为核心方法**：将 SyncPlan 转换为内建 workflow 参数并执行

### 2.2 数据流

```
用户选择 API -> DependencyResolver 解析依赖 -> 生成 ExecutionGraph
                                                    |
                                                    v
                                            创建 SyncPlan + SyncTasks
                                                    |
                                                    v
执行时: SyncPlan -> 转换为 APISyncConfig[] -> WorkflowExecutor -> TaskEngine
```

---

## 3. 领域模型设计

### 3.1 聚合根：SyncPlan

```go
type SyncPlan struct {
    ID              shared.ID
    Name            string
    Description     string
    DataSourceID    shared.ID
    DataStoreID     shared.ID              // 目标数据存储
    
    // 用户配置
    SelectedAPIs    []string               // 用户选择的 API 列表
    
    // 解析结果
    ResolvedAPIs    []string               // 解析后的完整 API 列表（含自动补充的）
    ExecutionGraph  *ExecutionGraph        // 依赖解析后的执行图
    
    // 调度配置
    CronExpression  *string                // 定时表达式（可选）
    
    // 状态
    Status          PlanStatus             // draft | resolved | enabled | disabled | running
    LastExecutedAt  *time.Time
    NextExecuteAt   *time.Time
    
    // 时间戳
    CreatedAt       shared.Timestamp
    UpdatedAt       shared.Timestamp
    
    // 聚合内实体（懒加载）
    Tasks           []SyncTask             // 各 API 的同步配置
    Executions      []SyncExecution        // 执行记录
}
```

### 3.2 聚合内实体：SyncTask

```go
type SyncTask struct {
    ID            shared.ID
    SyncPlanID    shared.ID
    APIName       string                   // API 名称
    SyncMode      SyncMode                 // direct | template
    
    // 参数配置
    Params        map[string]interface{}   // 固定参数
    ParamMappings []ParamMapping           // 参数映射规则（从上游获取）
    
    // 任务级依赖
    Dependencies  []string                 // 依赖的任务名称
    
    // 排序
    Level         int                      // 执行层级（0=无依赖）
    SortOrder     int                      // 同层级内的排序
    
    // 同步频率控制
    SyncFrequency time.Duration            // 同步频率（如 24h, 168h=7d, 720h=30d）
    LastSyncedAt  *time.Time               // 上次成功同步时间
    
    CreatedAt     shared.Timestamp
}
```

### 3.3 值对象：ExecutionGraph

```go
type ExecutionGraph struct {
    Levels      [][]string                    // 分层执行顺序
    MissingAPIs []string                      // 自动补充的依赖 API
    TaskConfigs map[string]*TaskConfig        // 每个 API 的任务配置
}

type TaskConfig struct {
    APIName       string
    SyncMode      SyncMode
    Dependencies  []string
    ParamMappings []ParamMapping
}
```

### 3.4 metadata 领域扩展：ParamDependency

```go
type ParamDependency struct {
    ParamName    string `json:"param_name"`     // 参数名，如 "ts_code"
    SourceAPI    string `json:"source_api"`     // 来源 API，如 "stock_basic"
    SourceField  string `json:"source_field"`   // 来源字段，如 "ts_code"
    IsList       bool   `json:"is_list"`        // 是否是列表
    FilterField  string `json:"filter_field"`   // 过滤字段（可选）
    FilterValue  any    `json:"filter_value"`   // 过滤值（可选）
}

// 扩展 APIMetadata
type APIMetadata struct {
    // ... 现有字段 ...
    ParamDependencies []ParamDependency `json:"param_dependencies"` // 新增
}
```

---

## 4. 领域服务

### 4.1 DependencyResolver

```go
type DependencyResolver interface {
    // Resolve 解析依赖关系
    Resolve(selectedAPIs []string, allAPIDependencies map[string][]ParamDependency) (*ExecutionGraph, error)
}
```

### 4.2 解析算法

1. 用户选择 API 列表
2. 构建依赖图
3. 检测循环依赖（有循环则返回错误）
4. BFS 查找所有依赖
5. 补充缺失的基础 API
6. 拓扑排序
7. 生成执行层级
8. 为每个 API 生成 TaskConfig
9. 返回 ExecutionGraph

---

## 5. Workflow 集成

### 5.1 ExecutionGraph -> APISyncConfig 转换

- 模板任务（template）：设置 `ParamKey` 和 `UpstreamTask`，触发 `GenerateDataSyncSubTasks`
- 直接任务（direct）：设置 `UpstreamParams` 参数映射，触发 `SyncAPIData` 的上游参数解析

### 5.2 同步频率处理

基础 API（如 `trade_cal`, `stock_basic`）不需要每次都同步：

| API 类型 | 推荐频率 | 说明 |
|----------|----------|------|
| trade_cal | 30d | 交易日历变化很少 |
| stock_basic | 7d | 股票列表每周可能有新股 |
| daily, adj_factor | 0（每次） | 日线数据需要实时 |

### 5.3 WorkflowExecutor 扩展

新增 `ExecuteBatchDataSyncWithConfigs` 方法，接收 `APIConfigs` 和 `SkipBaseAPIs` 参数。

---

## 6. 数据库设计

```sql
-- 同步计划表
CREATE TABLE sync_plan (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    data_source_id TEXT NOT NULL,
    data_store_id TEXT,
    selected_apis TEXT NOT NULL,           -- JSON array
    resolved_apis TEXT,                    -- JSON array
    execution_graph TEXT,                  -- JSON
    cron_expression TEXT,
    status TEXT NOT NULL DEFAULT 'draft',
    last_executed_at DATETIME,
    next_execute_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (data_source_id) REFERENCES data_source(id)
);

-- 同步任务表
CREATE TABLE sync_task (
    id TEXT PRIMARY KEY,
    sync_plan_id TEXT NOT NULL,
    api_name TEXT NOT NULL,
    sync_mode TEXT NOT NULL DEFAULT 'direct',
    params TEXT,                           -- JSON
    param_mappings TEXT,                   -- JSON array
    dependencies TEXT,                     -- JSON array
    level INTEGER DEFAULT 0,
    sort_order INTEGER DEFAULT 0,
    sync_frequency INTEGER DEFAULT 0,      -- 纳秒
    last_synced_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sync_plan_id) REFERENCES sync_plan(id) ON DELETE CASCADE
);

-- 同步执行记录表
CREATE TABLE sync_execution (
    id TEXT PRIMARY KEY,
    sync_plan_id TEXT NOT NULL,
    workflow_inst_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    started_at DATETIME,
    finished_at DATETIME,
    record_count INTEGER DEFAULT 0,
    error_message TEXT,
    execute_params TEXT,                   -- JSON
    synced_apis TEXT,                      -- JSON array
    skipped_apis TEXT,                     -- JSON array
    FOREIGN KEY (sync_plan_id) REFERENCES sync_plan(id)
);

-- 扩展 API 元数据表
ALTER TABLE api_metadata ADD COLUMN param_dependencies TEXT;
```

---

## 7. 使用示例

```go
// 1. 创建同步计划
plan, _ := syncService.CreateSyncPlan(ctx, contracts.CreateSyncPlanRequest{
    Name:         "日常行情同步",
    DataSourceID: tushareID,
    SelectedAPIs: []string{"daily", "adj_factor", "top_list"},
    // 系统自动解析依赖，补充 stock_basic 和 trade_cal
})

// 2. 执行同步计划
execID, _ := syncService.ExecuteSyncPlan(ctx, plan.ID, contracts.ExecuteSyncPlanParams{
    TargetDBPath: "/data/quant.duckdb",
    StartDate:    "20251201",
    EndDate:      "20251231",
})

// 3. 查看执行状态
exec, _ := syncService.GetExecution(ctx, execID)
fmt.Printf("Status: %s, Records: %d\n", exec.Status, exec.RecordCount)
```

---

## 8. 文件变更清单

| 文件路径 | 操作 | 说明 |
|---------|------|------|
| `domain/metadata/entity.go` | 修改 | 新增 ParamDependency |
| `domain/sync/entity.go` | 重构 | SyncPlan 聚合根，SyncTask 实体 |
| `domain/sync/service.go` | 修改 | 新增 DependencyResolver 接口 |
| `domain/sync/service_impl.go` | 修改 | 实现 DependencyResolver |
| `domain/sync/repository.go` | 重构 | SyncPlanRepository |
| `application/contracts/sync.go` | 重构 | SyncPlan 核心接口 |
| `application/impl/sync_impl.go` | 重构 | ExecuteSyncPlan 实现 |
| `domain/workflow/service.go` | 修改 | ExecuteBatchDataSyncWithConfigs |
| `migrations/xxx_sync_plan.sql` | 新增 | 数据库迁移 |
