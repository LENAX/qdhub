# QDHub - 量化数据管理平台

QDHub 是一个用于管理和同步量化数据源的平台，支持从多个数据源（如 Tushare）自动爬取元数据、创建数据表、批量同步和实时同步数据。

## 功能特性

- 📊 **数据源管理**：支持注册和管理多个数据源
- 🔍 **元数据爬取**：自动从数据源文档爬取 API 元数据
- 🎯 **API 同步策略**：为每个 API 配置同步策略，支持依赖关系管理
- 🗄️ **表结构生成**：根据元数据自动生成数据库表结构
- 📥 **同步计划**：统一的数据同步管理，支持依赖解析、批量执行、定时调度
- ⚡ **实时进度监控**：支持轮询和 SSE 流式监控，实时查看执行进度
- 🔄 **工作流引擎**：基于 Task Engine 的强大工作流系统
- 🎯 **内建工作流**：开箱即用的元数据爬取、建表、数据同步工作流

## 快速开始

### 1. 启动服务

```bash
# 使用默认配置启动（SQLite数据库，端口8080）
./bin/qdhub server

# 或指定配置
./bin/qdhub server --host 0.0.0.0 --port 8080 --mode release
```

服务启动后，会自动初始化内建工作流：
- `metadata_crawl` - 元数据爬取
- `create_tables` - 创建数据表
- `batch_data_sync` - 批量数据同步
- `realtime_data_sync` - 实时数据同步

### 2. 验证服务

访问 Swagger UI 查看 API 文档：
```
http://localhost:8080/swagger/index.html
```

### 3. 创建数据源

以 Tushare 为例：

```bash
curl -X POST http://localhost:8080/api/v1/datasources \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Tushare",
    "description": "Tushare Pro Data Source",
    "base_url": "http://api.tushare.pro",
    "doc_url": "https://tushare.pro/document/2"
  }'
```

响应示例：
```json
{
  "code": 201,
  "data": {
    "id": "ds_xxxxx",
    "name": "Tushare",
    "description": "Tushare Pro Data Source",
    "base_url": "http://api.tushare.pro",
    "doc_url": "https://tushare.pro/document/2"
  }
}
```

### 4. 设置 Token（如果需要）

```bash
curl -X POST http://localhost:8080/api/v1/datasources/{data_source_id}/token \
  -H "Content-Type: application/json" \
  -d '{
    "token": "your_tushare_token"
  }'
```

### 5. 执行元数据爬取

使用内建工作流爬取元数据：

```bash
curl -X POST http://localhost:8080/api/v1/workflows/built-in/metadata_crawl/execute \
  -H "Content-Type: application/json" \
  -d '{
    "trigger_type": "manual",
    "trigger_params": {
      "data_source_id": "ds_xxxxx",
      "data_source_name": "tushare",
      "max_api_crawl": 0
    }
  }'
```

响应示例：
```json
{
  "code": 200,
  "data": {
    "instance_id": "inst_xxxxx",
    "status": "started"
  }
}
```

### 6. 查看工作流执行状态

```bash
# 查看工作流实例状态（轮询方式）
curl http://localhost:8080/api/v1/instances/{instance_id}/progress

# 实时流式监控进度（SSE，推荐）
curl http://localhost:8080/api/v1/instances/{instance_id}/progress-stream

# 查看任务实例列表
curl http://localhost:8080/api/v1/instances/{instance_id}/tasks
```

**使用 SSE 实时监控进度：**

SSE（Server-Sent Events）接口提供实时进度更新，适合长时间运行的任务：

```bash
# 使用 curl 监听 SSE 流
curl -N http://localhost:8080/api/v1/instances/{instance_id}/progress-stream

# 或使用 JavaScript EventSource API
const eventSource = new EventSource('http://localhost:8080/api/v1/instances/{instance_id}/progress-stream');
eventSource.onmessage = (event) => {
  const progress = JSON.parse(event.data);
  console.log(`进度: ${progress.progress}%, 状态: ${progress.status}`);
};
```

### 7. 查看爬取的元数据

```bash
# 查看数据源的所有 API
curl http://localhost:8080/api/v1/datasources/{data_source_id}/apis

# 查看 API 详情
curl http://localhost:8080/api/v1/apis/{api_id}
```

## 完整工作流示例

### 场景：从 Tushare 爬取元数据并创建数据表

#### 步骤 1：创建数据源

```bash
DATA_SOURCE_ID=$(curl -s -X POST http://localhost:8080/api/v1/datasources \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Tushare",
    "description": "Tushare Pro Data Source",
    "base_url": "http://api.tushare.pro",
    "doc_url": "https://tushare.pro/document/2"
  }' | jq -r '.data.id')

echo "Data Source ID: $DATA_SOURCE_ID"
```

#### 步骤 2：设置 Token

```bash
curl -X POST http://localhost:8080/api/v1/datasources/$DATA_SOURCE_ID/token \
  -H "Content-Type: application/json" \
  -d '{
    "token": "your_tushare_token"
  }'
```

#### 步骤 3：执行元数据爬取

```bash
INSTANCE_ID=$(curl -s -X POST http://localhost:8080/api/v1/workflows/built-in/metadata_crawl/execute \
  -H "Content-Type: application/json" \
  -d "{
    \"trigger_type\": \"manual\",
    \"trigger_params\": {
      \"data_source_id\": \"$DATA_SOURCE_ID\",
      \"data_source_name\": \"tushare\",
      \"max_api_crawl\": 0
    }
  }" | jq -r '.data.instance_id')

echo "Workflow Instance ID: $INSTANCE_ID"
```

#### 步骤 4：等待爬取完成

```bash
# 轮询检查状态
while true; do
  STATUS=$(curl -s http://localhost:8080/api/v1/instances/$INSTANCE_ID/progress | jq -r '.data.status')
  echo "Status: $STATUS"
  if [ "$STATUS" = "Success" ] || [ "$STATUS" = "Failed" ]; then
    break
  fi
  sleep 5
done
```

#### 步骤 5：查看爬取的 API 列表

```bash
curl http://localhost:8080/api/v1/datasources/$DATA_SOURCE_ID/apis | jq '.data[] | {id, name, display_name}'
```

## API 端点

### 数据源管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/datasources` | 列出所有数据源 |
| POST | `/api/v1/datasources` | 创建数据源 |
| GET | `/api/v1/datasources/:id` | 获取数据源详情 |
| POST | `/api/v1/datasources/:id/refresh` | 刷新元数据（触发元数据爬取工作流） |
| POST | `/api/v1/datasources/:id/token` | 设置 Token |
| GET | `/api/v1/datasources/:id/token` | 获取 Token（不返回实际值） |

### API 同步策略管理

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/datasources/:id/api-sync-strategies` | 创建 API 同步策略 |
| GET | `/api/v1/datasources/:id/api-sync-strategies` | 列出数据源的所有 API 同步策略 |
| GET | `/api/v1/api-sync-strategies/:id` | 获取 API 同步策略详情 |
| PUT | `/api/v1/api-sync-strategies/:id` | 更新 API 同步策略 |
| DELETE | `/api/v1/api-sync-strategies/:id` | 删除 API 同步策略 |

### 数据存储管理

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/datastores` | 创建数据存储配置 |
| GET | `/api/v1/datastores` | 列出所有数据存储 |
| GET | `/api/v1/datastores/:id` | 获取数据存储详情 |
| POST | `/api/v1/datastores/:id/create-tables` | 为数据源创建所有 API 的表（触发建表工作流） |

### 同步计划管理

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/sync-plans` | 创建同步计划 |
| GET | `/api/v1/sync-plans` | 列出所有同步计划 |
| GET | `/api/v1/sync-plans/:id` | 获取同步计划详情 |
| PUT | `/api/v1/sync-plans/:id` | 更新同步计划 |
| DELETE | `/api/v1/sync-plans/:id` | 删除同步计划 |
| POST | `/api/v1/sync-plans/:id/resolve` | 解析同步计划的依赖关系 |
| POST | `/api/v1/sync-plans/:id/trigger` | 手动触发同步计划执行 |
| POST | `/api/v1/sync-plans/:id/enable` | 启用同步计划（注册定时任务） |
| POST | `/api/v1/sync-plans/:id/disable` | 禁用同步计划（取消定时任务） |
| GET | `/api/v1/sync-plans/:id/progress` | 获取同步计划进度（轮询） |
| GET | `/api/v1/sync-plans/:id/progress-stream` | 实时流式监控同步计划进度（SSE） |
| GET | `/api/v1/sync-plans/:id/executions` | 列出同步计划的所有执行记录 |
| GET | `/api/v1/executions/:id` | 获取执行记录详情 |
| POST | `/api/v1/executions/:id/cancel` | 取消执行 |

### 工作流实例管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/instances` | 列出工作流实例（需提供 workflow_id 查询参数） |
| GET | `/api/v1/instances/:id` | 获取实例详情 |
| GET | `/api/v1/instances/:id/progress` | 获取实例进度（轮询） |
| GET | `/api/v1/instances/:id/progress-stream` | 实时流式监控实例进度（SSE） |
| GET | `/api/v1/instances/:id/tasks` | 获取任务实例列表 |
| POST | `/api/v1/instances/:id/cancel` | 取消工作流实例 |

### 内建工作流

| 名称 | API名称 | 说明 |
|------|---------|------|
| 元数据爬取 | `metadata_crawl` | 从数据源爬取API文档并保存元数据 |
| 创建数据表 | `create_tables` | 根据元数据创建数据表结构 |
| 批量数据同步 | `batch_data_sync` | 批量同步历史数据到目标数据库 |
| 实时数据同步 | `realtime_data_sync` | 实时增量同步数据到目标数据库 |

## 内建工作流参数说明

### metadata_crawl（元数据爬取）

通过 `/api/v1/datasources/:id/refresh` 接口触发，或直接调用工作流：

```json
{
  "trigger_type": "manual",
  "trigger_params": {
    "data_source_id": "数据源ID",
    "data_source_name": "数据源名称（如：tushare）",
    "max_api_crawl": 0  // 0表示不限制
  }
}
```

### create_tables（创建数据表）

通过 `/api/v1/datastores/:id/create-tables` 接口触发，或直接调用工作流：

```json
{
  "trigger_type": "manual",
  "trigger_params": {
    "data_source_id": "数据源ID",
    "data_source_name": "数据源名称",
    "data_store_id": "数据存储ID",
    "max_tables": 0  // 0表示不限制
  }
}
```

### batch_data_sync（批量数据同步）

通过同步计划（SyncPlan）执行，支持依赖解析和自动调度。创建同步计划后，使用 `/api/v1/sync-plans/:id/trigger` 触发执行。

### realtime_data_sync（实时数据同步）

通过同步计划（SyncPlan）执行，支持 Cron 定时调度。创建同步计划时设置 `cron_expression`，然后启用计划即可自动执行。

## 同步计划（SyncPlan）使用说明

同步计划是推荐的数据同步方式，支持：
- **依赖解析**：自动解析 API 之间的依赖关系，按正确顺序执行
- **批量执行**：支持选择多个 API 批量同步
- **定时调度**：支持 Cron 表达式定时执行
- **进度监控**：支持轮询和 SSE 流式监控

### 创建同步计划示例

```bash
curl -X POST http://localhost:8080/api/v1/sync-plans \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Tushare 日线数据同步",
    "description": "同步 Tushare 的日线行情数据",
    "data_source_id": "ds_xxxxx",
    "data_store_id": "store_xxxxx",
    "selected_apis": ["daily", "stock_basic"],
    "cron_expression": "0 0 18 * * 1-5"
  }'
```

### 解析依赖关系

```bash
curl -X POST http://localhost:8080/api/v1/sync-plans/{plan_id}/resolve
```

### 触发执行

```bash
curl -X POST http://localhost:8080/api/v1/sync-plans/{plan_id}/trigger \
  -H "Content-Type: application/json" \
  -d '{
    "target_db_path": "./data/quant.db",
    "start_date": "20240101",
    "end_date": "20241231"
  }'
```

### 实时监控进度（SSE）

```bash
# 使用 curl 监听 SSE 流
curl -N http://localhost:8080/api/v1/sync-plans/{plan_id}/progress-stream
```

## 配置说明

### 数据库配置

默认使用 SQLite，数据库文件位于 `./data/qdhub.db`。

可以通过配置文件或环境变量修改：

```yaml
database:
  driver: "sqlite"  # sqlite | postgres
  dsn: "./data/qdhub.db"
```

### 服务器配置

```yaml
server:
  host: "0.0.0.0"
  port: 8080
  mode: "release"  # debug | release | test
```

## 开发

### 构建

```bash
make build
```

### 运行测试

```bash
# 单元测试
make test

# 集成测试
make test-integration

# E2E测试
make test-e2e
```

### 生成 Swagger 文档

```bash
make swagger
```

## 架构

QDHub 采用领域驱动设计（DDD）架构：

- **Domain Layer**：领域模型和业务逻辑
- **Application Layer**：应用服务，编排业务用例
- **Infrastructure Layer**：基础设施实现（数据库、Task Engine等）
- **Interface Layer**：HTTP API 接口

## 许可证

[添加许可证信息]

## 贡献

欢迎提交 Issue 和 Pull Request！
