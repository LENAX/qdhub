# QDHub

**QDHub量化数据管理系统**：统一管理数据源、API 元数据与同步计划，将外部量化 API（如 Tushare）数据同步到本地 DuckDB，供分析与回测使用。

---

## 功能与特性

### 一、数据源管理

- **数据源注册**：创建/编辑/删除数据源，配置名称、描述、Base URL、文档 URL；支持启用/停用状态。
- **API 元数据**：
  - **元数据爬取**：从数据源文档站（如 Tushare）爬取 API 目录与详情，解析出分类、接口名、请求参数、返回字段（含类型、主键、索引）并落库。
  - **分类与接口**：按 API 分类浏览，支持分页列表、按 ID 删除单条 API 元数据。
  - **公共数据 API**：可配置 `common_data_apis`（如 `trade_cal`、`stock_basic`），同步时优先从本地 DuckDB 复用，减少重复请求。
- **Token 管理**：为数据源设置 API Token，支持校验接口验证 Token 有效性；执行同步时按数据源取 Token。
- **API 同步策略**：为每个 API 配置同步方式，用于后续同步计划依赖解析与任务编排：
  - **同步参数类型**：无参数（`none`）、按交易日全市场（`trade_date`）、按证券代码拆分（`ts_code`）。
  - **日期范围**：是否支持按日期范围查询（`support_date_range`）。
  - **依赖与必填参数**：声明依赖的上游 API、必填参数列表；支持从元数据的 `param_dependencies` 或策略表补全，供依赖解析器生成执行图。

---

### 二、同步管理

- **同步计划（SyncPlan）**：
  - **创建与配置**：选择数据源、目标存储（DataStore）、要同步的 API 列表（`selected_apis`）；可选 Cron 表达式、默认执行参数（日期范围等）。
  - **增量模式**：开启后，定时触发时以「上次成功结束日期」为本次开始日期、当前日期为结束日期；可配置从某 API 表的某列取 `MAX()` 作为起始日期（`incremental_start_date_api/column`）。
  - **解析（Resolve）**：根据 API 依赖与同步策略解析出**执行图**（ExecutionGraph）：层级、任务配置（同步模式、参数映射、依赖），并生成 SyncTask 列表；解析后可启用计划。
  - **执行**：支持**手动触发**与**定时调度**；执行时按执行图提交到 Task Engine，按依赖顺序与参数拆分（如按股票代码）拉取数据并写入目标库。
- **执行与进度**：
  - **进度查询**：单次查询计划执行进度，或 **SSE 流式推送**（`/sync-plans/:id/progress-stream`）实时查看任务完成情况。
  - **执行历史**：查看计划的最近执行记录、单次执行的详情（任务级状态/日志）；支持**取消、暂停、恢复**执行。
- **计划控制**：启用/禁用计划（影响定时调度）；禁用后不再按 Cron 触发，手动执行仍可用。

---

### 三、存储管理

- **数据存储（QuantDataStore）**：
  - **类型**：支持 **DuckDB**（本地文件路径）、**ClickHouse**、**PostgreSQL**（DSN）；创建时指定类型与连接信息（`storage_path` 或 `dsn`）。
  - **CRUD**：创建、列表、详情、更新、删除；支持**连接校验**（Validate），确保路径/DSN 可用。
- **表与数据**：
  - **按数据源建表**：根据某数据源的 API 元数据，在指定 DataStore 中批量创建表结构（字段类型映射为 DuckDB/目标库类型，含主键、索引及同步用 `sync_batch_id`、`created_at`）。
  - **表列表与浏览**：列出存储内所有表；按表分页查询数据，便于预览与排查。
- **数据质量**：
  - **质量报告**：按表生成综合质量报告（缺失分析、重复分析、异常检测、有效区间等）。
  - **有效区间**：分析日期/时间维度的有效数据区间，便于判断可用的起止日期。
  - **缺失/重复/异常分析**：单表维度分析缺失值、重复记录、简单异常；**维度统计**（单维度分布）。
  - **修复执行**：根据分析结果提交修复动作（如删除重复、填充缺失），由服务端执行并返回影响行数。

---

### 四、数据分析

基于同步到 DuckDB 的数据，提供面向量化场景的查询与分析 API（均需认证，部分需 RBAC）：

- **基础数据**：K 线（支持复权、日/周/月）、交易日历、股票列表、股票快照（指定交易日与复权方式）、股票基本信息；指数列表、题材概念列表。
- **涨跌停**：涨跌停统计、涨跌停股票列表、涨停天梯（连板分布）、首板列表、今日/昨日涨停对比、涨停列表（带连板天数）；按板块的涨停统计与涨停股列表。
- **题材与板块**：题材热度、题材成分股、题材轮动统计；分板块涨停统计与个股。
- **财务**：财务指标查询、利润表/资产负债表/现金流量表数据。
- **技术指标**：基于 K 线在服务端计算 MA、RSI、MACD 等技术指标。

---

## 其他说明

- **工作流**：元数据爬取、建表、批量/实时数据同步均以 Task Engine 工作流执行，可查工作流实例状态与日志。
- **权限**：JWT + Casbin，内置 admin（读写）、guest（只读）账号；生产环境需修改默认密码并关闭 Swagger。

---

## 技术栈

| 类别     | 技术 |
|----------|------|
| 后端     | Go 1.24、Gin、SQLite/PostgreSQL/MySQL、DuckDB、Casbin、JWT |
| 任务引擎 | [LENAX/task-engine](https://github.com/LENAX/task-engine) |
| API 文档 | Swagger（`/swagger/index.html`，生产可关闭） |
| 部署     | Docker Compose，支持仅用镜像部署（见 [DEPLOY.md](./DEPLOY.md)） |

前端为独立工程 `qdhub-frontend`，与后端通过 Docker Compose 一起编排。

---

## 系统架构与设计

### 整体架构

QDHub 采用「前端控制台 + 后端服务 + 多存储」的分层架构：

- **前端控制台**：独立工程 `qdhub-frontend`，通过 JWT 调用后端 REST API。
- **后端服务（本仓库）**：
  - 提供数据源/元数据/同步计划/数据分析等 HTTP API。
  - 内部基于 Task Engine 编排批量/增量同步工作流。
- **配置数据库**（SQLite / PostgreSQL / MySQL）：
  - 存储数据源与 API 元数据、同步计划与执行记录、工作流定义、用户与权限规则等。
- **量化数据存储（QuantDataStore）**：
  - DuckDB（默认）、ClickHouse、PostgreSQL 等，用于落地实际行情、财务、因子等数据。
- **任务引擎存储**：
  - 由 [LENAX/task-engine](https://github.com/LENAX/task-engine) 使用的持久化，用于保存工作流实例、任务队列与日志。

可以简单理解为：

- **控制面（Control Plane）**：用户通过前端/HTTP API 管理数据源、API 元数据、同步计划、存储与权限。
- **数据面（Data Plane）**：Task Engine 按计划从外部 API 拉取数据并写入 DuckDB/其他存储，供分析 API 与自定义 SQL 使用。

### 设计初衷：为什么不是 Python 脚本 + crontab

很多量化团队一开始会用「一堆 Python 脚本 + crontab」同步数据，这在规模小、数据源少时是可行的，但会很快遇到：

- **运维复杂且不可观测**：
  - 脚本分散在多台机器/多个人手上，依赖与执行顺序靠人记忆或 README 维护。
  - 出问题时只能看 crontab 日志，很难追踪某次「同步全市场日线」具体跑了哪些步骤、失败在哪个环节。
- **缺少统一的元数据与配置中心**：
  - API 元数据、字段含义、表结构、增量规则分散在各个脚本里，修改一次 API 参数要到处 grep。
  - 难以做到「声明一个数据源 + 一组 API 元数据 → 一键建表 + 一键同步」。
- **难以保证幂等与回滚**：
  - 常见做法是「先删后插」「按时间覆盖」，一旦脚本或上游数据出问题，很难按批次回滚。
  - 检查点、多 API 联动的增量同步逻辑往往写死在脚本里，重复造轮子且容易出错。
- **缺少统一的权限控制与审计**：
  - 谁能改哪些脚本、谁改了同步参数、什么时候触发了大规模重跑，往往没有清晰记录。

QDHub 想做的是把这些「脚本里的隐性约定」显式化、平台化：

- 用**统一的元数据和配置数据库**描述数据源、API、同步策略与执行图，而不是散落在脚本里；
- 用**Task Engine 工作流**取代手写 crontab 依赖，自动处理重试、补偿、进度追踪与日志；
- 用**前端控制台 + RBAC**把同步计划的创建、执行、暂停/恢复和参数修改放到一个可视化界面里；
- 用**DuckDB/QuantDataStore 抽象**把数据存储统一起来，让上层分析与 API 不关心底层是本地文件还是远端库。

对于个人开发者或小团队，QDHub 目标是：**比「脚本 + crontab」多一点工程化成本，但换来长期可维护性和扩展空间**。

### 后端分层设计

后端遵循分层 + 领域驱动的结构，主要目录如下（与下方「项目结构」对应）：

- `cmd/`：进程入口。`server` 子命令读取配置，创建 `container`，启动 HTTP Server 与 Task Engine。
- `internal/interfaces/http/`：
  - Gin 路由与 Handler，负责 HTTP 入参校验、响应编码、SSE 推送等。
  - 通过中间件实现 JWT 认证与 Casbin RBAC 鉴权。
- `internal/application/`：
  - 应用服务层，编排跨领域用例逻辑，如创建同步计划、解析执行图、触发工作流、查询进度等。
  - 只依赖领域模型与仓储接口，不直接依赖具体存储实现。
- `internal/domain/`：
  - 领域模型与领域服务：
    - `metadata`：数据源/接口/字段元数据与解析逻辑。
    - `sync`：同步计划、执行图、任务拆分与增量规则。
    - `workflow`：与 Task Engine 的协作抽象。
    - `datastore`：量化数据存储抽象。
    - `auth`、`analysis` 等。
- `internal/infrastructure/`：
  - 各种适配器与仓储实现：
    - 关系型数据库访问、DuckDB/ClickHouse/PostgreSQL 客户端。
    - 外部数据源适配器（如 Tushare）。
    - Task Engine 集成（Job Functions、工作流模板、实例管理等）。
- `pkg/`：通用基础设施（配置、日志等），尽量不包含业务逻辑。

目标是让**业务规则尽可能停留在 `application`/`domain` 层**，HTTP、DB、外部 API、任务引擎都视为可替换的「外围适配器」。

### Task Engine 工作流与同步

数据同步与增量更新通过 [LENAX/task-engine](https://github.com/LENAX/task-engine) 完成，核心思路：

- **工作流（Workflow）**：
  - 描述一类同步过程（如「全量建表 + 批量同步」「按交易日增量同步」）。
  - 由一系列有依赖关系的任务节点组成（获取元数据 → 创建表结构 → 生成子任务 → 拉取并写入数据）。
- **Job Functions（见 `internal/infrastructure/taskengine/jobs/`）**：
  - `SyncAPIDataJob`：调用数据源 API（如 Tushare），将结果写入 DuckDB/目标存储，可使用 `sync_batch_id` 便于回滚。
  - `GenerateDataSyncSubTasksJob`：根据上游结果（如 `ts_code` 列表、交易日列表）一次性生成大量子任务，实现多股票/多日期并行同步。
  - `GetSyncCheckpointJob` / `UpdateSyncCheckpointJob`：通过 DuckDB 检查点表记录每个 API 的最后同步日期，实现增量同步。
  - `DeleteSyncedDataJob`：按批次 ID 删除已写入数据，用于失败时的补偿（SAGA）。
- **执行图（ExecutionGraph）与同步计划（SyncPlan）**：
  - 同步计划中声明数据源、目标存储、所选 API 及增量规则。
  - 解析（Resolve）后转换为 Task Engine 的执行图与任务列表，由 Worker 按依赖并发执行。

### 数据流与扩展点

**典型数据流（以同步某天全市场行情为例）**：

1. 用户在前端配置数据源（含 Token）、目标存储、需要同步的 API 列表与参数/增量策略。
2. 应用层根据 SyncPlan 解析出执行图：先拉取交易日历 → 按交易日生成子任务 → 每个子任务实际调用行情 API。
3. Task Engine 执行 Job Functions，将数据写入 DuckDB/目标存储，并记录 `sync_batch_id` 与检查点。
4. 分析/查询接口基于 DuckDB/量化存储，提供面向量化的查询 API 或只读 SQL。

**主要扩展点**：

- 接入新数据源：在 `infrastructure/datasource` 中实现新的数据源 Client，注册到 Registry；在元数据与同步策略中声明后即可被 SyncPlan 使用。
- 支持新存储：在 `datastore`/`infrastructure` 中实现新的 QuantDataStore 适配器。
- 新增分析能力：在 `domain/analysis` + `application` + `interfaces/http` 中扩展分析用例与 API。
- 新增工作流模板：在 `taskengine/workflows`/`jobs` 中增加新的 Job Function 与 Workflow 定义，即可支持新的同步/清洗/质量修复流程。

---

## 项目结构

```
qdhub/
├── cmd/                    # 入口：server、docs、version
├── configs/                # 配置文件（config.yaml、config.prod.yaml）
├── docs/                   # Swagger 生成文档
├── internal/
│   ├── application/       # 应用服务
│   ├── domain/            # 领域：metadata、sync、workflow、datastore、auth、analysis
│   ├── infrastructure/    # 持久化、Task Engine、数据源适配器（如 Tushare）
│   └── interfaces/http/   # HTTP 路由与 Handler
├── migrations/             # 数据库迁移
├── pkg/                    # 公共包（如 config）
├── scripts/                # 迁移、部署脚本
└── tests/                 # 单元、集成、E2E 测试
```

仓库根目录还包含：

- `docker-compose.yml`：后端 + 前端服务与数据卷。
- `docker-compose.image.yml`：仅用镜像拉取与运行（阿里云等环境）。
- `DEPLOY.md`：**阿里云部署说明**（CI 自动构建推送、本地构建、ECS 仅用镜像运行、Nginx HTTPS）。

---

## 快速开始

### 环境要求

- Go 1.24+
- 可选：Docker / Docker Compose（推荐用 Compose 跑全栈）

### 本地运行后端

```bash
cd qdhub
make deps
make run          # 或: make run-dev（go run）
# 默认：http://0.0.0.0:8080，配置见 configs/config.yaml
```

- 健康检查：`GET /health`
- API 文档：`GET /swagger/index.html`

### Docker Compose（后端 + 前端）

在**仓库根目录**（有 `docker-compose.yml` 的目录）：

```bash
# 开发：热重载（需 docker-compose.override.yml）
docker compose up

# 生产模式：后台运行
docker compose up -d
```

- 前端：`http://localhost:3001`
- 后端 API：`http://localhost:8080`

数据与日志默认挂载到 `./qdhub/data`、`./qdhub/logs`，可通过环境变量 `QDHUB_DATA_DIR`、`QDHUB_LOG_DIR` 覆盖。

### 默认账号（仅开发）

| 账号  | 密码    | 说明   |
|-------|---------|--------|
| admin | admin123 | 读写   |
| guest | guest123 | 只读   |

**生产环境务必**通过配置或环境变量 `QDHUB_AUTH_ADMIN_PASSWORD`、`QDHUB_AUTH_GUEST_PASSWORD` 修改为强密码，并关闭 Swagger（见下方配置）。

---

## 配置说明

主配置：`qdhub/configs/config.yaml`。关键项：

- **server**：host、port、`enable_swagger`（生产建议 `false`）。
- **database**：`driver`（sqlite/postgres）、`dsn`。
- **quantdb**：`duckdb_path`，同步目标 DuckDB 路径。
- **task_engine**：`worker_count`、`task_timeout`。
- **auth**：`admin_password`、`guest_password`（或使用环境变量覆盖）。
- **datasources**：如 Tushare 的 `enabled`、`base_url` 等。

环境变量会覆盖 YAML 中同名配置（如 `QDHUB_SERVER_ENABLE_SWAGGER`、`QDHUB_AUTH_ADMIN_PASSWORD`），部署时常用。

---

## 开发与测试

```bash
cd qdhub
make test-unit          # 单元测试
make test-integration   # 集成测试（需 -tags=integration）
make test-e2e           # E2E 测试（需 -tags=e2e）
make lint               # 代码检查
make docs               # 重新生成 Swagger 文档
```

---

## 部署

- **阿里云 ECS（仅用镜像、不上传源码）**：push tag 触发 CI 自动构建推送，或本地构建后推送到 ACR；ECS 上只保留 `docker-compose.image.yml` 与 `.env`，`pull` 后 `up`。步骤、环境变量与 Nginx HTTPS 示例见 **[DEPLOY.md](./DEPLOY.md)**。

---

## 许可证

见仓库内 LICENSE 文件（如有）。
