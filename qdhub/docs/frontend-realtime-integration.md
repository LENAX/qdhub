## QDHub 前端实时行情接入指南（realtime\_quote / realtime\_tick / realtime\_list）

本文面向 `qdhub-frontend`，说明如何通过后端 REST API 接入实时行情能力，以及本次改动带来的行为变化。

---

## 一、整体能力与数据流

- **支持的实时接口（Tushare 兼容命名）**
  - **`realtime_quote`**：个股实时盘口快照（多标的，轮询拉取，写入 DuckDB 表 `realtime_quote`）。
  - **`realtime_tick`**：个股当日分笔成交（单标的，内部 SSE Push，写入 DuckDB 表 `realtime_tick`）。
  - **`realtime_list`**：全市场实时涨跌榜（多标的，轮询拉取，写入 DuckDB 表 `realtime_list`）。
- **数据源**
  - **新浪（`src="sina"`）**：通过新浪行情接口获取 `realtime_quote` / `realtime_list` / `realtime_tick`。
  - **东方财富（`src="dc"`）**：
    - `realtime_quote`：东财单只股票实时盘口，支持大批量股票 Streaming 同步。
    - `realtime_tick`：通过东财 SSE 流接口获取分笔成交（内部真正 SSE Push）。
    - `realtime_list`：通过东财全市场列表接口并发分页拉取全量 A 股实时涨跌榜。
- **典型前端使用方式**
  - 前端**不直接调用外部行情源**，只通过 QDHub 后端：
    1. 管理 `SyncPlan`（计划模式 `plan_mode="realtime"`，选择实时 API）。
    2. 触发实时同步（Streaming 工作流）。
    3. 通过分析/自定义 SQL API 从 DuckDB 读取 `realtime_quote` / `realtime_tick` / `realtime_list` 表，用于看板或策略 UI。

---

## 二、创建实时同步计划（SyncPlan）接口规范

前端通过 `/api/v1/sync-plans` 管理同步计划；实时行情走 **Realtime 模式**。

- **创建 SyncPlan**
  - **URL**: `POST /api/v1/sync-plans`
  - **认证**: `Authorization: Bearer <JWT>`
  - **请求体（关键字段）**：

```json
{
  "name": "实时行情（新浪 quote+tick）",
  "description": "演示：使用新浪实时盘口 + 分笔",
  "data_source_id": "<tushare 数据源 ID>",
  "data_store_id": "<DuckDB DataStore ID>",
  "selected_apis": ["realtime_quote", "realtime_tick"],
  "plan_mode": "realtime",
  "pull_interval_seconds": 1,
  "default_execute_params": {
    "src": "sina"
  }
}
```

- **字段说明（与前端表单对应）**
  - **`data_source_id`**：通常选 `tushare` 这个数据源（后端会根据 Tushare 元数据与同步策略解析实时接口）。
  - **`data_store_id`**：目标 DuckDB/其他 QuantDataStore，将实时数据落到其中。
  - **`selected_apis`**：
    - 可选值包括：`"realtime_quote"`, `"realtime_tick"`, `"realtime_list"`（可单选，也可多选组合）。
  - **`plan_mode`**：
    - **必须为 `"realtime"`**，才能触发 Streaming 工作流，而不是批量历史同步。
  - **`pull_interval_seconds`**：
    - 对 **`realtime_quote` / `realtime_list` 有效**：控制轮询间隔（建议 1–5 秒）。
    - 对 **`realtime_tick`（东财 SSE Push）无实际意义**：底层通过长连接推送，无需轮询。
  - **`default_execute_params`**：
    - 传递给实时适配器的默认参数（兼容 Tushare 语义），核心是：
      - **`src`**：数据源路由，常用取值：
        - `"sina"`：使用新浪行情源。
        - `"dc"`：使用东方财富行情源（后端内部会路由到 Eastmoney Adapter）。

> 建议前端的「创建实时计划」表单里：
> - 提供 `plan_mode` 单选：`batch` / `realtime`，实时计划强制设为 `realtime`；
> - 提供 `selected_apis` 多选框：`realtime_quote` / `realtime_tick` / `realtime_list`；
> - 提供 `src` 数据源单选：`sina` / `eastmoney`（存到 `default_execute_params.src`）。

---

## 三、执行与监控实时同步

- **解析计划依赖（Resolve）**
  - **URL**: `POST /api/v1/sync-plans/{id}/resolve`
  - 前端可在创建或修改 SyncPlan 后调用一次，用于：
    - 根据 Tushare API 同步策略（`migrations/019_realtime_api_sync_strategy.up.sql`）解析出：
      - `realtime_quote`：需要的 `ts_code` 列表（依赖 `stock_basic`）等；
      - `realtime_tick`：每次按单只股票拆分；
      - `realtime_list`：无必填参数，全市场快照。

- **启用/禁用计划**
  - **启用**: `POST /api/v1/sync-plans/{id}/enable`
  - **禁用**: `POST /api/v1/sync-plans/{id}/disable`

- **触发一次实时 Streaming 执行**
  - **URL**: `POST /api/v1/sync-plans/{id}/execute`
  - 建议前端在「实时行情开关」或「手动启动按钮」处调用。

- **进度查询 & SSE 进度流**
  - **一次性查询**: `GET /api/v1/sync-plans/{id}/progress`
  - **SSE 流式进度**: `GET /api/v1/sync-plans/{id}/progress-stream`
    - 前端可通过 EventSource 订阅，实时展示「本次 Streaming 实例」的任务执行情况。

---

## 四、DuckDB 表结构与前端查询建议

前端查询/图表展示通常通过「分析 API」或者「自定义 SQL 执行」来实现，核心是知道落库表名与字段名。

- **`realtime_quote`（实时盘口快照）**
  - **表名**：`realtime_quote`
  - **关键字段**（与 Tushare 兼容）：
    - `ts_code`, `name`, `open`, `pre_close`, `price`, `high`, `low`,
    - `bid`, `ask`, `volume`, `amount`,
    - 一、二、三、四、五档买卖盘：`b1_v/b1_p` ... `b5_v/b5_p`, `a1_v/a1_p` ... `a5_v/a5_p`,
    - `date`, `time`, `trade_time`, `close`, `vol`。
  - **前端常见使用方式**
    - 实时看板：按 `ts_code` 过滤，展示 `price`、`pct_change`（可由前端计算）等字段。
    - 大盘监控：按 `price` 或 `amount` 排序，取前 N 行。

- **`realtime_tick`（当日分笔成交）**
  - **表名**：`realtime_tick`
  - **字段**：
    - `ts_code`：股票代码；
    - `time`：成交时间（如 `09:30:01`）；
    - `price`：成交价格；
    - `change`：价格变动（当前实现对东财为 0，后续可扩展）；
    - `volume`：成交量（手）；
    - `amount`：成交额（元，东财为 `price * volume * 100` 估算）；
    - `type`：成交性质（`买盘`/`卖盘`/`中性盘`）。
  - **东财 `realtime_tick` 的实现差异**
    - 内部通过 **SSE Push 模式**持续接收分笔数据，而非轮询 HTTP；
    - 对前端透明：只需配置 `selected_apis=["realtime_tick"]` 且 `src="dc"`/`eastmoney` 即可。

- **`realtime_list`（全市场涨跌榜）**
  - **表名**：`realtime_list`
  - **字段**（与 Tushare `realtime_list` 元数据一致）：
    - `ts_code`, `name`, `price`, `pct_change`, `change`,
    - `volume`, `amount`, `swing`, `high`, `low`, `open`, `close`,
    - `vol_ratio`, `turnover_rate`, `pe`, `pb`,
    - `total_mv`, `float_mv`, `rise`,
    - `"5min"`, `"60day"`, `"1tyear"`。
  - **前端常见使用方式**
    - 涨跌幅榜页面：按 `pct_change` 倒序，分页展示；
    - 资金流/市值视图：使用 `amount`、`total_mv` / `float_mv` 做聚合或筛选。

---

## 五、变更点总结（相对旧版本）

- **接口层（HTTP）**
  - **未增加新的 HTTP 路由**，前端仍然通过现有的 `/api/v1/sync-plans` 系列接口管理计划。
  - 变化点集中在 **`SyncPlan` 配置和 Streaming 行为**：
    - 允许在 `selected_apis` 中选择 `realtime_tick` / `realtime_list`；
    - `plan_mode="realtime"` 的计划会被路由到新的 Streaming 工作流（`RealtimeMarketStreaming`）。

- **行为层（后台实现，对前端透明）**
  - **东方财富 RealtimeAdapter 增强**：
    - 新增 `realtime_list`：全市场实时涨跌榜，多页并发拉取，按 Tushare 字段映射到 `realtime_list` 表。
    - 新增 `realtime_tick`：
      - Pull 模式：单次 SSE 会话读取一批分笔快照（用于非 Streaming 场景）。
      - Push 模式：真正的 SSE 长连接 + `TickPushCollector`，持续向 Task Engine 推送分笔数据。
  - **Streaming Collector 扩展**：
    - `QuotePullCollector` 现在支持 `realtime_list`，会在每个轮询周期拉一次全市场行情并写入 `realtime_list`。
    - `TickPushCollector` 用于 `realtime_tick` + 东财 SSE Push，代替旧的轮询 Fetch。

- **前端需要关注的唯一变更点**
  - 在实时计划配置 UI 中：
    - **增加 `realtime_list` / `realtime_tick` 的选择项**；
    - 显式暴露 `src` 数据源选项（`sina` / `eastmoney`），并写入 `default_execute_params.src`；
    - 对 `realtime_tick` 说明其为「推送模式」，`pull_interval_seconds` 仅对 quote/list 生效。

---

## 六、推荐前端交互示例

- **实时行情页（盘口 + 涨跌榜）**
  - 顶部选择数据源：`新浪` / `东方财富`。
  - 创建或选择一个 `plan_mode="realtime"` 的 SyncPlan：
    - `selected_apis=["realtime_quote","realtime_list"]`；
    - `default_execute_params.src` 根据用户选择设置；
  - 点击「启动实时同步」→ 调用 `POST /sync-plans/{id}/execute`。
  - 通过进度流 `/sync-plans/{id}/progress-stream` 展示「同步运行中」状态；
  - 刷新页面数据时，基于 DuckDB 表：
    - 行情快照用 `realtime_quote`；
    - 涨跌榜用 `realtime_list`。

- **分笔成交页（仅东财）**
  - 提醒用户：需在交易时间使用。
  - 选择/创建 SyncPlan：
    - `selected_apis=["realtime_tick"]`；
    - `default_execute_params.src="dc"`（或在数据源配置中指定使用东财）。
  - 启动后，前端定期查询或通过分析 API 读取 `realtime_tick`，绘制分时成交明细/成交分布图。

以上即前端在真实模式下接入实时行情（新浪 + 东方财富）的推荐指南，如有额外字段或表结构需求，可在 DuckDB 视图层新增派生列，对前端完全透明。 

