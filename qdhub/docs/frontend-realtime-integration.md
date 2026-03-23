# QDHub 前端实时数据接入开发指南

本文面向前端（如 `qdhub-frontend`），说明如何**同步**实时数据、**订阅**实时行情，以及当前支持的接口与推荐用法。

---

## 一、能力总览与数据流

### 1.1 支持的实时接口

| API 名称 | 说明 | 数据源/模式 | 落库表名 | 前端订阅来源 |
|----------|------|-------------|----------|--------------|
| **realtime_quote** | 个股实时盘口快照 | 新浪/东财，轮询 Pull | `realtime_quote` | DuckDB 查询 或 **WebSocket 快照**（若后端在跑全市场 tick 写入 LatestQuoteStore） |
| **realtime_tick** | 个股当日分笔成交 | 东财 SSE Push | `realtime_tick` | DuckDB 查询 |
| **realtime_list** | 全市场实时涨跌榜 | 新浪/东财，轮询 Pull | `realtime_list` | DuckDB 查询 |
| **rt_min** | 实时分钟 K 线 | 新浪/东财 Pull | `rt_min` | DuckDB 查询 |
| **rt_idx_min** | 指数实时分钟 | 同上 | `rt_idx_min` | DuckDB 查询 |
| **ts_realtime_mkt_tick** | Tushare 全市场 tick（五档盘口） | **内地 ts_proxy 转发**（默认）或直连 Tushare WS（`TUSHARE_REALTIME_SOURCE=direct`） | `ts_realtime_mkt_tick` | **WebSocket 快照**（推荐）或 DuckDB 查询 |

- **数据源**
  - **新浪（`src="sina"`）**：realtime_quote / realtime_list / realtime_tick。
  - **东方财富（`src="dc"` / `eastmoney`）**：realtime_quote（大批量）、realtime_tick（SSE Push）、realtime_list。
  - **Tushare（仅 ts_realtime_mkt_tick）**：默认经 **`TUSHARE_REALTIME_SOURCE=forward`** 连接内地 **ts_proxy**（由内地机订阅 `wss://ws.tushare.pro/listening` 再加密转发）；香港 QDHub 不直连 Tushare WS。可选 **`direct`** 时由本机连 Tushare 官方 WS。数据落库并写入**内存最新价缓存**（LatestQuoteStore），供前端 WebSocket 订阅。

- **环境与主数据源**
  - **环境变量 `QDHUB_ENV`**：`production`（生产）或 `development`（开发，默认）。仅当 `QDHUB_ENV=production`（或开发机显式配置 forward 的 `TUSHARE_PROXY_WS_URL` 等）时，`ts_realtime_mkt_tick` 才走上述 Tushare 全市场 tick 流；否则开发环境可将 `ts_realtime_mkt_tick` **降级为 Sina `realtime_quote`**（避免本机直连 Tushare 触发 IP 限制）。
  - **生产 tick 主路径（推荐）**：`TUSHARE_REALTIME_SOURCE=forward` 且配置 `TUSHARE_PROXY_WS_URL` / `TUSHARE_PROXY_RSA_PUBLIC_KEY_PATH`（或与 `realtime_sources` 中 `tushare_proxy` 合并后齐全）。此时**不会**在缺省情况下再静默回落到香港直连 Tushare WS。标准迁移里 **Sina `realtime_quote`** 对应计划（如 `realtime-sina-quote`）**无交易时间窗**，不会与 `ReconcileRunningWindow` 自动并行；仅在你手动 connect 或灾备切换时才会与 tick 流并存。**仅当前选中的数据源**写入 LatestQuoteStore；前端可通过 WebSocket 推送中的 `current_source`、`sources_health`、`sources_error` 感知（见下）。

### 1.2 前端典型用法

1. **同步侧**：通过 **SyncPlan**（`plan_mode="realtime"`）选择上述实时 API，调用 `POST /api/v1/sync-plans/:id/execute` 启动实时流，数据写入 DuckDB（及对 `ts_realtime_mkt_tick` 同时写内存缓存）。
2. **订阅侧**：
   - **WebSocket**（推荐用于实时看盘）：连接 `GET /api/v1/ws/realtime-quotes`，按 `ts_code` 或全市场订阅，每约 0.5s 收到一次快照。
   - **DuckDB / 分析 API**：对历史或离线看板，查询上述落库表。

---

## 二、接口接入方式（最新）

### 2.1 基础约定

- **Base URL**：`/api/v1`（相对当前域名）。
- **认证**：所有下述接口均在**受保护路由**下，需在请求头携带 JWT：
  - `Authorization: Bearer <access_token>`
- **Content-Type**：JSON 请求使用 `Content-Type: application/json`。

### 2.2 创建实时同步计划（SyncPlan）

**URL**: `POST /api/v1/sync-plans`  
**认证**: Bearer JWT

**请求体示例（实时模式）**：

```json
{
  "name": "实时行情（Tushare 全市场 tick）",
  "description": "Tushare WS 全市场 tick + 前端 WebSocket 订阅",
  "data_source_id": "<tushare 数据源 ID>",
  "data_store_id": "<DuckDB 数据存储 ID>",
  "selected_apis": ["ts_realtime_mkt_tick"],
  "plan_mode": "realtime",
  "pull_interval_seconds": 0,
  "default_execute_params": {}
}
```

**字段说明**：

| 字段 | 必填 | 说明 |
|------|------|------|
| **name** | 是 | 计划名称 |
| **data_source_id** | 是 | 数据源 ID（Tushare 全市场 tick 需选 tushare 数据源） |
| **data_store_id** | 是 | 目标数据存储 ID（DuckDB 路径由该存储决定） |
| **selected_apis** | 是 | 实时 API 列表，见上文「支持的实时接口」 |
| **plan_mode** | 是 | 必须为 **`"realtime"`** 才会走流式工作流 |
| **pull_interval_seconds** | 否 | 对 Pull 类 API（如 realtime_quote）为轮询间隔（秒）；对 **ts_realtime_mkt_tick** 无效（WS 长连接） |
| **default_execute_params** | 否 | 默认执行参数，如 `{"src":"sina"}` 或 `{"src":"dc"}` 用于 quote/tick/list |

**ts_realtime_mkt_tick 可选策略参数**（通过数据源下 API 同步策略配置，一般无需前端传）：

- **topic**：订阅主题，默认 `HQ_STK_TICK`。
- **codes**：代码过滤，默认 `["3*.SZ","0*.SZ","6*.SH"]`，可由后端从 `api_sync_strategies.fixed_params` 读取。

**其他实时 API 示例（新浪盘口 + 东财分笔）**：

```json
{
  "name": "实时行情（新浪 quote + 东财 tick）",
  "data_source_id": "<tushare 数据源 ID>",
  "data_store_id": "<数据存储 ID>",
  "selected_apis": ["realtime_quote", "realtime_tick"],
  "plan_mode": "realtime",
  "pull_interval_seconds": 1,
  "default_execute_params": { "src": "sina" }
}
```

> 说明：`realtime_tick` 使用东财时，后端为 SSE Push 模式，`pull_interval_seconds` 对 tick 无作用。

### 2.3 解析依赖、执行与监控

| 操作 | 方法 | URL | 说明 |
|------|------|-----|------|
| 解析计划依赖 | POST | `/api/v1/sync-plans/:id/resolve` | 创建/修改计划后建议调用一次，解析 API 依赖与参数 |
| 执行计划 | POST | `/api/v1/sync-plans/:id/execute` | 触发一次实时流执行，请求体可为 `{}` 或 `{"start_dt":"","end_dt":""}` |
| 启用/禁用 | POST | `/api/v1/sync-plans/:id/enable`、`/api/v1/sync-plans/:id/disable` | 启用后可按 cron 自动执行（若配置了 cron） |
| 进度查询 | GET | `/api/v1/sync-plans/:id/progress` | 本次执行进度 |
| 进度流（SSE） | GET | `/api/v1/sync-plans/:id/progress-stream` | EventSource 订阅，实时推送任务状态 |

**执行响应示例**：

```json
{
  "data": {
    "execution_id": "<sync_execution_uuid>",
    "status": "triggered"
  }
}
```

前端可用 `execution_id` 配合进度接口或 SSE 展示「同步运行中」状态。

**实时工作流中断时的状态同步**：当实时工作流因**被 ban（多 IP 限制）/超时/断连无法重连**等原因结束时，后端会将该次执行状态置为 `failed`，并将错误信息写入 `error_message`。前端应通过 **GET `/api/v1/sync-plans/:id/progress`** 轮询或 **GET `/api/v1/sync-plans/:id/progress-stream`** SSE 订阅获取最新状态；一旦收到 `status: "failed"`，应展示 `error_message` 给用户（如「实时行情接口被限制，工作流已取消」），并停止展示为运行中。

---

## 三、实时数据订阅：WebSocket 接口

当后端运行了 **ts_realtime_mkt_tick**（或未来扩展的其它写入「最新价缓存」的实时源）时，前端可通过 WebSocket 订阅**内存中的最新行情快照**，用于实时看盘、大屏等，无需轮询 REST。

### 3.1 连接与认证

- **URL**: `GET /api/v1/ws/realtime-quotes`  
  - 实际完整地址示例：`wss://<host>/api/v1/ws/realtime-quotes`（生产用 wss，开发可用 ws）。
- **认证**：与 REST 一致，需携带 JWT。WebSocket 在 HTTP 升级阶段会经过同一套 Gin 中间件，支持两种方式：
  - **请求头**：`Authorization: Bearer <access_token>`（部分 WS 库支持在构造函数里传入 headers）。
  - **查询参数**：浏览器原生 `WebSocket(url)` 无法带自定义头，可使用 `wss://<host>/api/v1/ws/realtime-quotes?token=<access_token>` 传入 token。
- **数据前提**：需先执行包含 **ts_realtime_mkt_tick** 的实时同步计划并处于运行中，后端才会往内存缓存写行情；否则 WS 连接正常但推送的 `items` 为空。

### 3.2 订阅协议（客户端 → 服务端）

连接建立后，客户端可**多次**发送 JSON 消息，更新订阅范围：

```json
{
  "action": "subscribe",
  "ts_codes": ["000001.SZ", "600000.SH"]
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| **action** | string | 固定为 **`"subscribe"`**（大小写不敏感，会 trim） |
| **ts_codes** | string[] | 要订阅的证券代码列表（`ts_code` 格式，如 `000001.SZ`）。**传空数组或不传**表示使用**默认订阅**（见下「收藏与 WS 订阅联动」） |

- 每次发送新的 `subscribe` 会**覆盖**之前的订阅（不是追加）。
- 不发送 `subscribe` 或发送的 `ts_codes` 为空时，服务端**不再**默认推全市场，而是用**当前登录用户的股票收藏列表**作为默认推送列表；若用户未登录或收藏为空，则推全市场。

### 3.3 服务端推送格式（服务端 → 客户端）

服务端按**约 0.5 秒**间隔推送一条 JSON 文本消息（WebSocket `TextMessage`），结构统一为：

```json
{
  "type": "snapshot",
  "scope": "full",
  "timestamp": 1710123456789,
  "items": {
    "000001.SZ": {
      "ts_code": "000001.SZ",
      "code": "000001.SZ",
      "name": "平安银行",
      "trade_time": "2026-03-10 09:30:01",
      "pre_price": 10.01,
      "price": 10.12,
      "open": 10.00,
      "high": 10.20,
      "low": 9.98,
      "close": 10.10,
      "volume": 100000,
      "amount": 1012000,
      "ask_price1": 10.13,
      "ask_volume1": 500,
      "bid_price1": 10.12,
      "bid_volume1": 1200
    }
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| **type** | string | 固定为 `"snapshot"` |
| **scope** | string | `"full"` 表示全市场；`"subset"` 表示仅当前订阅的 `ts_codes` |
| **timestamp** | number | 服务端生成快照的时间戳（毫秒） |
| **ts_codes** | string[] | 仅当 `scope === "subset"` 时存在，当前订阅的代码列表 |
| **items** | object | key 为 `ts_code`，value 为该标的的最新一条行情（字段见下） |
| **current_source** | string | 当前写入缓存的数据源：`tushare_ws` 或 `sina`。开发环境仅 Sina 时为 `sina`。 |
| **sources_health** | object | 各数据源健康程度，如 `{ "tushare_ws": "healthy", "sina": "unavailable" }`。取值：`healthy`、`degraded`、`unavailable`。 |
| **sources_error** | object | 各数据源**最近一次故障原因**，如 `{ "tushare_ws": "在线Ip数量超限, 请联系管理员", "sina": "" }`。健康或从未故障时该 key 为空字符串，便于前端展示具体原因（被 ban、断连、403 等）。 |

**items 中单条行情字段**（与 Tushare HQ_STK_TICK / `ts_realtime_mkt_tick` 对齐）：

- 标识：`code`, `ts_code`, `name`
- 时间：`trade_time`
- 价格与量：`pre_price`, `price`, `open`, `high`, `low`, `close`, `volume`, `amount`, `num`, `open_int`
- 五档：`ask_price1`～`ask_price5`, `ask_volume1`～`ask_volume5`, `bid_price1`～`bid_price5`, `bid_volume1`～`bid_volume5`

（具体以服务端实际返回为准；未推送的字段可能不存在。）

### 3.3.1 收藏列表与 WS 订阅联动

- 连接 `GET /api/v1/ws/realtime-quotes` 时需携带 JWT；服务端在升级请求中解析当前用户。
- **未发送** `subscribe` 或发送的 `ts_codes` 为**空**时：服务端以当前用户的**收藏列表**（`GET /api/v1/watchlist` 对应的数据）作为默认订阅，按 **subset** 模式只推送收藏标的；若用户未登录或收藏为空，则按 **full** 推全市场。
- 客户端显式发送 `{"action":"subscribe","ts_codes":["000001.SZ",...]}` 时，始终按指定 `ts_codes` 推送，覆盖默认。
- 前端可先调用 `GET /api/v1/watchlist` 获取收藏，再将 `ts_codes` 通过 `subscribe` 发送以显式指定订阅；不发送时即使用服务端默认的收藏列表。

### 3.4 分时、盘口与量价字段说明

- **实时分时图**：以 Tushare WS 为主数据；使用 WebSocket 推送的 `items[ts_code]` 中的 `price`、`trade_time`、`volume` 等绘制分时曲线；可选拉取 `GET /api/v1/analysis/realtime-tick?ts_code=&limit=`（从 `ts_realtime_mkt_tick` 查）补充分笔列表。
- **量价**：同一 snapshot 中 `volume`（成交量）、`amount`（成交额）、`price`（最新价），以及分笔接口的逐笔量价。
- **盘口（五档）**：使用 snapshot 中 `ask_price1`～`ask_price5`、`ask_volume1`～`ask_volume5`、`bid_price1`～`bid_price5`、`bid_volume1`～`bid_volume5` 渲染五档买卖盘。

### 3.5 前端使用要点

- 连接后即可收到推送；未发 `subscribe` 或 `ts_codes` 为空时，已登录用户按**收藏列表**推送，否则全市场。
- 需要「只收部分标的」时，发一次 `{"action":"subscribe","ts_codes":["000001.SZ",...]}` 即可。
- 推送频率约 500ms 一次，前端可根据 `timestamp` 做去抖或限频渲染。
- 断线重连后需重新发送 `subscribe`（服务端不持久化订阅状态）。

**当 `items` 为空但连接正常时**：若收到 `type === "snapshot"` 且 `scope === "subset"`、`ts_codes` 非空但 `items` 为空，且 `sources_health[current_source] === "healthy"`，表示数据源连接正常但缓存中尚未收到这些标的的 tick（例如刚连接、非交易时段或上游尚未推送）。前端建议展示「等待行情数据」或「暂无该标的的实时数据」，避免用户误以为连接异常；等后端收到 tick 后，后续 snapshot 的 `items` 会逐步有数据。

---

## 四、DuckDB 表与查询建议

实时流写入的 DuckDB 表可用于分析、历史回放或未接 WebSocket 时的看板。

| 表名 | 说明 | 关键字段 |
|------|------|----------|
| **realtime_quote** | 实时盘口快照 | ts_code, name, open, pre_close, price, high, low, bid, ask, volume, amount, b1_v/b1_p…a5_v/a5_p, date, time, trade_time |
| **realtime_tick** | 分笔成交 | ts_code, time, price, change, volume, amount, type |
| **realtime_list** | 全市场涨跌榜 | ts_code, name, price, pct_change, change, volume, amount, turnover_rate, total_mv, float_mv, … |
| **ts_realtime_mkt_tick** | Tushare 全市场 tick | code, ts_code, name, trade_time, pre_price, price, open, high, low, close, volume, amount, num, ask_price1～5, ask_volume1～5, bid_price1～5, bid_volume1～5 |

前端可通过「分析 API」或自定义 SQL 执行接口查询上述表，按 `ts_code`、`trade_time` 等过滤与排序。

---

## 五、推荐前端流程小结

1. **配置实时计划**
   - 创建 SyncPlan：`plan_mode="realtime"`，`selected_apis` 包含 `ts_realtime_mkt_tick`（或 realtime_quote / realtime_tick / realtime_list）。
   - 调用 `POST /sync-plans/:id/resolve`，再 `POST /sync-plans/:id/execute` 启动同步。

2. **实时看盘（Tushare 全市场 tick）**
   - 连接 `GET /api/v1/ws/realtime-quotes`，带 JWT。
   - 可选：发 `{"action":"subscribe","ts_codes":["000001.SZ",...]}` 收部分标的；不发则收全市场。
   - 每条约 0.5s 的 `snapshot` 消息中取 `items` 渲染行情。

3. **进度与状态**
   - 使用 `GET /sync-plans/:id/progress` 或 `GET /sync-plans/:id/progress-stream`（SSE）展示执行状态。

4. **历史/离线**
   - 通过分析或 SQL 接口查询 DuckDB 表 `realtime_quote`、`realtime_tick`、`realtime_list`、`ts_realtime_mkt_tick`。
   - **历史回放 API**（见下节）：K 线 `GET /api/v1/analysis/kline`（日/周/月）；分时与盘口回放 `GET /api/v1/analysis/intraday-ticks?ts_code=&trade_date=`；可选分钟 K `GET /api/v1/analysis/intraday-kline?ts_code=&trade_date=&period=1m`。按交易日选择器切换 `trade_date` 即可。

---

## 六、历史回放 API 列表

| 接口 | 说明 | 参数 |
|------|------|------|
| `GET /api/v1/analysis/kline` | 日/周/月 K 线 | ts_code, start_date, end_date, adjust_type, period |
| `GET /api/v1/analysis/intraday-ticks` | 按日分时 + 盘口回放（tick 序列，含五档） | ts_code, trade_date |
| `GET /api/v1/analysis/intraday-kline` | 分钟 K 线（若有） | ts_code, trade_date, period=1m |
| `GET /api/v1/analysis/realtime-tick` | 当日实时分笔（最近 N 条） | ts_code, limit |

前端可按交易日选择器切换 `trade_date`，调用上述接口实现历史分时、盘口与 K 线回放。

---

## 七、新闻流请求与展示建议

- **列表接口**：`GET /api/v1/analysis/news`
  - **参数**：`order=time_desc`（默认）或 `time_asc`；`limit`（默认 50）；`offset`；`sources=cls,sina`（逗号分隔来源过滤）；`start_date`、`end_date`（日期范围）；`ts_code`、`category`（可选）。
  - 后端从 `news` 或 `major_news` 表查询，按 `publish_time` 排序，支持来源过滤。
- **SSE 实时流**：`GET /api/v1/analysis/news/stream`（推荐用于财联社式实时滚动）
  - **响应**：`Content-Type: text/event-stream`，`Cache-Control: no-cache`，`Connection: keep-alive`。
  - **查询参数**：`interval_sec`（轮询间隔秒，默认 10）、`limit`（每批条数）、`sources`（来源过滤）、`since`（仅推送该时间之后的新闻，格式与 `publish_time` 一致）。
  - **事件**：服务端按 `interval_sec` 轮询 `ListNews`，以 SSE 事件推送：`event: news`，`data: JSON 数组`；每约 15 秒发送 `: keepalive\n\n` 防代理断连。连接关闭或 `ctx.Done()` 时结束。
- **展示**：按时间倒序、滚动列表；每项包含：id、title、content、source、publish_time、relate_stocks、category/tags（若有），样式可参考财联社电报流（时间戳、标题、来源、标签）。**新闻流**优先使用 `GET /api/v1/analysis/news/stream` 做实时滚动。

以上为当前**接口接入方式**与**实时数据订阅方式**的完整说明；如有新接口或字段变更，以实际后端实现为准。
