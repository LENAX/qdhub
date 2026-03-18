# QDHub 前端接入文档

本文档面向前端（如 qdhub-frontend）开发，说明认证方式、收藏列表、股票搜索、**实时数据源管理**、实时行情 WebSocket、分析接口与新闻流等接入约定。更详细的实时数据流与 SyncPlan 配置见 [frontend-realtime-integration.md](./frontend-realtime-integration.md)。

---

## 最近更新（与接口同步）

| 更新内容 | 说明 |
|----------|------|
| **实时数据源管理 API** | 新增 `/api/v1/realtime-sources` 完整 CRUD、健康快照、心跳 SSE、单源健康检查、**连接/断开**（connect/disconnect），用于配置与切换实时数据源（如 Tushare 直连、ts_proxy 转发等）。详见下文「实时数据源管理」。 |
| **WebSocket 快照字段** | `/api/v1/ws/realtime-quotes` 推送的每条 snapshot 增加 `current_source`、`sources_health`、`sources_error`，便于前端展示当前数据源与多源故障原因。 |
| **Connect/Disconnect** | 管理员可通过 `POST .../realtime-sources/:id/connect` 与 `POST .../realtime-sources/:id/disconnect` 控制某数据源是否向内存缓存写行情；连接后前端 WS 即可收到该源数据。 |

---

## 一、通用约定

### 1.1 Base URL 与认证

| 项目 | 说明 |
|------|------|
| **Base URL** | `/api/v1`（相对当前域名，生产需配置完整 host） |
| **认证** | 除登录/注册外，所有接口均在受保护路由下，请求头需携带：`Authorization: Bearer <access_token>` |
| **请求格式** | JSON 请求使用 `Content-Type: application/json` |
| **响应格式** | 统一为 `{ "code": 0, "message": "ok", "data": ... }`，`code !== 0` 表示业务错误 |

### 1.2 认证接口

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/auth/register` | 注册，请求体 `{ "username", "email", "password" }` |
| POST | `/api/v1/auth/login` | 登录，请求体 `{ "username", "password" }`，响应 `data.access_token`、`data.refresh_token` |
| POST | `/api/v1/auth/refresh` | 刷新 Token，请求体 `{ "refresh_token": "..." }`，响应新的 `access_token`（需 Bearer） |
| GET | `/api/v1/auth/me` | 获取当前用户信息（需 Bearer） |
| PUT | `/api/v1/auth/password` | 修改密码（需 Bearer），请求体含旧密码与新密码 |

前端登录成功后保存 `access_token`（及可选 `refresh_token`），后续请求在 Header 中带上 `Authorization: Bearer <access_token>` 即可。

---

## 二、股票收藏列表（Watchlist）

用于「自选股」：当前用户的收藏列表，并与实时行情 WebSocket 默认订阅联动（见第四节）。

### 2.1 接口列表

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/watchlist` | 获取当前用户收藏列表（含 ts_code、name 等） |
| POST | `/api/v1/watchlist` | 添加收藏，请求体 `{ "ts_code": "000001.SZ" }` |
| DELETE | `/api/v1/watchlist/:ts_code` | 取消收藏，如 `/api/v1/watchlist/000001.SZ` |

### 2.2 GET /api/v1/watchlist 响应

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "items": [
      {
        "ts_code": "000001.SZ",
        "name": "平安银行",
        "sort_order": 0,
        "created_at": "2026-03-12T10:00:00+08:00"
      }
    ]
  }
}
```

- `data.items` 为数组，按 `sort_order` 升序；`name` 由后端联表 `stock_basic` 填充，无数据时可为空字符串。
- 未收藏时 `items` 为空数组 `[]`。

### 2.3 前端用法建议

- 自选页：进入页面前请求 `GET /api/v1/watchlist`，渲染列表；提供「添加/删除」按钮，调用 POST/DELETE 后刷新列表或本地增删。
- 与实时行情联动：连接 WebSocket 后**不发送** `subscribe` 或发送 `ts_codes: []` 时，服务端会按**当前用户收藏列表**推送行情，无需前端再传一次列表（也可先 GET watchlist 再发 `subscribe` 显式指定）。

---

## 三、股票搜索（含拼音/缩写）

用于搜索框、联想列表：支持名称、代码、拼音缩写（cnspell）查询。

### 3.1 接口

**GET** `/api/v1/analysis/stocks`

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| query | string | 否 | 关键词，对 name/ts_code/symbol/cnspell 模糊匹配 |
| search_type | string | 否 | 设为 `cnspell` 时**仅按拼音缩写**匹配 query，适合联想 |
| limit | int | 否 | 条数，默认 100 |
| offset | int | 否 | 偏移，默认 0 |
| market | string | 否 | 市场过滤 |
| list_status | string | 否 | 上市状态等 |

### 3.2 响应

```json
{
  "code": 0,
  "data": [
    {
      "ts_code": "000001.SZ",
      "symbol": "000001",
      "name": "平安银行",
      "area": "深圳",
      "industry": "银行",
      "market": "主板",
      "list_date": "19910403",
      "is_hs": "S"
    }
  ]
}
```

### 3.3 前端用法建议

- 联想输入：用户输入拼音首字母（如 `PA`）时，请求 `GET /api/v1/analysis/stocks?query=PA&search_type=cnspell&limit=20`，下拉展示匹配结果。
- 综合搜索：不传 `search_type` 时按名称、代码、拼音等多字段匹配，适合「搜索框」综合搜索。

---

## 四、实时行情订阅（WebSocket）

用于实时看盘：连接后按约 0.5 秒收到一次全量/订阅标的快照（含五档、最新价、成交量等）。

### 4.1 连接

- **URL**：`GET /api/v1/ws/realtime-quotes`（实际为 `ws(s)://<host>/api/v1/ws/realtime-quotes`）
- **认证**：需携带 JWT，两种方式二选一：
  - **请求头**：建立 WebSocket 时在 HTTP 请求头中设置 `Authorization: Bearer <access_token>`（多数 WS 库支持）。
  - **查询参数**：浏览器原生 `WebSocket(url)` 无法带自定义头时，可使用 `ws(s)://<host>/api/v1/ws/realtime-quotes?token=<access_token>` 传 token。

### 4.2 订阅（客户端 → 服务端）

连接建立后发送 JSON 文本消息：

```json
{
  "action": "subscribe",
  "ts_codes": ["000001.SZ", "600519.SH"]
}
```

- `ts_codes` 为空数组或不发送 `subscribe`：服务端使用**当前用户收藏列表**作为默认订阅；未登录或收藏为空时推全市场。
- 每次发送新的 `subscribe` 会**覆盖**此前订阅，不是追加。

### 4.3 推送格式（服务端 → 客户端）

服务端按约 0.5 秒推送一条 JSON，例如：

```json
{
  "type": "snapshot",
  "scope": "subset",
  "timestamp": 1710123456789,
  "ts_codes": ["000001.SZ", "600519.SH"],
  "items": {
    "000001.SZ": {
      "ts_code": "000001.SZ",
      "name": "平安银行",
      "trade_time": "2026-03-12 09:30:01",
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

- **分时/量价**：使用 `price`、`trade_time`、`volume`、`amount` 等绘制分时与量价。
- **五档盘口**：使用 `ask_price1`～`ask_price5`、`ask_volume1`～`ask_volume5`、`bid_price1`～`bid_price5`、`bid_volume1`～`bid_volume5` 渲染买卖五档。
- **数据源感知**：每条 snapshot 还包含 `current_source`（当前写入缓存的源，如 `tushare_forward`、`tushare_ws`、`sina`）、`sources_health`（各源健康状态）、`sources_error`（各源最近错误信息），可用于展示「当前数据源」或故障提示。

---

## 五、实时数据源管理（Realtime Sources）

用于配置与切换实时行情数据源（Tushare 直连、ts_proxy 转发、新浪、东财等），以及查看健康状态、手动连接/断开某源。**需 RBAC 权限**（如 `realtime-sources` 资源），通常仅管理员可写。

### 5.1 接口列表

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/realtime-sources` | 列表（全部数据源） |
| POST | `/api/v1/realtime-sources` | 创建，请求体见下 |
| GET | `/api/v1/realtime-sources/:id` | 单条详情 |
| PUT | `/api/v1/realtime-sources/:id` | 更新 |
| DELETE | `/api/v1/realtime-sources/:id` | 删除 |
| GET | `/api/v1/realtime-sources/health` | 健康快照（一次 JSON：所有源 + current_source + sources_health + sources_error） |
| GET | `/api/v1/realtime-sources/heartbeat` | 心跳（SSE），每约 5 秒推送一次同上快照 |
| GET | `/api/v1/realtime-sources/:id/health` | 触发单源健康检查并返回结果 |
| POST | `/api/v1/realtime-sources/:id/connect` | 连接该数据源（开始向内存缓存写行情，供 WS 推送） |
| POST | `/api/v1/realtime-sources/:id/disconnect` | 断开该数据源 |

### 5.2 数据源类型与 Config

| type | 说明 | config（JSON 字符串）常见字段 |
|------|------|------------------------------|
| `tushare_forward` | ts_proxy 转发 | `ws_url`、`rsa_public_key_path` |
| `tushare_ws` | Tushare 直连 WebSocket | `endpoint`、`token` |
| `sina` / `eastmoney` | 新浪/东财（Pull 类） | 视后端实现，可为 `{}` |

前端创建/更新时，`config` 为**字符串**，内容为 JSON，例如：`"{\"ws_url\":\"ws://host/realtime\",\"rsa_public_key_path\":\"/path/to/pub.pem\"}"`。

### 5.3 创建 POST /api/v1/realtime-sources

**请求体**：

```json
{
  "name": "Tushare 转发",
  "type": "tushare_forward",
  "config": "{\"ws_url\":\"ws://proxy:8888/realtime\",\"rsa_public_key_path\":\"/opt/keys/pub.pem\"}",
  "priority": 1,
  "is_primary": false,
  "health_check_on_startup": true,
  "enabled": true
}
```

- `name`、`type` 必填；`config` 为 JSON 字符串，类型不同字段不同；`priority` 数字越小越优先；`enabled` 为 false 时不会参与健康检查与连接。

**响应**：`201 Created`，`data` 为完整数据源对象（含 `id`、`created_at`、`updated_at` 等）。

### 5.4 列表与详情响应结构

**GET /api/v1/realtime-sources** 响应示例：

```json
{
  "code": 0,
  "message": "ok",
  "data": [
    {
      "id": "aaaaaaaa-0004-4000-8000-000000000001",
      "name": "Tushare 转发",
      "type": "tushare_forward",
      "config": "{\"ws_url\":\"...\",\"rsa_public_key_path\":\"...\"}",
      "priority": 1,
      "is_primary": false,
      "health_check_on_startup": true,
      "enabled": true,
      "last_health_status": "healthy",
      "last_health_at": "2026-03-18T10:00:00+08:00",
      "last_health_error": "",
      "created_at": "2026-03-01T00:00:00Z",
      "updated_at": "2026-03-18T10:00:00Z"
    }
  ]
}
```

单条 **GET /api/v1/realtime-sources/:id** 的 `data` 为上述单元素结构。

### 5.5 健康快照与心跳 SSE

**GET /api/v1/realtime-sources/health** 一次返回：

```json
{
  "code": 0,
  "data": {
    "sources": [
      {
        "id": "...",
        "name": "Tushare 转发",
        "type": "tushare_forward",
        "last_health_status": "healthy",
        "last_health_at": "2026-03-18T10:00:00+08:00",
        "last_health_error": ""
      }
    ],
    "current_source": "tushare_forward",
    "sources_health": { "tushare_forward": "healthy", "sina": "healthy", "tushare_ws": "healthy", "eastmoney": "healthy" },
    "sources_error": { "tushare_forward": "", "sina": "", "tushare_ws": "", "eastmoney": "" }
  }
}
```

**GET /api/v1/realtime-sources/heartbeat** 为 SSE，`Content-Type: text/event-stream`，每约 5 秒发送一次 `event: message`，`data` 为上述 `data` 对象的 JSON 字符串。前端可用 `EventSource` 或带 Header 的 SSE 客户端订阅（需带 `Authorization: Bearer <access_token>`）。

### 5.6 单源健康检查与连接/断开

- **GET /api/v1/realtime-sources/:id/health**：触发该源一次健康检查，响应 `data.status`（如 `healthy`）、`data.error`（错误信息）。
- **POST /api/v1/realtime-sources/:id/connect**：请求连接该源（若服务端配置了 connector，会启动对应 collector 写内存缓存）。成功响应 `200`，`data.message`: `"connect request accepted"`；若未配置 connector 则同样 200，但 `data.note`: `"connector not configured"`。
- **POST /api/v1/realtime-sources/:id/disconnect**：请求断开该源。成功响应 `200`，`data.message`: `"disconnect request accepted"`。

### 5.7 前端用法建议

- **设置页**：列表用 `GET /api/v1/realtime-sources`；大屏/状态栏用 `GET /api/v1/realtime-sources/health` 或 SSE `heartbeat` 展示当前源与各源健康/错误。
- **连接控制**：对某条源提供「连接」「断开」按钮，调用 `POST .../connect`、`POST .../disconnect`；连接成功后，实时行情 WebSocket 会开始收到该源写入的行情（见第四节）。
- **创建/编辑**：根据 `type` 渲染不同 config 表单（如 `tushare_forward` 填 `ws_url`、`rsa_public_key_path`），提交时把 config 对象 `JSON.stringify` 后作为字符串传给 `config`。
- **本地开发 + 内地 ts_proxy**：若内地已部署 ts_proxy，本地开发环境可配置 `tushare_forward` 源指向该 ts_proxy（ws_url + 公钥路径），由内地连 Tushare、本地只连 ts_proxy，**不会触发 Tushare IP 限制**。详见 [design-tushare-realtime-tunnel.md](./design-tushare-realtime-tunnel.md) 第十节。

---

## 六、分析接口（REST）

以下接口均需 `Authorization: Bearer <access_token>`。

### 6.1 实时分笔与历史分时

| 方法 | 路径 | 说明 | 常用参数 |
|------|------|------|----------|
| GET | `/api/v1/analysis/realtime-tick` | 当日实时分笔（最近 N 条） | ts_code, limit（如 500） |
| GET | `/api/v1/analysis/intraday-ticks` | 按日分时 + 盘口回放（tick 序列，含五档） | ts_code, trade_date |

### 6.2 K 线与分钟 K

| 方法 | 路径 | 说明 | 常用参数 |
|------|------|------|----------|
| GET | `/api/v1/analysis/kline` | 日/周/月 K 线 | ts_code, start_date, end_date, adjust_type, period |
| GET | `/api/v1/analysis/intraday-kline` | 分钟 K 线 | ts_code, trade_date, period=1m |

### 6.3 资金流

| 方法 | 路径 | 说明 | 常用参数 |
|------|------|------|----------|
| GET | `/api/v1/analysis/moneyflow` | 同花顺个股资金流向 | ts_code, trade_date |
| GET | `/api/v1/analysis/moneyflow-concept` | 同花顺概念板块资金流入 | trade_date, concept（可选） |

### 6.4 新闻列表

**GET** `/api/v1/analysis/news`

| 参数 | 说明 |
|------|------|
| order | time_desc（默认）/ time_asc |
| limit, offset | 分页，默认 limit=50 |
| sources | 来源过滤，逗号分隔，如 cls,sina |
| start_date, end_date | 日期范围 |
| ts_code, category | 可选 |

响应 `data` 为新闻列表，每项含 id、title、content、source、publish_time、relate_stocks、category/tags 等，按时间倒序展示即可。

---

## 七、新闻流（SSE 实时推送）

用于「财联社式」实时新闻流：服务端按间隔轮询并推送最新新闻，前端用 EventSource 消费。

### 7.1 接口

**GET** `/api/v1/analysis/news/stream`

- **认证**：需在请求头携带 `Authorization: Bearer <access_token>`（EventSource 不支持自定义 Header 时，需通过支持 Header 的封装或 `?token=` 扩展，当前以 Header 为准）。
- **响应**：`Content-Type: text/event-stream`，`Cache-Control: no-cache`，`Connection: keep-alive`。

### 7.2 查询参数

| 参数 | 说明 | 默认 |
|------|------|------|
| interval_sec | 轮询间隔（秒） | 10 |
| limit | 每批条数 | 同 ListNews 默认 |
| sources | 来源过滤 | 无 |
| since | 仅推送该时间之后的新闻 | 无 |

### 7.3 事件格式

- 数据事件：`event: news`，`data: <JSON 数组>`，数组元素与 `GET /api/v1/analysis/news` 单条结构一致。
- 保活：约每 15 秒发送 `: keepalive\n\n`，防止代理断连。
- 连接关闭或服务端结束即停止推送。

### 7.4 前端用法建议

- 使用 `EventSource` 或支持自定义 Header 的 SSE 库连接 `GET /api/v1/analysis/news/stream`。
- 监听 `event: news`，解析 `data` 为 JSON 数组，追加到列表顶部或按时间合并去重。
- 展示样式可参考财联社电报流：时间戳、标题、来源、标签。

---

## 八、历史回放与看盘流程小结

### 8.1 历史回放

- **K 线**：`GET /api/v1/analysis/kline`（日/周/月）+ 可选 `GET /api/v1/analysis/intraday-kline`（分钟）。
- **分时 + 盘口**：`GET /api/v1/analysis/intraday-ticks?ts_code=&trade_date=`，按 `trade_time` 排序绘制分时曲线，五档取自 tick 内字段。
- 前端通过交易日选择器切换 `trade_date` 即可切换回放日。

### 8.2 推荐看盘流程

1. **登录**：`POST /api/v1/auth/login`，保存 `access_token`（可选 `refresh_token` 用于 `POST /api/v1/auth/refresh`）。
2. **自选**：`GET /api/v1/watchlist` 展示收藏；增删用 POST/DELETE `/api/v1/watchlist`。
3. **实时数据源**（可选，管理员）：`GET /api/v1/realtime-sources` 列表；需接入行情时对某源调用 `POST .../realtime-sources/:id/connect`；健康状态可用 `GET .../realtime-sources/health` 或 SSE `.../heartbeat`。
4. **实时行情**：连接 `GET /api/v1/ws/realtime-quotes` 并带 Token（Header 或 `?token=`）；不发 `subscribe` 或 `ts_codes: []` 则按收藏列表推送；用 `items[ts_code]` 渲染分时、五档、量价；可用 `current_source`、`sources_health`、`sources_error` 展示数据源与故障提示。
5. **分笔**：可选 `GET /api/v1/analysis/realtime-tick?ts_code=&limit=` 补充分笔列表。
6. **历史**：切换交易日时请求 kline、intraday-ticks（及可选 intraday-kline）。
7. **新闻**：列表用 `GET /api/v1/analysis/news`；实时流用 `GET /api/v1/analysis/news/stream`（SSE）。

---

## 九、错误与状态码

| HTTP 状态码 | 说明 |
|-------------|------|
| 200 | 成功 |
| 201 | 创建成功（如 POST watchlist、POST realtime-sources） |
| 204 | 删除成功无 body（如 DELETE realtime-sources/:id） |
| 400 | 参数错误，看 body 中 message |
| 401 | 未认证或 Token 无效，需重新登录 |
| 403 | 无权限（RBAC），当前角色不可访问该资源 |
| 404 | 资源不存在 |
| 500 | 服务端错误，看 body 或日志 |

业务错误时 `code !== 0`，前端可根据 `code` 与 `message` 做提示。

---

## 十、相关文档

- [frontend-realtime-integration.md](./frontend-realtime-integration.md)：实时数据流、SyncPlan 配置、WebSocket 与 DuckDB 表说明、历史回放 API 细节。
