# QDHub 前端接入文档

本文档面向前端（如 qdhub-frontend）开发，说明认证方式、收藏列表、股票搜索、实时行情、分析接口与新闻流等接入约定。更详细的实时数据流与 SyncPlan 配置见 [frontend-realtime-integration.md](./frontend-realtime-integration.md)。

---

## 一、通用约定

### 1.1 Base URL 与认证

| 项目 | 说明 |
|------|------|
| **Base URL** | `/api/v1`（相对当前域名，生产需配置完整 host） |
| **认证** | 除登录/注册外，所有接口均在受保护路由下，请求头需携带：`Authorization: Bearer <access_token>` |
| **请求格式** | JSON 请求使用 `Content-Type: application/json` |
| **响应格式** | 统一为 `{ "code": 0, "message": "ok", "data": ... }`，`code !== 0` 表示业务错误 |

### 1.2 登录获取 Token

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/auth/register` | 注册，请求体 `{ "username", "email", "password" }` |
| POST | `/api/v1/auth/login` | 登录，请求体 `{ "username", "password" }`，响应 `data.access_token`、`data.refresh_token` |

前端登录成功后保存 `access_token`，后续请求在 Header 中带上即可。

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
- **认证**：在建立 WebSocket 的 HTTP 请求头中携带 `Authorization: Bearer <access_token>`

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

---

## 五、分析接口（REST）

以下接口均需 `Authorization: Bearer <access_token>`。

### 5.1 实时分笔与历史分时

| 方法 | 路径 | 说明 | 常用参数 |
|------|------|------|----------|
| GET | `/api/v1/analysis/realtime-tick` | 当日实时分笔（最近 N 条） | ts_code, limit（如 500） |
| GET | `/api/v1/analysis/intraday-ticks` | 按日分时 + 盘口回放（tick 序列，含五档） | ts_code, trade_date |

### 5.2 K 线与分钟 K

| 方法 | 路径 | 说明 | 常用参数 |
|------|------|------|----------|
| GET | `/api/v1/analysis/kline` | 日/周/月 K 线 | ts_code, start_date, end_date, adjust_type, period |
| GET | `/api/v1/analysis/intraday-kline` | 分钟 K 线 | ts_code, trade_date, period=1m |

### 5.3 资金流

| 方法 | 路径 | 说明 | 常用参数 |
|------|------|------|----------|
| GET | `/api/v1/analysis/moneyflow` | 同花顺个股资金流向 | ts_code, trade_date |
| GET | `/api/v1/analysis/moneyflow-concept` | 同花顺概念板块资金流入 | trade_date, concept（可选） |

### 5.4 新闻列表

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

## 六、新闻流（SSE 实时推送）

用于「财联社式」实时新闻流：服务端按间隔轮询并推送最新新闻，前端用 EventSource 消费。

### 6.1 接口

**GET** `/api/v1/analysis/news/stream`

- **认证**：需在请求头携带 `Authorization: Bearer <access_token>`（EventSource 不支持自定义 Header 时，需通过支持 Header 的封装或 `?token=` 扩展，当前以 Header 为准）。
- **响应**：`Content-Type: text/event-stream`，`Cache-Control: no-cache`，`Connection: keep-alive`。

### 6.2 查询参数

| 参数 | 说明 | 默认 |
|------|------|------|
| interval_sec | 轮询间隔（秒） | 10 |
| limit | 每批条数 | 同 ListNews 默认 |
| sources | 来源过滤 | 无 |
| since | 仅推送该时间之后的新闻 | 无 |

### 6.3 事件格式

- 数据事件：`event: news`，`data: <JSON 数组>`，数组元素与 `GET /api/v1/analysis/news` 单条结构一致。
- 保活：约每 15 秒发送 `: keepalive\n\n`，防止代理断连。
- 连接关闭或服务端结束即停止推送。

### 6.4 前端用法建议

- 使用 `EventSource` 或支持自定义 Header 的 SSE 库连接 `GET /api/v1/analysis/news/stream`。
- 监听 `event: news`，解析 `data` 为 JSON 数组，追加到列表顶部或按时间合并去重。
- 展示样式可参考财联社电报流：时间戳、标题、来源、标签。

---

## 七、历史回放与看盘流程小结

### 7.1 历史回放

- **K 线**：`GET /api/v1/analysis/kline`（日/周/月）+ 可选 `GET /api/v1/analysis/intraday-kline`（分钟）。
- **分时 + 盘口**：`GET /api/v1/analysis/intraday-ticks?ts_code=&trade_date=`，按 `trade_time` 排序绘制分时曲线，五档取自 tick 内字段。
- 前端通过交易日选择器切换 `trade_date` 即可切换回放日。

### 7.2 推荐看盘流程

1. **登录**：`POST /api/v1/auth/login`，保存 `access_token`。
2. **自选**：`GET /api/v1/watchlist` 展示收藏；增删用 POST/DELETE `/api/v1/watchlist`。
3. **实时行情**：连接 `GET /api/v1/ws/realtime-quotes` 并带 Token；不发 `subscribe` 或 `ts_codes: []` 则按收藏列表推送；用 `items[ts_code]` 渲染分时、五档、量价。
4. **分笔**：可选 `GET /api/v1/analysis/realtime-tick?ts_code=&limit=` 补充分笔列表。
5. **历史**：切换交易日时请求 kline、intraday-ticks（及可选 intraday-kline）。
6. **新闻**：列表用 `GET /api/v1/analysis/news`；实时流用 `GET /api/v1/analysis/news/stream`（SSE）。

---

## 八、错误与状态码

| HTTP 状态码 | 说明 |
|-------------|------|
| 200 | 成功 |
| 201 | 创建成功（如 POST watchlist） |
| 400 | 参数错误，看 body 中 message |
| 401 | 未认证或 Token 无效，需重新登录 |
| 403 | 无权限（RBAC），当前角色不可访问该资源 |
| 404 | 资源不存在 |
| 500 | 服务端错误，看 body 或日志 |

业务错误时 `code !== 0`，前端可根据 `code` 与 `message` 做提示。

---

## 九、相关文档

- [frontend-realtime-integration.md](./frontend-realtime-integration.md)：实时数据流、SyncPlan 配置、WebSocket 与 DuckDB 表说明、历史回放 API 细节。
