## QDHub 实时数据源管理前端接入文档

本文只聚焦「实时数据源管理」（`/api/v1/realtime-sources` 相关能力），面向前端或运维控制台，说明如何：

- **查看与编辑** 实时行情数据源（Tushare 直连、内地 ts_proxy 转发、新浪、东财等）
- **启动时健康检查** 的展示与排查
- 通过 **Connect / Disconnect** 控制哪些源真正向内存行情缓存写数据，从而影响前端 WebSocket 行情

更完整的整体前端接入文档请参考 `frontend-integration-guide.md`，实时流与工作流设计见 `design-tushare-realtime-tunnel.md` 与 `design-realtime-multi-source-failover.md`。

---

## 一、核心概念总览

- **RealtimeSource**：一条实时数据源配置记录，对应一类实时行情来源，例如：
  - `tushare_proxy`：通过 **内地 ts_proxy 转发服务** 接入 Tushare 实时行情（方案 B：RSA 交换 AES + AES 加密）
  - `tushare_ws`：QDHUB 直接连 `wss://ws.tushare.pro/listening`
  - `sina`：新浪实时行情（轮询 Pull）
  - `eastmoney`：东方财富实时行情（轮询 Pull + SSE Push）
- **字段含义（简化版）**：
  - `name`：人类可读名称（如「Tushare 转发」）
  - `type`：上面提到的类型字符串
  - `config`：**字符串形式的 JSON**，保存各类型特有配置（例如 `ws_url`、`rsa_public_key_path`、`endpoint`、`token`）
  - `priority`：优先级，小的优先
  - `is_primary`：是否主用
  - `health_check_on_startup`：是否在应用启动时做连通性自检
  - `enabled`：是否启用该源（未启用的不会参与自检与选择）
  - `last_health_status` / `last_health_error`：最近一次健康检查结果
- **Connect / Disconnect 与前端行情的关系**：
  - 后端通过 Workflow + Collector（如 ForwardTickCollector）从实时源拉/收行情，并写入 **LatestQuoteStore**。
  - 前端的 WebSocket `/api/v1/ws/realtime-quotes` **只读 LatestQuoteStore**。
  - **Connect 现已具备「触发实时工作流」的语义**：调用 `POST /realtime-sources/:id/connect` 等价于对该源执行 **StartRealtimeSync**，即启动该源对应的实时 SyncPlan（若尚未运行），并切换 Selector 为当前源，从而开始向 Store 写数据。
  - **Disconnect** 等价于 **StopRealtimeSync**：终止该源对应的实时工作流执行，不再写入 Store 与 data store。
  - **主源**（如 ts_proxy）在交易时间窗（9:30–11:30、13:00–15:00）内会由调度层自动启动/停止；**灾备源**（如 sina、tushare_ws）不自动调度，仅在人工 connect 或故障切换时启动。

前端不需要关心 Collector 细节，只需要知道：**某源 Connect 成功 + 健康状态为 healthy，且被 Selector 选为 current_source 时，WebSocket 行情就应该能从该源收到数据**。

---

## 二、API 一览

所有接口都在 `/api/v1/realtime-sources` 下面，均需要 JWT + RBAC（通常只给管理员角色）。

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/realtime-sources` | 列出所有实时数据源（按 priority 排序） |
| POST | `/api/v1/realtime-sources` | 创建新的实时数据源 |
| GET | `/api/v1/realtime-sources/:id` | 获取单个数据源详情 |
| PUT | `/api/v1/realtime-sources/:id` | 更新数据源信息 |
| DELETE | `/api/v1/realtime-sources/:id` | 删除数据源 |
| GET | `/api/v1/realtime-sources/health` | **健康快照**：一次性返回所有源 + 当前源信息 |
| GET | `/api/v1/realtime-sources/heartbeat` | **健康心跳（SSE）**：周期性推送健康快照 |
| GET | `/api/v1/realtime-sources/:id/health` | 对某个源执行单次健康检查 |
| POST | `/api/v1/realtime-sources/:id/connect` | 请求「连接」该源：**启动该源对应的实时 SyncPlan**（StartRealtimeSync），开始写 Store 并参与 Selector |
| POST | `/api/v1/realtime-sources/:id/disconnect` | 请求「断开」该源：**停止该源对应的实时工作流**（StopRealtimeSync），不再写 Store |

统一响应格式为：

```json
{ "code": 0, "message": "ok", "data": ... }
```

业务错误时 `code != 0`，HTTP 状态码通常仍为 200，前端应根据 `code` / `message` 做提示。

### 交易时间窗与自动调度

- **主源**（如 ts_proxy，`is_primary=true`）绑定的实时 SyncPlan 配置了交易时间窗（早盘 9:30–11:30、午盘 13:00–15:00）。调度层每分钟执行 **ReconcileRunningWindow**：在时间窗内自动启动该计划，在时间窗外自动停止。
- **对前端的影响**：在交易时段内，主源可能**无需人工 connect** 即已自动开始推送；非交易时段内主源会自动停止。灾备源（sina、tushare_ws）不会自动启停，仅在前端 connect 或故障切换时启动。
- 若在非交易时段调用 connect，后端仍会记录并执行 StartRealtimeSync（启动对应计划）；是否拒绝或提示「待开盘自动启动」可由实现决定。

---

## 三、数据结构细节

### 3.1 列表与详情（GET /realtime-sources）

**列表响应示例：**

```json
{
  "code": 0,
  "data": [
    {
      "id": "aaaaaaaa-0001-4000-8000-000000000001",
      "name": "Tushare 转发",
      "type": "tushare_proxy",
      "config": "{\"ws_url\":\"ws://47.107.235.54:8888/realtime\",\"rsa_public_key_path\":\"/Users/me/.key/public.pem\"}",
      "priority": 1,
      "is_primary": true,
      "health_check_on_startup": true,
      "enabled": true,
      "last_health_status": "healthy",
      "last_health_at": "2026-03-18T09:00:00+08:00",
      "last_health_error": "",
      "created_at": "2026-03-01T00:00:00Z",
      "updated_at": "2026-03-18T09:00:00Z"
    }
  ]
}
```

注意：

- `config` 是 **字符串**，内层是 JSON，需要前端再做一次 `JSON.parse` 才能用来渲染表单。
- `last_health_status` 取值：
  - `"healthy"`：最近一次连通性检查成功
  - `"unhealthy"`：逻辑错误（例如缺配置、证书错误、解密失败）
  - `"unavailable"`：网络不可达、远端返回非 200 等

### 3.2 创建/更新请求体

**创建（POST /realtime-sources）请求体：**

```json
{
  "name": "Tushare 转发（内地 ts_proxy）",
  "type": "tushare_proxy",
  "config": "{\"ws_url\":\"ws://47.107.235.54:8888/realtime\",\"rsa_public_key_path\":\"/Users/me/.key/public.pem\"}",
  "priority": 1,
  "is_primary": true,
  "health_check_on_startup": true,
  "enabled": true
}
```

**更新（PUT /realtime-sources/:id）请求体（部分字段可选）：**

```json
{
  "name": "Tushare 转发（内地 ts_proxy）",
  "config": "{\"ws_url\":\"ws://47.107.235.54:8888/realtime\",\"rsa_public_key_path\":\"/Users/me/.key/public.pem\"}",
  "priority": 1,
  "is_primary": true,
  "health_check_on_startup": true,
  "enabled": true
}
```

前端表单层建议处理为「外层字段 + 内层 config JSON」：

- 表单模型例子：
  - `name: string`
  - `type: 'tushare_proxy' | 'tushare_ws' | 'sina' | 'eastmoney'`
  - `priority: number`
  - `is_primary: boolean`
  - `health_check_on_startup: boolean`
  - `enabled: boolean`
  - `configForm: { ws_url?: string; rsa_public_key_path?: string; endpoint?: string; token?: string }`
- 提交时：`config = JSON.stringify(configForm)`，再拼到外层 JSON 里发给后端。
- 编辑时：从响应里取 `config` 字符串，用 `JSON.parse` 恢复成 `configForm`。

### 3.3 不同 type 下的 config 字段

| type | 典型用途 | config 内字段 |
|------|----------|---------------|
| `tushare_proxy` | 通过内地 ts_proxy 转发 Tushare 行情 | `ws_url`: 转发 WS 地址，如 `ws://47.107.235.54:8888/realtime`；`rsa_public_key_path`: RSA 公钥路径（仅存在于 QDHub 所在机器上） |
| `tushare_ws` | 直连 Tushare WebSocket | `endpoint`: 默认 `wss://ws.tushare.pro/listening`；`token`: Tushare Token（可为空，使用数据源 token） |
| `sina` | 新浪行情 Pull | 一般 `{}`，无需额外字段 |
| `eastmoney` | 东财行情 / 分笔 | 一般 `{}`，无需额外字段 |

具体字段可能随实现扩展，以后如有新增，会在 `frontend-integration-guide.md` 中补充。

---

## 四、健康检查与启动自检

### 4.1 启动自检（Startup Health Check）

后端在应用启动时会自动对所有「启用且勾选了 `health_check_on_startup`」的数据源做一次健康检查，行为包括：

- 调用 `ListEnabledForHealthCheck()`，筛选 `enabled=1 AND health_check_on_startup=1` 的源；
- 对每条源调用类型特定的检查逻辑：
  - `tushare_proxy`：
    - 若顶层配置（`config.yaml` 或环境变量）中同时设置了：
      - `tushare.proxy_ws_url` / `TUSHARE_PROXY_WS_URL`（兼容旧键 `tushare.forward_ws_url`）
      - `tushare.proxy_rsa_public_key_path` / `TUSHARE_PROXY_RSA_PUBLIC_KEY_PATH`（兼容旧键 `tushare.forward_rsa_public_key_path`）
    - 则**优先使用应用配置**中的 `ws_url` 与 `rsa_public_key_path` 连内地 ts_proxy 做一次握手 + 接收首帧校验；
    - 否则退回到使用该源 `config` 中的 `ws_url` / `rsa_public_key_path` 做检查。
  - `sina`：访问一个固定的新浪行情 URL，看 HTTP 是否 200。
  - `eastmoney`：访问一个固定的东财 API，看 HTTP 是否 200。
  - `tushare_ws`：简单尝试连 `endpoint` 是否能 `Dial` 成功。
- 检查结束后会更新 `last_health_status`、`last_health_error` 等字段。

你在日志中可以看到类似：

- `Realtime sources: running startup connectivity check (health_check_on_startup=true)...`
- `Realtime sources: checking Tushare 转发 (tushare_proxy)...`
- `Realtime source Tushare 转发 (tushare_proxy) health: healthy`

### 4.2 手动健康检查

**GET `/api/v1/realtime-sources/:id/health`**

- 行为：对单个源执行一次健康检查，并立即返回结果，不会触发全局自检。
- 响应示例：

```json
{
  "code": 0,
  "data": {
    "status": "healthy",
    "error": ""
  }
}
```

前端可在「操作」列加一个「检查连通性」按钮，点击时调用该接口并弹出结果。

### 4.3 健康快照与心跳（适合状态页、大屏）

**GET `/api/v1/realtime-sources/health`**

- 一次返回所有源的简要信息 + 当前 Selector 状态：

```json
{
  "code": 0,
  "data": {
    "sources": [
      {
        "id": "aaaa...",
        "name": "Tushare 转发",
        "type": "tushare_proxy",
        "last_health_status": "healthy",
        "last_health_at": "2026-03-18T10:00:00+08:00",
        "last_health_error": ""
      }
    ],
    "current_source": "tushare_proxy",
    "sources_health": {
      "tushare_proxy": "healthy",
      "tushare_ws": "unavailable",
      "sina": "healthy",
      "eastmoney": "healthy"
    },
    "sources_error": {
      "tushare_proxy": "",
      "tushare_ws": "在线Ip数量超限, 请联系管理员",
      "sina": "",
      "eastmoney": ""
    }
  }
}
```

- `current_source`：当前写入 LatestQuoteStore 的主数据源（前端 WebSocket 也会返回这个字段）。
- `sources_health` / `sources_error`：用于绘制多源状态灯、Tips 等。

**GET `/api/v1/realtime-sources/heartbeat`**

- SSE 接口：`Content-Type: text/event-stream`。
- 每隔约 5 秒发送一条 `event: message`，`data:` 后面是与 `/health` 中 `data` 字段相同的 JSON 字符串。
- 适合在监控大屏或状态页用 `EventSource` 订阅。

---

## 五、Connect / Disconnect 行为与前端用法

### 5.1 Connect / Disconnect 语义

**POST `/api/v1/realtime-sources/:id/connect`**

- 语义：请求「连接」该源，让它开始参与实时采集并写入 LatestQuoteStore。
- 实际行为取决于后端是否为该部署注入了对应的 Connector（例如 ForwardTickCollector / TushareWSTickCollector）。
- 响应：

```json
{
  "code": 0,
  "data": {
    "message": "connect request accepted"
  }
}
```

如果当前环境未配置 Connector，可能会多一个 `note: "connector not configured"` 字段，仅作提示。

**POST `/api/v1/realtime-sources/:id/disconnect`**

- 语义：请求「断开」该源，停止从该源采集行情并写入 Store。
- 响应结构与 connect 类似，只是 `message` 不同。

### 5.2 前端推荐交互

在「实时数据源管理」页面上，可以这样设计：

- **列表列**：
  - 名称、类型、优先级、是否主用、是否启用
  - 最近健康状态（图标 + 文本）
  - 当前是否已连接（可由后端在未来提供字段；目前可通过健康 + Selector 状态推断）
- **操作列按钮**：
  - 编辑（打开弹窗，支持修改 name / priority / config / flags）
  - 单次健康检查（调用 `GET /:id/health`）
  - Connect / Disconnect（调用对应 POST）

交互建议：

- 点击 **Connect** 后：
  - 立即禁用按钮，显示「连接中…」；
  - 等响应返回之后，提示「已发送连接请求」；
  - 后续可以轮询 `/realtime-sources/health` 或订阅心跳 SSE，以确认该源 `last_health_status` 是否变为 `healthy`，以及 `current_source` 是否切到了该 type。
- 点击 **Disconnect** 后：
  - 同样给出提示，并在状态页或 WebSocket 行情上观察数据是否停止从该源流入。

---

## 六、本地开发：利用内地 ts_proxy 调试

在本地开发环境中，可以通过内地 ts_proxy（部署在国内服务器）接收 Tushare 行情，再由本地 QDHub 连接该 ts_proxy，从而：

- 避免本地 IP 直接连 Tushare 造成 IP 限制；
- 使用真实 Tushare 行情调试前端，效果与生产接入类似。

典型做法（简化）：

1. **后端配置**（`qdhub/configs/config.yaml`）：

```yaml
tushare:
  realtime_source: "forward"
  forward_ws_url: "ws://<内地服务器IP>:8888/realtime"
  forward_rsa_public_key_path: "~/.key/public.pem"  # 内地提供的公钥，保存在本机
```

2. 启动 QDHub：`make run`，查看日志中启动自检：
   - `Realtime sources: checking Tushare 转发 (tushare_proxy)...`
   - `Realtime source Tushare 转发 (tushare_proxy) health: healthy`

3. 在前端「实时数据源管理」界面中：
   - 查看 `Tushare 转发` 源是否为 `enabled=true` 且 `health_check_on_startup=true`；
   - 如有需要，手动点击「连接」按钮（调用 `POST /realtime-sources/:id/connect`）。

4. 前端行情页连接 WebSocket `/api/v1/ws/realtime-quotes`，观察：
   - `current_source` 是否为 `tushare_proxy`；
   - `items` 中是否有实时 Tushare 行情。

更详细的 ts_proxy 部署与协议说明见 `design-tushare-realtime-tunnel.md` 与 `ts_proxy/DEPLOY.md`。

---

## 七、小结

- **管理页**负责 CRUD + 健康检查 + Connect/Disconnect；**行情页**只连 WebSocket，不直接关心哪个源在采集。
- 启动时健康自检会自动更新各源状态，并优先使用应用配置中的 ts_proxy 地址做连通性测试，便于本地调试。
- 前端只要严格按本文档的请求/响应格式接入，就可以方便地：
  - 观测多实时源的状态（健康 / 故障原因）；
  - 控制哪些源参与实时行情采集；
  - 在本地或测试环境安全地使用内地 ts_proxy 进行 Tushare 行情调试。

