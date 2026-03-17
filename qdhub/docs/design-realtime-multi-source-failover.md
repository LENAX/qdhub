# 多实时数据源与环境切换 + 生产端双 Collector 与故障切换

## 目标

1. **环境区分**：生产 = Tushare WS 与 Sina **同时运行**，仅从**当前选中源**写 Store；开发 = 仅 Sina。
2. **前端有限感知**：同一 WebSocket 推送中除行情快照外，返回**当前数据源**、**各源健康度**（healthy/degraded/unavailable）以及**各源最近故障原因**（如「在线Ip数量超限」、「connection reset」），便于前端展示来源、状态与具体错误。
3. **生产故障切换**：当前选中 collector 故障时**切换到另一源**，另一路已在跑，立即接续写 Store。

---

## 一、现状

- 实时数据来源：Tushare WebSocket（`ts_realtime_mkt_tick`）或 Pull（`realtime_quote` 等，Sina/东财）。
- 前端：`GET /api/v1/ws/realtime-quotes` 从 `LatestQuoteStore` 取快照；目前**仅 Tushare WS 采集时写入 Store**，Pull 只落 DuckDB。
- 执行入口：`workflow_executor.executeRealtimeStreaming` 根据 `APINames` 含 `ts_realtime_mkt_tick` 走 WS，否则走 Pull。

---

## 二、数据流（生产双 Collector + 切换）

```mermaid
flowchart TB
  subgraph trigger [触发]
    P[SyncPlan]
  end
  subgraph prod [生产 双 Collector 同时运行]
    P --> E1{Env=production?}
    E1 -->|是| StartBoth[同时启动 WS + Sina]
    StartBoth --> TushareWS[Tushare WS Workflow]
    StartBoth --> SinaPull[Sina realtime_quote Workflow]
    TushareWS --> DB1[ts_realtime_mkt_tick 表]
    SinaPull --> DB2[realtime_quote 表]
    Selector[RealtimeSourceSelector\nactive = tushare_ws 或 sina]
    TushareWS --> Selector
    SinaPull --> Selector
    Selector -->|仅当前 active 写| Store[LatestQuoteStore]
    TushareWS -.->|故障| Detect[检测当前 active 故障]
    SinaPull -.->|故障| Detect
    Detect --> Switch[SwitchTo 另一源]
    Switch --> Selector
  end
  subgraph dev [开发]
    P --> E2{Env=development?}
    E2 -->|是| ForceQuote[仅 realtime_quote + Sina]
    ForceQuote --> SinaOnly[Sina Pull]
    SinaOnly --> Store
    SinaOnly --> DB2
  end
  Store --> WS[/ws/realtime-quotes]
  WS --> FE[前端]
```

- **生产**：计划含 `ts_realtime_mkt_tick` 时**同时启动** Tushare WS 与 Sina 两个 workflow；**RealtimeSourceSelector** 维护当前选中源（默认 `tushare_ws`），**仅当前选中源**的数据写入 LatestQuoteStore。当前选中源故障时调用 `SwitchTo(另一源)`，另一源已在运行，立即接续写 Store。
- **开发**：仅启动 Sina，不启动 WS；Sina 直接写 Store。

---

## 三、实现要点

### 3.1 环境变量与配置（与原方案一致）

- `QDHUB_ENV`：`production` | `development`，默认 `development`。
- [container/container.go](internal/infrastructure/container/container.go)：`Config.RealtimeEnv`，默认 `development`。
- [cmd/server.go](cmd/server.go)：从 viper 读 `env` 写入 `config.RealtimeEnv`。
- [workflow_executor.go](internal/infrastructure/taskengine/workflow_executor.go)：根据 `realtimeEnv == "production"` 决定是否对 `ts_realtime_mkt_tick` 走 WS；development 下改为 `realtime_quote` + Pull。

### 3.2 写 LatestQuoteStore 与「仅当前选中源」写入

- **RealtimeSourceSelector**：维护 `active`；暴露 `ShouldWriteToStore(source)`、`SwitchTo(source)`；**前端有限感知**：`CurrentSource()`、`SourcesHealth()`、**`SourcesError() map[string]string`**（各源最近故障原因）；故障时由 Sync 层调用 **`RecordSourceError(source, errMsg)`** 写入该源错误文案（如 exec.ErrorMessage）。
- **WS / Sina 写 Store**：写前分别检查 `ShouldWriteToStore(...)`，仅 true 时写。开发环境仅 Sina 时 Selector 恒为 sina 或 nil。
- **统一字段**：Store 中统一为 Tushare 字段名，Sina 写前做映射。
- **前端有限感知**：[realtime_ws_handler.go](internal/interfaces/http/realtime_ws_handler.go) 注入 Selector；推送在 `type`、`scope`、`timestamp`、`items` 基础上增加 **`current_source`**、**`sources_health`**、**`sources_error`**（各源最近故障原因，健康时可空）。Selector 为 nil 时仅返回 sina 及单源、无错误。

### 3.3 生产端：双 workflow 启动与故障切换

**双 workflow 启动**：

- 生产且计划为 `ts_realtime_mkt_tick` 时，**同时启动**两个 workflow：Tushare WS 与 Sina `realtime_quote`。Sync 层（ExecuteSyncPlan）连续两次调用执行层（一次 APINames=ts_realtime_mkt_tick，一次 APINames=realtime_quote），创建**两个** execution 并提交两个 workflow。通过 `exec.SyncedAPIs` 区分：WS 对应 `ts_realtime_mkt_tick`，Sina 对应 `realtime_quote`。

**故障切换触发条件**：在将某次执行标记为失败时，若为**生产**且计划为**实时**且**该失败执行对应当前 active**，则先 **Selector.RecordSourceError(该源, exec.ErrorMessage)**（或 workflow 错误文案），再 **Selector.SwitchTo(另一源)**，不将计划置为已完成。前端通过 `sources_error` 可见该源具体故障原因（如被 ban、断连、403 等）。

**失败检测发生处**：GetExecutionProgress、HandleExecutionCallback、ReconcileRunningExecutions。在 exec 被标为 Failed 并持久化后，若对应当前 active，则先 **RecordSourceError(对应 source, errMsg)**，再 **SwitchTo(另一源)**，不执行 plan.MarkCompleted。

**Sync 层**：注入 RealtimeSourceSelector；ExecuteSyncPlan 在 production 且 ts_realtime_mkt_tick 时发起双 workflow；三处失败处理中根据 exec.SyncedAPIs 与当前 active 判断是否调用 SwitchTo。

### 3.4 配置注入链（补充）

- **WorkflowExecutor**：`realtimeEnv`；production 时按请求 APINames 提交 WS 或 Pull（由 Sync 两次调用实现双 workflow）。
- **RealtimeSourceSelector**：container 创建并注入；WS/Sina 写 Store 处注入或通过全局/Store 层访问。
- **SyncApplicationServiceImpl**：`realtimeEnv`、RealtimeSourceSelector；ExecuteSyncPlan 生产双 workflow；三处失败处理调用 SwitchTo。
- **Container**：传入 `c.config.RealtimeEnv`、RealtimeSourceSelector。

---

## 四、文件修改清单（双 Collector + 切换）

| 文件/组件 | 修改内容 |
|-----------|----------|
| container/container.go | Config.RealtimeEnv；创建并注入 RealtimeSourceSelector；NewWorkflowExecutor(..., RealtimeEnv)；NewSyncApplicationService(..., RealtimeEnv, Selector) |
| cmd/server.go | 从 viper 读取 `env` 写入 `config.RealtimeEnv` |
| workflow_executor.go | realtimeEnv；development 且 ts_realtime_mkt_tick 时只提交 Pull；production 时按 APINames 提交 WS 或 Pull |
| **RealtimeSourceSelector**（新） | active；ShouldWriteToStore/SwitchTo；CurrentSource、SourcesHealth、**SourcesError()**；故障时 **RecordSourceError(source, errMsg)** |
| ws_collector.go 或 WS 写 Store job | 写 Store 前 ShouldWriteToStore("tushare_ws")，仅 true 时写 |
| **realtime_ws_handler.go** | 注入 Selector；推送增加 **current_source**、**sources_health**、**sources_error**；nil 时仅 sina 且无错误 |
| realtime_jobs.go | RealtimeQuoteStreamHandlerJob 写 DuckDB 后若 ShouldWriteToStore("sina") 则写 Store；Sina 字段映射见 3.2 |
| sync_impl.go | ExecuteSyncPlan 生产双 workflow；三处失败处理：失败 exec 对应当前 active 时先 RecordSourceError(该源, errMsg)，再 SwitchTo(另一源)，不 MarkCompleted(plan) |
| frontend-realtime-integration.md | 补充 QDHUB_ENV、双 Collector 与切换、**前端有限感知**：current_source、sources_health、**sources_error**（各源最近故障原因）含义与取值 |

---

## 五、可选后续

- **恢复主路径**：故障转移后若希望次日或手动恢复为 Tushare WS，可增加「按时间或接口重新启动计划（主路径）」逻辑，首版可不做。
- **字段归一化**：若前端对 Store 字段有强约束，可在写 Store 前将 realtime_quote 字段映射为与 Tushare tick 一致的 key。

---

## 六、收尾（主体开发完成后）

- **Docker 镜像注入 QDHUB_ENV**：完成上述主体开发后，再修改镜像与部署配置，使打包推送的镜像在运行时为生产环境。
  - [qdhub/Dockerfile](qdhub/Dockerfile)：在最终阶段（`FROM debian:12-slim` 之后）增加 `ENV QDHUB_ENV=production`，使镜像默认以 production 运行。
  - [docker-compose.image.yml](docker-compose.image.yml)：在 backend 的 `environment` 中增加 `QDHUB_ENV=production`，与 Dockerfile 一致，便于 ECS 部署时显式使用生产环境。

**备注（完成后必做）**：开发完成后需（1）**更新前端开发文档**（如 frontend-realtime-integration.md 或前端项目内对接说明），包含 WebSocket 新字段 `current_source`、`sources_health`、`sources_error` 及环境/双源说明；（2）**执行集成测试**（workflow 执行层、Selector、Sync 故障切换分支）；（3）**执行 e2e 测试**（实时同步 + WebSocket 订阅、开发/生产环境分支、可选故障切换场景）。
