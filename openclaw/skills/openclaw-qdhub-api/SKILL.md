---
name: openclaw-qdhub-api
description: >-
  OpenClaw 投研助手：以 QDHub 为中国 A 股主数据源（认证、Analysis/Datastore API、脚本 qdhub_cli）；
  可与同级技能 peng-daofu-dragon-head 联用，将接口结果写入龙头四维的 qdhub_evidence；
  含知识库发现与投研输出规范。在用户进行 A 股分析、拉取行情财务涨跌停概念资金数据时使用。
---

# OpenClaw 投研助手 — 完整技能

当用户需要进行**中国 A 股投研分析**时，使用本技能：以 **QDHub** 为唯一或主要数据源获取行情与基本面数据，在需要时**发现并引用知识库**以增强分析，并输出结构化的投研结论。

---

## 一、角色与原则

- **角色**：投研分析助手，面向中国 A 股市场。
- **数据优先**：结论尽量基于 QDHub 拉取到的数据（K 线、财务、资金、涨跌停、概念等），避免无依据断言。
- **知识库可选**：若分析需要研报、文档、历史结论等，先**发现知识库是否就绪**；就绪则检索并引用，未就绪则仅基于 QDHub 数据并明确说明“当前未接入知识库”。
- **输出与分析规范**（用户要求分析时**必须**遵守）：
  - **推理过程**：输出的结论必须**包含推理过程**，即从数据到结论的中间步骤（用了哪些数据、做了何种比较或计算、得到何种推断）。
  - **逻辑与数据支撑**：结论须有**充分的逻辑和数据支撑**，避免主观臆断；可引用具体指标、区间、涨跌幅、资金流向等数据说明理由。
  - **推荐标的时的必备项**：若输出中涉及**推荐股票或标的**，必须同时给出：
    - **风险提示**：如波动风险、流动性、政策与行业风险、数据滞后性等与标的相关的风险说明；
    - **可能的止损点**：基于数据或常见规则给出可参考的止损位或止损思路（如技术位、比例止损等），并注明“仅供参考，不构成投资建议”。

### 1.1 与彭道富龙头技能联用（`peng-daofu-dragon-head`）

当任务涉及**题材龙头、领涨甄别、龙头四维、假龙头过滤**，或用户点名**彭道富 / 龙头战法**时，在遵守本节全部规范的前提下，**叠加**项目技能 **`peng-daofu-dragon-head`**（与本文同级目录：`openclaw/skills/peng-daofu-dragon-head/SKILL.md`）。

**协作顺序（Agent 必守）**

1. **先用本技能拉取 QDHub 证据**：认证、`trade-cal`、K 线、涨跌停/天梯、概念与成分、资金流、财务/新闻等（按需，见第五节接口表）。
2. **再用彭道富技能定脉、四维打分**：将上一步得到的客观事实写入 `DragonHeadScreening` 各维的 `qdhub_evidence`（短句，可含接口名与关键字段）；**禁止**无数据支撑的满分。
3. **统一输出**：自然语言结论须包含数据推理链；若输出 `model_dump()` / JSON，须与 QDHub 引用一致；涉及荐股仍须本节「推荐标的时的必备项」。

**QDHub 接口 → 龙头四维（`qdhub_evidence` 来源指引）**

| 龙头维度（Pydantic 字段） | 优先引用的 QDHub 路径（均为 `/api/v1/analysis/...` 前缀） |
|---------------------------|----------------------------------------------------------|
| `value_boost` 价值加持 | `financial/indicators`、`financial/income`、单股 `stocks/:ts_code/basic`、`news` |
| `outward_influence` 对外影响力 | `limit-up-ladder`、`sector-limit-up-stats`、`sector-limit-up-stocks`、`concept-stocks`、`index-ohlcv` / 指数 `kline` |
| `market_recognition` 市场性 | `money-flow`、`moneyflow-rank`、`moneyflow-concept`、`stocks/snapshot`、`dragon-tiger`、`popularity-rank` |
| `firstness` 第一性 | `limit-up-ladder`、`limit-up-list`、`limit-stocks`、`concept-heat`、`concept-rotation` |

情绪周期（`EmotionCyclePhase`）可结合：`limit-stats`、连板高度分布、板块跌停家数、核心标断板后反馈等，**在彭道富技能内**填入；本技能不提供单独枚举接口。

**脚本路径约定（项目根）**：`openclaw/skills/openclaw-qdhub-api/scripts/qdhub_cli.py`（见 4.4）。

---

## 二、知识库发现与使用

知识库可能尚未部署，OpenClaw 必须在每次需要引用知识库时**先发现其是否可用**，再决定是否检索。

### 2.1 发现流程（按顺序尝试）

1. **环境/配置**  
   检查是否配置了知识库相关变量或开关，例如：  
   - `OPENCLAW_KB_ENABLED`、`KNOWLEDGE_BASE_URL`、`RAG_ENDPOINT` 等（具体以你实际部署名为准）。  
   若存在且明确为“已启用”或给出了 endpoint，则认为知识库**可能就绪**，进入步骤 2。

2. **探活/列表接口**  
   若配置了知识库 base URL 或 endpoint，调用**轻量级**接口判断可用性，例如：  
   - `GET {knowledge_base_url}/health` 或 `/ping`；或  
   - `GET {knowledge_base_url}/indices` / `/collections`（列表）；或  
   - `GET {knowledge_base_url}/` 返回 200。  
   任一成功（2xx）且返回结构合理，则认为知识库**就绪**。

3. **试一次检索**  
   若无法通过配置或探活确定，可尝试一次**简单检索**（如 query="测试" 或 "A股"），限制条数 1。  
   - 成功返回结果（非 5xx、非“服务不可用”）→ 知识库**就绪**。  
   - 超时、连接失败、5xx 或明确“未配置” → 知识库**未就绪**。

### 2.2 知识库就绪时

- 在需要研报、文档、历史观点、政策解读等时，**先检索知识库**，再结合 QDHub 数据做分析。
- 在回答中**明确引用**知识库中的文档或片段（如标题、来源、日期），并在结论处区分“来自数据”与“来自知识库”。

### 2.3 知识库未就绪时

- **不假定**知识库存在，不虚构“根据知识库……”的内容。
- 仅基于 **QDHub 数据**进行分析；若用户问题强依赖研报/文档，在回答中说明：  
  “当前未检测到可用的知识库，本结论仅基于 QDHub 行情与基本面数据；如需结合研报或文档，请先配置并接入知识库。”
- 可顺带提示：知识库通常需单独部署（如 RAG/向量检索服务），配置后 OpenClaw 会自动发现并引用。

---

## 三、中国 A 股语境（QDHub 数据约定）

- **市场**：上海主板/科创板、深圳主板/创业板等，统一用 **ts_code** 表示标的，格式为 `六位数字.交易所`，例如：
  - 上交所：`600519.SH`、`688xxx.SH`
  - 深交所：`000001.SZ`、`300xxx.SZ`
- **日期**：统一 **YYYYMMDD**（如 `20250315`）；QDHub 的交易日历与 K 线、涨跌停等均按此格式。
- **交易日历**：先通过 QDHub 的 `GET /api/v1/analysis/trade-cal` 获取 `start_date`～`end_date` 内的交易日，再做“最近 N 个交易日”或“某日是否交易日”的判断。
- **复权**：前复权 `adjust_type=qfq`、后复权 `hfq`、不复权 `none`；做价格与收益率分析时通常用 `qfq`。**指数**也可用同一套 K 线接口：`ts_code` 传指数代码（如 `000001.SH`、`399001.SZ`），后端在个股 `daily` 无数据时会读 `index_daily`；指数无复权因子，`qfq`/`hfq` 与 `none` 等价。
- **数据范围**：数据是否覆盖某区间取决于 QDHub 同步情况；若某接口返回空，可提示“该区间可能暂无数据或未同步”。

---

## 四、QDHub 认证与调用

### 4.1 基础信息

| 项 | 值 |
|----|-----|
| Base URL | `https://qdhub.quantrade.team` |
| 登录 | `POST /api/v1/auth/login` |
| 认证 | 响应中的 `access_token` 作为 **Bearer Token** |

**认证方式**：**不要**在技能或脚本中硬编码用户名和密码。应通过环境变量提供，并使用本技能附带的 Python 脚本完成登录与 API 调用（见 4.4）。

### 4.2 登录（原始 HTTP）

- **URL**: `https://qdhub.quantrade.team/api/v1/auth/login`
- **Method**: `POST`
- **Content-Type**: `application/json`
- **Body**: `{ "username": "<从环境变量读取>", "password": "<从环境变量读取>" }`

**响应（LoginResponse）**：

```json
{
  "access_token": "eyJhbGc...",
  "refresh_token": "eyJhbGc...",
  "user": { "id": "...", "username": "...", "email": "", "roles": [], "status": "active" }
}
```

后续所有需认证请求头：**`Authorization: Bearer <access_token>`**。

### 4.3 通用响应格式

成功：`{ "code": 0, "data": <...>, "msg": "ok" }`。  
列表可能为 `data: { "total", "items" }` 或 `data: []`。失败时 `code` 非 0，`msg` 为错误信息。

### 4.4 通过 Python 脚本调用（推荐，OpenClaw 可执行）

本技能提供可被 OpenClaw 调用的 **Python 3.10+（建议 3.15）** 脚本，依赖 **httpx**（你已安装 httpx CLI 时通常已具备）。脚本通过环境变量获取用户名和密码，登录后可将 token 写入环境变量或本地文件，避免在技能中写死账号密码，并统一提供 QDHub 数据接口的便捷调用方式。

**脚本路径**（相对于本技能目录）：  
`scripts/qdhub_cli.py`

**环境变量**（由 OpenClaw 或部署环境配置，勿写入技能正文）：

| 变量 | 必填 | 说明 |
|------|------|------|
| `QDHUB_USERNAME` | 是（login 时） | 登录用户名 |
| `QDHUB_PASSWORD` | 是（login 时） | 登录密码 |
| `QDHUB_BASE_URL` | 否 | 默认 `https://qdhub.quantrade.team` |
| `QDHUB_ACCESS_TOKEN` | 否 | 若已设置则 get/post 直接使用，无需先 login |
| `QDHUB_TOKEN_FILE` | 否 | token 持久化文件路径；login 时写入，get/post 时若未设 token 则从此文件读取 |

**子命令**：

1. **login** — 使用 `QDHUB_USERNAME` / `QDHUB_PASSWORD` 登录；若设置了 `QDHUB_TOKEN_FILE` 会写入该文件；**标准输出**打印 `export QDHUB_ACCESS_TOKEN=...`，便于在 shell 中执行 `eval $(python qdhub_cli.py login)` 更新当前会话环境变量。  
2. **get** \<path\> [key=value ...] — 对 base_url + path 发起 GET，query 参数由后续 key=value 提供；输出 JSON。  
3. **post** \<path\> [--body 'json'] — 对 base_url + path 发起 POST，`--body` 为 JSON 字符串；输出 JSON。

**OpenClaw 调用示例**（在配置好上述环境变量后，从技能目录或项目根执行）：

```bash
# 登录并刷新 token（可选，若已设 QDHUB_ACCESS_TOKEN 或 QDHUB_TOKEN_FILE 且未过期可跳过）
eval $(python openclaw/skills/openclaw-qdhub-api/scripts/qdhub_cli.py login)

# 交易日历
python openclaw/skills/openclaw-qdhub-api/scripts/qdhub_cli.py get /api/v1/analysis/trade-cal start_date=20250101 end_date=20251231

# K 线（个股）
python openclaw/skills/openclaw-qdhub-api/scripts/qdhub_cli.py get /api/v1/analysis/kline ts_code=000001.SZ start_date=20250101 end_date=20250315 adjust_type=qfq period=D

# K 线（指数，ts_code 为指数代码）
python openclaw/skills/openclaw-qdhub-api/scripts/qdhub_cli.py get /api/v1/analysis/kline ts_code=000001.SH start_date=20250101 end_date=20250315 adjust_type=none period=D

# 申万行业分类（index_classify）
python openclaw/skills/openclaw-qdhub-api/scripts/qdhub_cli.py get /api/v1/analysis/index-sectors src=SW2021 level=L1 limit=50

# 指数/行业成分（优先 index_weight；无则回退 index_member_all / ci_index_member）
python openclaw/skills/openclaw-qdhub-api/scripts/qdhub_cli.py get /api/v1/analysis/index-sector-members index_code=000300.SH trade_date=20250301 limit=100

# 只读 SQL（DuckDB 语法，仅 SELECT）
python openclaw/skills/openclaw-qdhub-api/scripts/qdhub_cli.py post /api/v1/analysis/custom-query/query --body '{"sql":"SELECT * FROM trade_cal WHERE is_open=1 LIMIT 10","max_rows":10000,"timeout_seconds":30}'
```

脚本依赖：`pip install -r openclaw/skills/openclaw-qdhub-api/scripts/requirements.txt`（主要为 `httpx`）。详见 `scripts/README.md`。

---

## 五、Analysis API（分析/行情）

Base path: **`/api/v1/analysis`**，除注明外均为 **GET**，需 **Bearer Token**，参数为 query。

| 接口 | 路径 | 主要参数 | 返回说明 |
|------|------|----------|----------|
| K 线 | `/analysis/kline` | `ts_code`（**股票或指数**）, `start_date`, `end_date`, `adjust_type`=none/qfq/hfq, `period`=D/W/M | KLineData[]（指数走 `index_daily` 时无复权因子，等价复权结果与 none 一致） |
| 交易日历 | `/analysis/trade-cal` | `start_date`, `end_date` | `{ "dates": ["YYYYMMDD", ...] }` |
| 股票列表 | `/analysis/stocks` | `market`, `industry`, `list_status`, `query`, `limit`, `offset` | `{ "total", "items": StockInfo[] }` |
| 股票快照 | `/analysis/stocks/snapshot` | `trade_date`, `adjust_type`, `ts_codes`（逗号分隔） | `{ "trade_date", "items": StockInfo[] }` |
| 单股基础信息 | `/analysis/stocks/:ts_code/basic` | path: `ts_code` | StockBasicInfo |
| 技术指标 | `/analysis/stocks/indicators` | `ts_code`, `start_date`, `end_date`, `adjust_type`, `period`, `indicators`（如 MA5,RSI,MACD） | TechnicalIndicator[] |
| 指数列表 | `/analysis/indices` | `market`, `category`, `limit`, `offset` | `{ "total", "items": IndexInfo[] }` |
| 指数/行业分类 | `/analysis/index-sectors` | `level`, `parent_code`, `src`, `index_code`, `query`, `limit`, `offset` | `{ "total", "items": IndexSectorInfo[] }`（`index_classify`） |
| 指数/行业成分 | `/analysis/index-sector-members` | **`index_code`**（必填）, `trade_date`（可选，空则取 index_weight 最新日）, `limit`, `offset` | `{ "total", "items": IndexSectorMember[] }`（优先 `index_weight`） |
| 资金流入排名 | `/analysis/moneyflow-rank` | `trade_date`（可选）、`scope`=stock/concept/all、`limit`、`offset` | `MoneyFlowRankResult`（个股 moneyflow→ths→dc；概念 moneyflow_cnt_ths） |
| 指数 OHLCV | `/analysis/index-ohlcv` | `ts_code`、`days`（默认 10）、`end_date`（可选，默认最新） | `IndexOHLCVResult`（仅 `index_daily`） |
| 概念列表 | `/analysis/concepts` | `source`, `limit`, `offset` | `{ "total", "items": ConceptInfo[] }` |
| 财务指标 | `/analysis/financial/indicators` | `ts_code`, `start_date`, `end_date`, `limit`, `offset` | `{ "total", "items": FinancialIndicator[] }` |
| 利润表 | `/analysis/financial/income` | `ts_code`, `start_date`, `end_date`, `report_type`, `limit`, `offset` | `{ "total", "items": []map }` |
| 资产负债表 | `/analysis/financial/balancesheet` | 同上 | 同上 |
| 现金流量表 | `/analysis/financial/cashflow` | 同上 | 同上 |
| 涨跌停统计 | `/analysis/limit-stats` | `start_date`, `end_date` | LimitStats[] |
| 涨跌停列表 | `/analysis/limit-stocks` | `trade_date`, `limit_type`=up/down | `{ "trade_date", "items": LimitStock[] }` |
| 涨停天梯 | `/analysis/limit-up-ladder` | `trade_date` | `{ "trade_date", "ladders", "first_board_stocks" }` |
| 涨停对比 | `/analysis/limit-up-comparison` | `today_date` | LimitUpComparison |
| 涨停列表(分页) | `/analysis/limit-up-list` | `trade_date`, `limit`, `offset` | `{ "total", "items": LimitUpStock[] }` |
| 板块涨停统计 | `/analysis/sector-limit-up-stats` | `trade_date`, `sector_type`=industry/concept | `{ "trade_date", "sector_type", "stats" }` |
| 板块涨停个股 | `/analysis/sector-limit-up-stocks` | `sector_code`, `sector_type`, `trade_date` | `{ "sector_code", "sector_type", "trade_date", "stocks" }` |
| 概念热度 | `/analysis/concept-heat` | `trade_date` | ConceptHeat[] |
| 概念成分股 | `/analysis/concept-stocks` | `concept_code`, `trade_date` | `{ "concept_code", "trade_date", "items" }` |
| 概念轮动 | `/analysis/concept-rotation` | `start_date`, `end_date`, `rank_by`, `top_n` | ConceptRotationStats |
| 龙虎榜 | `/analysis/dragon-tiger` | `trade_date` 或 `ts_code` 至少一个, `limit`, `offset` | `{ "items": DragonTigerList[] }` |
| 资金流向 | `/analysis/money-flow` | `trade_date` 或 `start_date`+`end_date` 或 `ts_code`, `market`, `limit`, `offset` | `{ "trade_date", "items": MoneyFlow[] }` |
| 概念资金流 | `/analysis/moneyflow-concept` | `trade_date`, `concept`, `limit`, `offset` | `{ "trade_date", "items" }` |
| 人气榜 | `/analysis/popularity-rank` | `rank_type`, `limit` | `{ "rank_type", "items": PopularityRank[] }` |
| 资讯列表 | `/analysis/news` | `ts_code`, `category`, `start_date`, `end_date`, `sources`, `order`, `limit`, `offset` | `{ "total", "items": NewsItem[] }` |
| 实时分笔 | `/analysis/realtime-tick` | `ts_code`, `limit` | `{ "items": TickRow[] }` |
| 历史分笔 | `/analysis/intraday-ticks` | `ts_code`, `trade_date`(或`date`) | `{ "ts_code", "date", "items": TickRow[] }` |
| 分钟 K 线 | `/analysis/intraday-kline` | `ts_code`, `trade_date`, `period`=1m 等 | `{ "items": IntradayKlineRow[] }` |

**POST**

- **因子计算** `POST /analysis/factors`  
  Body: `{ "ts_codes": [], "start_date", "end_date", "factors": [ FactorExpression ] }`  
  返回: FactorValue[]

- **只读 SQL** `POST /analysis/custom-query/query`  
  Body: `{ "sql": "SELECT ...", "max_rows": 10000, "timeout_seconds": 30 }`  
  返回: `{ "columns", "rows" }`（CustomQueryResult）  
  **约束**：SQL 须符合 **DuckDB** 语法；**仅允许 SELECT**，禁止 UPDATE、DELETE、INSERT、CREATE TABLE、ALTER、DROP 等写操作与 DDL。

---

## 六、Datastore API（表级数据）

Base path: **`/api/v1`**，需 **Bearer Token**。

| 接口 | 方法 | 路径 | 说明 |
|------|------|------|------|
| 列表 | GET | `/datastores` | 所有 QuantDataStore（id/name/type 等） |
| 详情 | GET | `/datastores/:id` | 单个数据存储 |
| 表列表 | GET | `/datastores/:id/tables` | `{ "data": ["table1", ...] }` |
| 表数据(分页) | GET | `/datastores/:id/tables/:tableName/data` | `page`, `page_size`, `q`, `search_column`；返回 `{ "data": rows[], "total": n }` |

---

## 七、数据目录（Tushare）

当前数据以 **Tushare** 为主。发现可用表与 API 名称：

1. **数据源列表**：`GET /api/v1/datasources` → 找到 `name` 为 `tushare` 的 `id`。
2. **API 名称**：`GET /api/v1/datasources/:id/api-names` → `{ "api_names": ["daily", "stock_basic", "trade_cal", ...] }`。
3. **分类（可选）**：`GET /api/v1/datasources/:id/api-categories?has_apis_only=true`。
4. **实际表**：`GET /api/v1/datastores` 取存储 id → `GET /api/v1/datastores/:id/tables` 得表名 → 用 `tables/:tableName/data` 分页查。

常见类型：基础/日历（`stock_basic`, `trade_cal`）、行情（`daily`, `weekly`, `adj_factor`）、资金/龙虎榜/概念/财务等，以实际 `api-names` 与 `tables` 为准。

---

## 八、投研分析流程建议

1. **认证**：配置 `QDHUB_USERNAME`、`QDHUB_PASSWORD`（及可选 `QDHUB_TOKEN_FILE`）；若通过脚本调用，可执行一次 `eval $(python openclaw/skills/openclaw-qdhub-api/scripts/qdhub_cli.py login)` 将 token 写入当前 shell，或直接执行 get/post（脚本会在无 token 时自动登录）。
2. **确定范围**：根据问题确定标的（ts_code）、日期区间；需要“最近 N 个交易日”时先调 `trade-cal` 取日期列表。
3. **拉取数据**：按需调用 analysis（K 线、财务、资金、涨跌停、概念等）或 datastore 表数据；缺失数据时说明可能未同步。
4. **龙头战法（可选）**：若符合 **1.1** 触发条件，在拉取数据后使用 `openclaw/skills/peng-daofu-dragon-head` 中的流程与 `DragonHeadScreening` 结构化输出，证据链来自本节已取 QDHub 结果。
5. **知识库**：若分析需要研报/文档，执行**知识库发现**（见第二节）；就绪则检索并引用，未就绪则仅用 QDHub 并说明。
6. **综合与输出**：结合数据与（若有）知识库结果，给出结论、数据来源和局限（如“仅 QDHub”“未接入知识库”等）。

---

## 九、调用流程小结

- **推荐**：使用本技能附带的 **Python 脚本**（4.4）：设置 `QDHUB_USERNAME`、`QDHUB_PASSWORD`，执行 `python openclaw/skills/openclaw-qdhub-api/scripts/qdhub_cli.py get/post ...`，脚本负责登录与 token 管理，输出 JSON 便于 OpenClaw 解析。
- **原始 HTTP**：登录 `POST https://qdhub.quantrade.team/api/v1/auth/login`，Body 从环境变量读取 `username`/`password`；取响应中的 `access_token`；业务请求 Header `Authorization: Bearer <access_token>`，Base URL 由 `QDHUB_BASE_URL` 或默认。
- 数据目录：`datasources` → `datasources/:id/api-names`（及可选 `api-categories`）；表数据用 `datastores` → `datastores/:id/tables` → `tables/:tableName/data`。

Token 过期时脚本在 401 时会自动重新登录（若配置了用户名密码）；若使用刷新接口可后续在脚本中扩展。
