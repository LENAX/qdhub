# 彭道富龙头战法 — Agent 速查

## 一花三叶（DragonPath）

| `path` 值 | 含义 | 数据侧重 |
|-----------|------|----------|
| `equity` | 股权/价值龙头，长期视角 | 基本面、行业地位、盈利趋势 |
| `white_horse` | 白马/价值投机，主升浪波段 | 业绩预期 + 题材 + 量价资金 |
| `dark_horse` | 黑马/盘面人气 | 涨停结构、概念梯队、换手与情绪 |

## 龙头四维（DimensionAssessment）

| 字段名 | 彭氏维度 | 要点 |
|--------|----------|------|
| `value_boost` | 价值加持 | 能否落脚业绩逻辑（价、量、订单、行业复苏等）；纯概念则减分 |
| `outward_influence` | 对外影响力 | 对大盘/板块的带动；**板块效应**弱则降权 |
| `market_recognition` | 市场性 | 资金与定价是否认可；逻辑自洽但市场不认 → `pending` 或等确认 |
| `firstness` | 第一性 | 同梯队辨识度、率先性、空间地位；**领涨非独涨** |

## 情绪周期（EmotionCyclePhase）

| 值 | 说明 |
|----|------|
| `unknown` | 未判断 |
| `start` / `ferment` / `climax` | 主升相关阶段 |
| `ebb` | 退潮；**黑马**与模型 `recommendation` 默认从严 |

## 破执：关键标签（代码常量）

以下任一出现在 `false_pattern_flags` 中即 `has_critical_false_pattern`，**通常应 `recommendation=pass`**（以模型计算为准）：

- `pure_hype_no_value_anchor`
- `no_sector_effect`
- `result_only_chasing`

其它自定义标签可用于说明风险，未必全部在常量集中。

## recommendation 规则（实现摘要）

- 任维 `verdict=fail` 或命中上述关键标签 → `pass`
- `dark_horse` + `cycle_phase=ebb` → `pass`
- 四维全 `pass` 且加权分 ≥ 7.5 → `strong`
- 加权分 ≥ 5 且无 `fail` → `watch`
- 其余 → `pass`

完整定义见 `openclaw/schemas/dragon_head.py`。

## 与全文的对应

体系阐述：`openclaw/knowledge/彭道富龙头战法-OpenClaw选股体系.md`。

## QDHub → 四维（写入 `qdhub_evidence`）

路径均为 `GET/POST .../api/v1/analysis/` 后缀（与 `openclaw-qdhub-api` 第五节一致）。

| 维度 | 常用接口 |
|------|----------|
| `value_boost` | `financial/indicators`、`financial/income`、`stocks/:ts_code/basic`、`news` |
| `outward_influence` | `limit-up-ladder`、`sector-limit-up-stats`、`sector-limit-up-stocks`、`concept-stocks`、指数 `kline` / `index-ohlcv` |
| `market_recognition` | `money-flow`、`moneyflow-rank`、`moneyflow-concept`、`stocks/snapshot`、`dragon-tiger`、`popularity-rank` |
| `firstness` | `limit-up-ladder`、`limit-up-list`、`limit-stocks`、`concept-heat`、`concept-rotation` |

**情绪周期辅助**（无单独枚举接口）：`limit-stats`、`limit-stocks`（跌停家数）、天梯高度与核心标断板反馈等，由 Agent 归纳后写入 `cycle_phase`。
