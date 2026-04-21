---
name: peng-daofu-dragon-head
description: >-
  彭道富龙头战法：一花三叶、龙头四维、破执过滤、Pydantic 模型 DragonHeadScreening；
  须与同级技能 openclaw-qdhub-api 配合，先拉 QDHub 再填 qdhub_evidence。
  在用户提到龙头战法、彭道富、龙头四维、题材龙头、领涨甄别、假龙头过滤或结构化龙头打分时使用。
---

# 彭道富龙头战法（OpenClaw）

## 何时启用

- 用户要做**龙头 / 领涨 / 题材核心**筛选、复盘或论证，且认同或指定**彭道富框架**。
- 用户要求**结构化输出**（JSON / 可校验对象），而非散文式股评。

## 与 QDHub 技能强制联用

本技能**不替代**数据获取。凡涉及 A 股实盘数据，必须先加载并执行 **`openclaw-qdhub-api`**：

- 技能路径（相对项目根）：**`openclaw/skills/openclaw-qdhub-api/SKILL.md`**
- CLI：**`openclaw/skills/openclaw-qdhub-api/scripts/qdhub_cli.py`**（`QDHUB_USERNAME` / `QDHUB_PASSWORD` / token，见该技能 §4.4）

**固定顺序**：`openclaw-qdhub-api` 拉数 → 本技能定脉、四维、`false_pattern_flags`、（可选）`cycle_phase` → 组装 **`DragonHeadScreening`**（唯一代码源：`openclaw/schemas/dragon_head.py`）。

**证据写法**：`qdhub_evidence` 每条建议带「接口路径 + 日期/代码 + 关键字段摘要」，与 `openclaw-qdhub-api` §1.1 映射表一致；数据缺失时写 `verdict=pending` 并说明缺哪类接口，禁止编造。

## 与其它材料的关系

- **理论全文**：`openclaw/knowledge/彭道富龙头战法-OpenClaw选股体系.md`
- **四维 / 破执速查**：[reference.md](reference.md)（含 QDHub 接口索引）

## 核心流程（Agent 必做）

1. **定脉（三选一）**  
   `DragonPath`：`equity`（股权/价值龙头）｜`white_horse`（白马/价值投机波段）｜`dark_horse`（黑马/盘面人气）。  
   不确定则先 `white_horse` 或向用户确认；**禁止**在未定脉时把短线情绪规则套在长持价值票上。

2. **定级（行情）**  
   结合指数环境、主线是否清晰、板块涨停分布，判断当前是否值得做龙头博弈；退潮期对 **黑马** 默认从严（见模型规则）。

3. **四维逐项填证据**  
   每一维使用 `DimensionAssessment`：`score` 0～10、`verdict`（`pass`/`pending`/`fail`）、`rationale`、`qdhub_evidence`（短句列表，对应真实拉取的数据）。  
   四维含义见 reference.md；**禁止**无证据打满分。

4. **破执标签**  
   将可疑点写入 `false_pattern_flags`（字符串列表）。命中 **`CRITICAL_FALSE_PATTERN_FLAGS`**（定义在 `openclaw/schemas/dragon_head.py`）时，模型会将 `recommendation` 压为 `pass`，Agent 须在自然语言结论中说明原因。

5. **可选情绪周期**  
   `EmotionCyclePhase`：`unknown`｜`start`｜`ferment`｜`climax`｜`ebb`。黑马脉在 `ebb` 时模型层默认不参与强推。

6. **产出**  
   - 使用 **`DragonHeadScreening`**（`openclaw/schemas/dragon_head.py`）组装实例；对用户展示时给出 `model_dump()` 或 `to_summary_dict()`，并保留推理链。  
   - 若涉及**推荐标的**，仍须遵守 `openclaw-qdhub-api`：**风险提示**、**止损思路**、并声明不构成投资建议。

## Pydantic 使用要点

- 依赖：`pip install -r openclaw/requirements.txt`（项目根）。
- 导入：`from openclaw.schemas.dragon_head import DragonHeadScreening, DimensionAssessment, ...`（`PYTHONPATH` 含项目根）。
- 权重：`DragonHeadWeights` 默认价值加持 0.30，其余见代码；自定义时**四维和须为 1**。
- 校验 JSON：见 **[scripts/README.md](scripts/README.md)**。

## 输出检查清单

- [ ] 已选 `path`，且叙事与持有周期一致  
- [ ] 四维均有 `qdhub_evidence` 或明确标注数据缺失  
- [ ] 已检查 `false_pattern_flags` 与关键常量  
- [ ] 已输出 `weighted_score`、`recommendation`  
- [ ] 若 `openclaw-qdhub-api` 适用，已满足其投研输出规范  

## 更多示例

见 **[examples.md](examples.md)**。
