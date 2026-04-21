# 示例：组装 DragonHeadScreening

以下仅为**结构示例**，分数与结论不得当作真实荐股。

```python
from openclaw.schemas.dragon_head import (
    DimensionAssessment,
    DragonHeadScreening,
    DragonPath,
    EmotionCyclePhase,
    Verdict,
)

row = DragonHeadScreening(
    ts_code="600519.SH",
    trade_date="20260421",
    path=DragonPath.WHITE_HORSE,
    cycle_phase=EmotionCyclePhase.FERMENT,
    value_boost=DimensionAssessment(
        score=8.0,
        verdict=Verdict.PASS,
        rationale="提价+动销逻辑与公开信息一致",
        qdhub_evidence=["财务/预告字段摘要…", "行业新闻日期…"],
    ),
    outward_influence=DimensionAssessment(
        score=7.0,
        verdict=Verdict.PASS,
        rationale="同板块多只跟风走强",
        qdhub_evidence=["概念成分涨幅分布…"],
    ),
    market_recognition=DimensionAssessment(
        score=8.0,
        verdict=Verdict.PASS,
        rationale="成交额与资金与叙事同步",
        qdhub_evidence=["资金流接口摘要…"],
    ),
    firstness=DimensionAssessment(
        score=7.5,
        verdict=Verdict.PASS,
        rationale="同题材涨幅与辨识度领先",
        qdhub_evidence=["涨停梯队/涨幅排序…"],
    ),
    false_pattern_flags=[],
)

print(row.model_dump())
print(row.to_summary_dict())
```

**命中破执（应导向 pass）示例**：

```python
DragonHeadScreening(
    ts_code="000001.SZ",
    trade_date="20260421",
    path=DragonPath.DARK_HORSE,
    cycle_phase=EmotionCyclePhase.START,
    value_boost=DimensionAssessment(
        score=2.0, verdict=Verdict.FAIL, rationale="纯情绪，无业绩锚"
    ),
    outward_influence=DimensionAssessment(
        score=4.0, verdict=Verdict.PENDING, rationale="板块跟风弱"
    ),
    market_recognition=DimensionAssessment(
        score=6.0, verdict=Verdict.PASS, rationale="短线资金集中"
    ),
    firstness=DimensionAssessment(
        score=7.0, verdict=Verdict.PASS, rationale="率先涨停"
    ),
    false_pattern_flags=["no_sector_effect", "pure_hype_no_value_anchor"],
)
# recommendation 预期为 pass
```
