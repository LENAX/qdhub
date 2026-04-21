"""DragonHeadScreening 模型行为测试（真实 pydantic，无 mock）。"""

from openclaw.schemas.dragon_head import (
    CRITICAL_FALSE_PATTERN_FLAGS,
    DimensionAssessment,
    DragonHeadScreening,
    DragonHeadWeights,
    DragonPath,
    EmotionCyclePhase,
    Verdict,
)


def _base_dims() -> tuple[DimensionAssessment, DimensionAssessment, DimensionAssessment, DimensionAssessment]:
    ok = DimensionAssessment(score=8.0, verdict=Verdict.PASS, rationale="ok")
    return ok, ok, ok, ok


def test_weighted_score_and_strong_recommendation():
    vb, inf, mkt, fst = _base_dims()
    row = DragonHeadScreening(
        ts_code="600519.SH",
        trade_date="20260421",
        path=DragonPath.WHITE_HORSE,
        cycle_phase=EmotionCyclePhase.FERMENT,
        value_boost=vb,
        outward_influence=inf,
        market_recognition=mkt,
        firstness=fst,
    )
    assert row.weighted_score == 8.0
    assert row.recommendation == "strong"


def test_critical_flag_forces_pass():
    vb, inf, mkt, fst = _base_dims()
    flag = next(iter(CRITICAL_FALSE_PATTERN_FLAGS))
    row = DragonHeadScreening(
        ts_code="000001.SZ",
        trade_date="20260421",
        path=DragonPath.DARK_HORSE,
        value_boost=vb,
        outward_influence=inf,
        market_recognition=mkt,
        firstness=fst,
        false_pattern_flags=[flag],
    )
    assert row.has_critical_false_pattern
    assert row.recommendation == "pass"


def test_dark_horse_ebb_forces_pass():
    vb, inf, mkt, fst = _base_dims()
    row = DragonHeadScreening(
        ts_code="300001.SZ",
        trade_date="20260421",
        path=DragonPath.DARK_HORSE,
        cycle_phase=EmotionCyclePhase.EBB,
        value_boost=vb,
        outward_influence=inf,
        market_recognition=mkt,
        firstness=fst,
    )
    assert row.recommendation == "pass"


def test_fail_dimension_forces_pass():
    vb, inf, mkt, fst = _base_dims()
    bad = DimensionAssessment(score=3.0, verdict=Verdict.FAIL, rationale="无板块效应")
    row = DragonHeadScreening(
        ts_code="688001.SH",
        trade_date="20260421",
        path=DragonPath.DARK_HORSE,
        value_boost=vb,
        outward_influence=bad,
        market_recognition=mkt,
        firstness=fst,
    )
    assert row.recommendation == "pass"


def test_weights_must_sum_to_one():
    try:
        DragonHeadWeights(
            value_boost=0.5,
            outward_influence=0.5,
            market_recognition=0.25,
            firstness=0.20,
        )
    except ValueError:
        return
    raise AssertionError("expected ValueError")
