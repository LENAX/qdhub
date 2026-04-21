"""彭道富龙头战法：OpenClaw / Agent 结构化选股打分（Pydantic v2）。

与 `knowledge/彭道富龙头战法-OpenClaw选股体系.md` 对应；数值与结论仅供程序与人工复核，不构成投资建议。
"""

from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator, model_validator

# 破执：若命中以下标签，human/agent 应强烈倾向不参与或降级
CRITICAL_FALSE_PATTERN_FLAGS: frozenset[str] = frozenset(
    {
        "pure_hype_no_value_anchor",  # 纯情绪、无法落脚价值链条
        "no_sector_effect",  # 独涨、无板块效应
        "result_only_chasing",  # 仅以涨幅/连板结果追认龙头
    }
)


class DragonPath(str, Enum):
    """一花三叶：先定脉再打分。"""

    EQUITY = "equity"  # 股权 / 价值龙头
    WHITE_HORSE = "white_horse"  # 白马 / 价值投机、主升浪波段
    DARK_HORSE = "dark_horse"  # 黑马 / 盘面人气与情绪


class EmotionCyclePhase(str, Enum):
    """情绪周期（可选）：用于约束黑马脉的参与窗口。"""

    UNKNOWN = "unknown"
    START = "start"  # 启动
    FERMENT = "ferment"  # 发酵
    CLIMAX = "climax"  # 高潮
    EBB = "ebb"  # 退潮


class Verdict(str, Enum):
    """单维结论：通过 / 待验证 / 不通过。"""

    PASS = "pass"
    PENDING = "pending"
    FAIL = "fail"


class DimensionAssessment(BaseModel):
    """龙头四维之一：分数 + 结论 + 依据。"""

    model_config = ConfigDict(str_strip_whitespace=True)

    score: float = Field(ge=0.0, le=10.0, description="0～10 分，建议整数或半档")
    verdict: Verdict
    rationale: str = Field(default="", max_length=4000)
    qdhub_evidence: list[str] = Field(
        default_factory=list,
        description="来自 QDHub 的短证据链，如接口字段、日期、数值",
    )

    @field_validator("qdhub_evidence")
    @classmethod
    def _cap_evidence_items(cls, v: list[str]) -> list[str]:
        if len(v) > 32:
            raise ValueError("qdhub_evidence 最多 32 条，请合并摘要")
        return v


class DragonHeadWeights(BaseModel):
    """四维权重：默认略抬高「价值加持」占比，与彭氏强调一致；总和须为 1。"""

    model_config = ConfigDict(frozen=True)

    value_boost: float = Field(default=0.30, ge=0.0, le=1.0, description="价值加持")
    outward_influence: float = Field(default=0.25, ge=0.0, le=1.0, description="对外影响力")
    market_recognition: float = Field(default=0.25, ge=0.0, le=1.0, description="市场性")
    firstness: float = Field(default=0.20, ge=0.0, le=1.0, description="第一性")

    @model_validator(mode="after")
    def _sum_to_one(self) -> DragonHeadWeights:
        s = (
            self.value_boost
            + self.outward_influence
            + self.market_recognition
            + self.firstness
        )
        if abs(s - 1.0) > 1e-6:
            raise ValueError(f"四维权重之和须为 1，当前为 {s}")
        return self


class DragonHeadScreening(BaseModel):
    """单次标的筛查结果：输入标识 + 四维 + 破执标签 + 衍生建议。"""

    model_config = ConfigDict(str_strip_whitespace=True)

    ts_code: str = Field(min_length=8, max_length=16, description="如 600519.SH")
    trade_date: str = Field(min_length=8, max_length=8, pattern=r"^\d{8}$")
    path: DragonPath
    cycle_phase: EmotionCyclePhase = EmotionCyclePhase.UNKNOWN

    value_boost: DimensionAssessment = Field(description="价值加持")
    outward_influence: DimensionAssessment = Field(description="对外影响力")
    market_recognition: DimensionAssessment = Field(description="市场性")
    firstness: DimensionAssessment = Field(description="第一性")

    weights: DragonHeadWeights = Field(default_factory=DragonHeadWeights)
    false_pattern_flags: list[str] = Field(
        default_factory=list,
        description="破执过滤标签，如 no_sector_effect、result_only_chasing",
    )
    notes: list[str] = Field(default_factory=list, max_length=16, description="人工/Agent 备注")

    @field_validator("false_pattern_flags")
    @classmethod
    def _cap_flags(cls, v: list[str]) -> list[str]:
        if len(v) > 24:
            raise ValueError("false_pattern_flags 过多，请合并")
        return v

    @computed_field
    @property
    def weighted_score(self) -> float:
        w = self.weights
        return (
            self.value_boost.score * w.value_boost
            + self.outward_influence.score * w.outward_influence
            + self.market_recognition.score * w.market_recognition
            + self.firstness.score * w.firstness
        )

    @computed_field
    @property
    def has_critical_false_pattern(self) -> bool:
        return bool(CRITICAL_FALSE_PATTERN_FLAGS.intersection(self.false_pattern_flags))

    @computed_field
    @property
    def any_dimension_fail(self) -> bool:
        return any(
            d.verdict == Verdict.FAIL
            for d in (
                self.value_boost,
                self.outward_influence,
                self.market_recognition,
                self.firstness,
            )
        )

    @computed_field
    @property
    def all_dimensions_pass(self) -> bool:
        return all(
            d.verdict == Verdict.PASS
            for d in (
                self.value_boost,
                self.outward_influence,
                self.market_recognition,
                self.firstness,
            )
        )

    @computed_field
    @property
    def recommendation(self) -> Literal["strong", "watch", "pass"]:
        """保守、可自动化：强信号要求四维全过、无关键破执、加权分与周期过关。"""
        if self.has_critical_false_pattern or self.any_dimension_fail:
            return "pass"
        if self.path == DragonPath.DARK_HORSE and self.cycle_phase == EmotionCyclePhase.EBB:
            return "pass"
        if self.all_dimensions_pass and self.weighted_score >= 7.5:
            return "strong"
        if self.weighted_score >= 5.0 and not self.any_dimension_fail:
            return "watch"
        return "pass"

    def to_summary_dict(self) -> dict[str, object]:
        """便于日志与 LLM 上下文：扁平、短键。"""
        return {
            "ts_code": self.ts_code,
            "trade_date": self.trade_date,
            "path": self.path,
            "cycle_phase": self.cycle_phase,
            "weighted_score": round(self.weighted_score, 4),
            "recommendation": self.recommendation,
            "all_pass": self.all_dimensions_pass,
            "critical_flag": self.has_critical_false_pattern,
            "flags": list(self.false_pattern_flags),
        }
