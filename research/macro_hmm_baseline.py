"""
宏观 regime 基线：对一元收益序列拟合高斯 HMM，输出隐状态与简单仓位规则。

使用合成数据或外部拉取的指数收益；不包含任何实盘建议。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Tuple

import numpy as np
from hmmlearn import hmm


@dataclass(frozen=True)
class HMMBaselineResult:
    """样本内拟合 + 样本外状态推断结果。"""

    n_states: int
    train_log_prob: float
    train_states: np.ndarray
    test_states: np.ndarray
    test_position_weights: np.ndarray


def _build_returns(n: int, seed: int, vol_high: float = 0.02, vol_low: float = 0.005) -> np.ndarray:
    """两段式波动率合成日收益，便于 HMM 学到至少两种状态。"""
    rng = np.random.default_rng(seed)
    mid = n // 2
    r1 = rng.normal(0.0003, vol_low, size=mid)
    r2 = rng.normal(-0.0002, vol_high, size=n - mid)
    return np.concatenate([r1, r2])


def fit_gaussian_hmm_returns(
    train_returns: np.ndarray,
    n_states: int = 2,
    seed: int = 0,
    n_iter: int = 200,
) -> hmm.GaussianHMM:
    """
    用训练集收益拟合 1 维高斯发射 HMM。
    train_returns: shape (T,) 或 (T,1)
    """
    x = np.asarray(train_returns, dtype=float).reshape(-1, 1)
    model = hmm.GaussianHMM(
        n_components=n_states,
        covariance_type="full",
        n_iter=n_iter,
        random_state=seed,
    )
    model.fit(x)
    return model


def infer_states(model: hmm.GaussianHMM, returns: np.ndarray) -> np.ndarray:
    x = np.asarray(returns, dtype=float).reshape(-1, 1)
    _, states = model.decode(x, algorithm="viterbi")
    return np.asarray(states, dtype=int)


def position_from_states(
    states: np.ndarray,
    high_vol_state: int,
    *,
    w_low_vol: float = 1.0,
    w_high_vol: float = 0.5,
) -> np.ndarray:
    """
    简单风险预算：将「高波动」对应的状态映射为较低仓位权重。
    """
    w = np.where(states == high_vol_state, w_high_vol, w_low_vol)
    return w.astype(float)


def run_macro_hmm_baseline(
    returns: np.ndarray,
    train_ratio: float = 0.6,
    n_states: int = 2,
    seed: int = 42,
) -> HMMBaselineResult:
    """
    时间切分：前 train_ratio 训练，后段样本外推断状态与仓位权重。
    高波动状态 id 取训练段各状态收益方差较大者。
    """
    r = np.asarray(returns, dtype=float).reshape(-1)
    t = len(r)
    split = int(t * train_ratio)
    if split < 20 or t - split < 10:
        raise ValueError("序列太短：请增加长度或调整 train_ratio")

    train_r = r[:split]
    test_r = r[split:]

    model = fit_gaussian_hmm_returns(train_r, n_states=n_states, seed=seed)
    train_states = infer_states(model, train_r)
    test_states = infer_states(model, test_r)

    # 用训练段估计各状态收益方差，选方差最大的 state 为高波
    train_x = train_r.reshape(-1, 1)
    vars_ = []
    for s in range(n_states):
        mask = train_states == s
        if np.sum(mask) < 2:
            vars_.append(0.0)
        else:
            vars_.append(float(np.var(train_x[mask])))
    high_vol_state = int(np.argmax(vars_))

    train_lp = float(model.score(train_r.reshape(-1, 1)))
    w_test = position_from_states(test_states, high_vol_state)

    return HMMBaselineResult(
        n_states=n_states,
        train_log_prob=train_lp,
        train_states=train_states,
        test_states=test_states,
        test_position_weights=w_test,
    )


def synthetic_demo(seed: int = 7) -> Tuple[np.ndarray, HMMBaselineResult]:
    """默认合成序列 + 基线运行，供测试与 notebook。"""
    r = _build_returns(300, seed=seed)
    res = run_macro_hmm_baseline(r, train_ratio=0.6, n_states=2, seed=seed + 1)
    return r, res
