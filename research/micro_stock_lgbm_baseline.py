"""
微观选股基线：与 screening 思路对齐的 **少量日频特征**（均线距离、量比、ATR 比、资金排名代理）+
GBDT（优先 LightGBM）回归未来超额收益。标签与特征均为合成数据，便于本地跑通管线。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

import numpy as np
import pandas as pd

from gbdt_backend import fit_predict_gbdt_regression


# 与 screening 文档一致的「族」：MA / 成交量 / ATR / 资金排名（代理）
MICRO_FEATURE_COLS = [
    "ma_distance",  # 收盘相对 MA 的相对距离
    "volume_ratio",  # 当日量 / 过去均量
    "atr_ratio",  # ATR / 收盘
    "moneyflow_rank_norm",  # 0-1 排名代理
]


@dataclass(frozen=True)
class MicroLGBMResult:
    train_dates: List[pd.Timestamp]
    valid_dates: List[pd.Timestamp]
    test_dates: List[pd.Timestamp]
    metric_valid: float
    metric_test: float
    feature_cols: Tuple[str, ...]
    backend: str


def make_synthetic_stock_panel(
    n_days: int = 100,
    n_stocks: int = 40,
    seed: int = 0,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    dates = pd.date_range("2020-03-01", periods=n_days, freq="B")
    for d in dates:
        for i in range(n_stocks):
            ma_d = rng.normal(0, 0.02)
            vol_r = rng.lognormal(0, 0.2)
            atr_r = rng.uniform(0.01, 0.05)
            mf = rng.uniform(0, 1)
            y = 0.8 * (0.5 - mf) + 0.5 * ma_d + 0.2 * (vol_r - 1.0) + rng.normal(0, 0.008)
            rows.append(
                {
                    "date": d,
                    "symbol_id": i,
                    "ma_distance": ma_d,
                    "volume_ratio": vol_r,
                    "atr_ratio": atr_r,
                    "moneyflow_rank_norm": mf,
                    "_y_raw": y,
                }
            )
    df = pd.DataFrame(rows)
    df["y"] = df["_y_raw"]
    return df.drop(columns=["_y_raw"])


def time_based_split(
    df: pd.DataFrame,
    train_frac: float = 0.5,
    valid_frac: float = 0.2,
):
    dates = sorted(df["date"].unique())
    n = len(dates)
    i1 = int(n * train_frac)
    i2 = int(n * (train_frac + valid_frac))
    d_train, d_valid, d_test = dates[:i1], dates[i1:i2], dates[i2:]
    train = df[df["date"].isin(d_train)]
    valid = df[df["date"].isin(d_valid)]
    test = df[df["date"].isin(d_test)]
    return train, valid, test


def _rmse(a, b) -> float:
    return float(np.sqrt(np.mean((a - b) ** 2)))


def run_micro_lgbm_baseline(
    df: pd.DataFrame | None = None,
    *,
    train_frac: float = 0.5,
    valid_frac: float = 0.2,
    seed: int = 2,
) -> MicroLGBMResult:
    if df is None:
        df = make_synthetic_stock_panel(n_days=100, n_stocks=40, seed=seed)

    train, valid, test = time_based_split(df, train_frac=train_frac, valid_frac=valid_frac)
    X_train = train[MICRO_FEATURE_COLS].to_numpy(dtype=float)
    y_train = train["y"].to_numpy(dtype=float)
    X_valid = valid[MICRO_FEATURE_COLS].to_numpy(dtype=float)
    y_valid = valid["y"].to_numpy(dtype=float)
    X_test = test[MICRO_FEATURE_COLS].to_numpy(dtype=float)
    y_test = test["y"].to_numpy(dtype=float)

    pred_v, pred_t, backend = fit_predict_gbdt_regression(
        X_train,
        y_train,
        X_valid,
        y_valid,
        X_test,
        seed=seed,
        lgb_num_rounds=300,
        lgb_early_stopping=30,
        lgb_num_leaves=31,
        lgb_lr=0.05,
    )

    return MicroLGBMResult(
        train_dates=sorted(train["date"].unique().tolist()),
        valid_dates=sorted(valid["date"].unique().tolist()),
        test_dates=sorted(test["date"].unique().tolist()),
        metric_valid=_rmse(y_valid, pred_v),
        metric_test=_rmse(y_test, pred_t),
        feature_cols=tuple(MICRO_FEATURE_COLS),
        backend=backend,
    )
