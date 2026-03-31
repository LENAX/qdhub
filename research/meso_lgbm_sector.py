"""
中观基线：板块横截面特征 + GBDT（优先 LightGBM）回归未来超额收益；按日期时间切分训练/验证/测试。

特征为合成占位（轮动强度、资金排名变化等）；接入实盘时替换为 QDHub 概念/资金接口特征。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

import numpy as np
import pandas as pd

from gbdt_backend import fit_predict_gbdt_regression


FEATURE_COLS = ["rot_momentum", "flow_rank_delta", "heat_z"]


@dataclass(frozen=True)
class MesoLGBMResult:
    train_dates: List[pd.Timestamp]
    valid_dates: List[pd.Timestamp]
    test_dates: List[pd.Timestamp]
    metric_valid: float
    metric_test: float
    feature_cols: Tuple[str, ...]
    backend: str


def make_synthetic_sector_panel(
    n_days: int = 120,
    n_sectors: int = 15,
    seed: int = 0,
) -> pd.DataFrame:
    """
    每日每板块一行：特征与标签由同一组随机数生成（**合成** 可学习关系）。
    实盘应对齐 pipeline 文档：仅用 t 及以前特征预测 t→t+H 收益；此处不模拟时间错位，仅验证 GBDT 管线。
    """
    rng = np.random.default_rng(seed)
    rows = []
    dates = pd.date_range("2020-01-01", periods=n_days, freq="B")
    for d in dates:
        for s in range(n_sectors):
            rot = rng.normal(0, 1)
            flow = rng.normal(0, 1)
            heat = rng.normal(0, 1)
            # 可预测部分 + 少量噪声（便于 GBDT 在样本外优于「预测均值」）
            y = 0.2 * rot + 0.15 * flow + 0.12 * heat + rng.normal(0, 0.01)
            rows.append(
                {
                    "date": d,
                    "sector_id": s,
                    "rot_momentum": rot,
                    "flow_rank_delta": flow,
                    "heat_z": heat,
                    "_y_raw": y,
                }
            )
    df = pd.DataFrame(rows)
    # 同一 (date, sector) 上，特征与标签由同一组随机数生成；y 表示「用当日可得特征解释的收益代理」
    df["y"] = df["_y_raw"]
    return df.drop(columns=["_y_raw"])


def time_based_split(
    df: pd.DataFrame,
    train_frac: float = 0.5,
    valid_frac: float = 0.2,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dates = sorted(df["date"].unique())
    n = len(dates)
    i1 = int(n * train_frac)
    i2 = int(n * (train_frac + valid_frac))
    d_train, d_valid, d_test = dates[:i1], dates[i1:i2], dates[i2:]
    train = df[df["date"].isin(d_train)]
    valid = df[df["date"].isin(d_valid)]
    test = df[df["date"].isin(d_test)]
    return train, valid, test


def _rmse(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.sqrt(np.mean((a - b) ** 2)))


def run_meso_lgbm_baseline(
    df: pd.DataFrame | None = None,
    *,
    train_frac: float = 0.5,
    valid_frac: float = 0.2,
    seed: int = 1,
) -> MesoLGBMResult:
    if df is None:
        df = make_synthetic_sector_panel(n_days=120, n_sectors=15, seed=seed)

    train, valid, test = time_based_split(df, train_frac=train_frac, valid_frac=valid_frac)
    X_train = train[FEATURE_COLS].to_numpy(dtype=float)
    y_train = train["y"].to_numpy(dtype=float)
    X_valid = valid[FEATURE_COLS].to_numpy(dtype=float)
    y_valid = valid["y"].to_numpy(dtype=float)
    X_test = test[FEATURE_COLS].to_numpy(dtype=float)
    y_test = test["y"].to_numpy(dtype=float)

    pred_v, pred_t, backend = fit_predict_gbdt_regression(
        X_train,
        y_train,
        X_valid,
        y_valid,
        X_test,
        seed=seed,
        lgb_num_rounds=200,
        lgb_early_stopping=20,
        lgb_num_leaves=15,
        lgb_lr=0.05,
    )
    metric_valid = _rmse(y_valid, pred_v)
    metric_test = _rmse(y_test, pred_t)

    return MesoLGBMResult(
        train_dates=sorted(train["date"].unique().tolist()),
        valid_dates=sorted(valid["date"].unique().tolist()),
        test_dates=sorted(test["date"].unique().tolist()),
        metric_valid=metric_valid,
        metric_test=metric_test,
        feature_cols=tuple(FEATURE_COLS),
        backend=backend,
    )
