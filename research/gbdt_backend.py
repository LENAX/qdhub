"""
GBDT 训练后端：优先 LightGBM（与指南一致）；若导入失败或 macOS 缺少 libomp 等导致 OSError，
则回退到 sklearn.ensemble.HistGradientBoostingRegressor，保证研究管线可运行。
"""

from __future__ import annotations

from typing import Callable, Optional, Tuple

import numpy as np

TrainFn = Callable[..., Tuple[np.ndarray, np.ndarray]]


def _import_lightgbm():
    import lightgbm as lgb

    return lgb


def lightgbm_available() -> bool:
    try:
        _import_lightgbm()
    except (ImportError, OSError):
        return False
    return True


def fit_predict_gbdt_regression(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_valid: np.ndarray,
    y_valid: np.ndarray,
    X_test: np.ndarray,
    *,
    seed: int,
    lgb_num_rounds: int = 200,
    lgb_early_stopping: int = 20,
    lgb_num_leaves: int = 15,
    lgb_lr: float = 0.05,
) -> Tuple[np.ndarray, np.ndarray, str]:
    """
    返回 (pred_valid, pred_test, backend_name)，backend_name 为 'lightgbm' 或 'sklearn_hgbr'。
    """
    lgb = None
    try:
        lgb = _import_lightgbm()
    except (ImportError, OSError):
        pass

    if lgb is not None:
        train_set = lgb.Dataset(X_train, label=y_train)
        valid_set = lgb.Dataset(X_valid, label=y_valid, reference=train_set)
        params = {
            "objective": "regression",
            "metric": "rmse",
            "verbosity": -1,
            "seed": seed,
            "num_leaves": lgb_num_leaves,
            "learning_rate": lgb_lr,
        }
        booster = lgb.train(
            params,
            train_set,
            num_boost_round=lgb_num_rounds,
            valid_sets=[valid_set],
            callbacks=[lgb.early_stopping(stopping_rounds=lgb_early_stopping, verbose=False)],
        )
        return booster.predict(X_valid), booster.predict(X_test), "lightgbm"

    from sklearn.ensemble import HistGradientBoostingRegressor

    # 仅用训练集拟合，避免旧版 sklearn 参数差异；验证/测试仅用于评估
    model = HistGradientBoostingRegressor(
        max_iter=max(lgb_num_rounds, 100),
        max_depth=min(lgb_num_leaves // 2, 8),
        learning_rate=lgb_lr,
        random_state=seed,
    )
    model.fit(X_train, y_train)
    return model.predict(X_valid), model.predict(X_test), "sklearn_hgbr"
