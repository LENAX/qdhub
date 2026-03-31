"""中观 GBDT（LightGBM 或 sklearn 回退）：合成板块面板上训练/验证/测试 RMSE。"""

import numpy as np

from meso_lgbm_sector import (
    make_synthetic_sector_panel,
    run_meso_lgbm_baseline,
    time_based_split,
)


def test_meso_baseline_better_than_mean_predictor():
    df = make_synthetic_sector_panel(n_days=150, n_sectors=20, seed=5)
    res = run_meso_lgbm_baseline(df, train_frac=0.5, valid_frac=0.2, seed=99)
    assert res.backend in ("lightgbm", "sklearn_hgbr")
    assert res.metric_valid > 0 and res.metric_test > 0
    _, _, test = time_based_split(df, 0.5, 0.2)
    y = test["y"].to_numpy(float)
    naive = float(np.sqrt(np.mean((y - y.mean()) ** 2)))
    assert res.metric_test < naive * 0.99
