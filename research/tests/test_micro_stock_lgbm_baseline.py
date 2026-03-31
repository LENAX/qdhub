"""微观 GBDT：与 screening 族特征对齐的合成面板。"""

import numpy as np

from micro_stock_lgbm_baseline import (
    make_synthetic_stock_panel,
    run_micro_lgbm_baseline,
    time_based_split,
)


def test_micro_baseline_better_than_mean_predictor():
    df = make_synthetic_stock_panel(n_days=120, n_stocks=50, seed=8)
    res = run_micro_lgbm_baseline(df, train_frac=0.5, valid_frac=0.2, seed=100)
    assert res.backend in ("lightgbm", "sklearn_hgbr")
    assert res.metric_test > 0
    _, _, test = time_based_split(df, 0.5, 0.2)
    y = test["y"].to_numpy(float)
    naive = float(np.sqrt(np.mean((y - y.mean()) ** 2)))
    assert res.metric_test < naive * 0.99
