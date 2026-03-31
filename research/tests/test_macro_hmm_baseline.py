"""宏观 HMM 基线：合成收益上拟合与样本外推断（无 mock，全真实数值计算）。"""

import numpy as np

from macro_hmm_baseline import (
    run_macro_hmm_baseline,
    synthetic_demo,
    _build_returns,
)


def test_synthetic_returns_has_two_regimes():
    r = _build_returns(200, seed=1)
    assert len(r) == 200
    assert np.std(r[:100]) < np.std(r[100:])


def test_run_macro_hmm_baseline_shapes_and_weights():
    r = _build_returns(250, seed=3)
    res = run_macro_hmm_baseline(r, train_ratio=0.6, n_states=2, seed=11)
    assert res.n_states == 2
    assert len(res.test_states) == 250 - int(250 * 0.6)
    assert len(res.test_position_weights) == len(res.test_states)
    assert np.all(np.isin(res.test_position_weights, [0.5, 1.0]))
    assert np.isfinite(res.train_log_prob)


def test_synthetic_demo_runs():
    r, res = synthetic_demo(seed=9)
    assert len(r) == 300
    assert res.test_position_weights.size > 0
