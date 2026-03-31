# 交易模型研究基线（合成数据）

本目录实现 [《交易模型研究框架指南》](../doc/spec/trading-model-research-framework.md) 中的 **可运行最小示例**：

| 模块 | 说明 |
|------|------|
| `macro_hmm_baseline.py` | 指数（或合成）收益序列 → 高斯 HMM → 样本外状态与简单仓位权重 |
| `meso_lgbm_sector.py` | 板块横截面合成特征 → GBDT（**优先 LightGBM**，缺 libomp 等时回退 `sklearn` HGBR）→ 时间切分 RMSE |
| `micro_stock_lgbm_baseline.py` | 与 screening 口径接近的日频特征（MA/量/ATR/资金排名代理）→ 同上 |
| `gbdt_backend.py` | LightGBM / sklearn 统一训练入口 |

**依赖**：

```bash
pip install -r research/requirements.txt
```

**测试**（自仓库根目录）：

```bash
PYTHONPATH=research pytest research/tests -q
```

**说明**：数据均为 **合成**，用于验证管线与库版本；接入 QDHub 真实数据时，替换 `make_synthetic_*` 为 API/Parquet 读取，并遵守 [trading-model-research-pipeline.md](../doc/spec/trading-model-research-pipeline.md) 的对齐与防泄漏约定。
