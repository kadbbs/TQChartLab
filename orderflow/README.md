# 5分钟仿订单流

这是一个独立模块，用于把现有 `tick` 数据聚合成 `5分钟级仿订单流特征表`。

目标不是重建逐档逐秒真实盘口，而是给 `5分钟K线` 补一层内部质量分析特征。

## 当前输出的核心字段

- `buy_est_5m`
- `sell_est_5m`
- `delta_5m`
- `delta_ratio_5m`
- `cvd_5m`
- `cvd_change_5m`
- `cvd_slope_3`
- `imbalance_mean_5m`
- `imbalance_std_5m`
- `imbalance_close_5m`
- `microprice_bias_5m`
- `dOI_5m`
- `efficiency`
- `efficiency2`
- `return_per_delta`
- `return_per_volume`
- `oi_delta_confirm`
- `regime`

## 方向估计逻辑

因为你当前只有低频 tick，不做真实逐档成交重建，而是采用近似估计：

1. 价格上涨，记为主动买
2. 价格下跌，记为主动卖
3. 价格不变时，优先参考 `ask1 / bid1`
4. 再不行，参考 `mid / microprice`
5. 都不明显时，沿用上一笔方向

这更适合做 `5分钟级订单流画像`，不适合当逐档成交明细。

## 使用示例

```bash
./myvenv/bin/python run_orderflow_5m.py \
  --symbol DCE.v2609 \
  --start 2025-09-12 \
  --end 2026-03-20
```

默认输出到：

- `orderflow_outputs/<symbol>_pseudo_orderflow_5m.csv`
- `orderflow_outputs/<symbol>_pseudo_orderflow_5m.parquet`
