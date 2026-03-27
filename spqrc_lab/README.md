# SPQRC Lab

`SPQRC` 是当前项目里独立出来的一套研究型训练工程，目标不是替代现有实时图表，而是为后续真正的：

- path signature
- queue-reactive state
- conformal interval
- roughness control

提供一个可训练、可评估、可逐步接回前端的骨架。

## 当前实现

这一版是一个可落地的 MVP：

1. 从 `DuckDB tick` 构建 `500ms` 快照
2. 在每个 `5分钟` 决策点提取：
   - 路径效率 / 翻转率 / 粗糙度代理
   - 盘口不平衡 / microprice gap / dVol / dOI
   - 截断到二阶的离散 signature 特征
3. 生成状态标签：
   - `push_up`
   - `push_down`
   - `fade_up`
   - `fade_down`
   - `noise`
4. 训练：
   - 状态分类器：`GradientBoostingClassifier`
   - 区间回归：`GradientBoostingRegressor(loss="quantile")`
   - 简易 conformal 校准：基于 calibration residual 的滚动分位数

## 目录

- [features.py](/home/bs/code/qh/tq/spqrc_lab/features.py)
  负责 `500ms` 快照、路径特征、signature 特征、状态标签构建
- [train.py](/home/bs/code/qh/tq/spqrc_lab/train.py)
  负责训练和输出 `summary.json / predictions.csv`
- [run_spqrc_train.py](/home/bs/code/qh/tq/run_spqrc_train.py)
  命令行入口

## 运行

```bash
./myvenv/bin/python run_spqrc_train.py \
  --symbol DCE.v2609 \
  --start 2025-09-01 \
  --end 2026-03-20
```

输出默认会写到：

```text
spqrc_outputs/<symbol>/
```

主要结果：

- `summary.json`
- `predictions.csv`

## 当前边界

这还不是完整学术版：

- signature 目前是离散二阶截断实现
- queue-reactive 状态标签还是规则生成，不是人工标注或 EM/HMM 训练
- conformal 目前是简单 calibration quantile，不是完整 online weighted conformal
- roughness 目前是工程上可用的 proxy，不是完整 rough-vol 估计

但这套骨架的价值在于：

- 能开始训练
- 能对真实单品种 `5分钟` 数据做实验
- 后续可以逐块替换成更强的模型，而不需要推翻现有工程
