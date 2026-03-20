# TQ Chart Workbench

一个基于天勤 `tqsdk` 的期货图表工作台，后端做数据源抽象和指标计算，前端使用 `lightweight-charts` 渲染 K 线、成交量和自定义指标。当前项目已经支持把历史 tick 归档到本地 `DuckDB`，并直接从本地库回放 `Tick / Range / Renko / 时间 K 线`。

当前已经支持：

- 天勤行情拉取
- DuckDB 本地 tick 归档与回放
- 数据源抽象，方便后续切换别的数据源
- 合约切换
- 周期切换
- `lightweight-charts` 多面板展示
- 前端输入指标参数，后端实时计算
- 自定义指标动态加载
- 合约代码到可读名称的映射

## 界面示例



![界面示例](docs/image.png)



## 功能概览

- 主图显示 K 线和成交量
- 副图显示 MACD、STC 等指标
- 合约列表来自天勤合约目录，前端显示友好名称
- 指标参数在前端可编辑，修改后重新请求后端计算
- 前 200 根 K 线只参与计算，不参与显示，避免指标预热阶段把价格轴压坏
- 多图表时间范围联动
- 十字光标在 K 线和指标面板之间联动

## 项目结构

```text
.
├── web_tq_chart.py              # 启动入口
├── custom_indicators.py         # 你的自定义指标
├── custom_indicators.example.py # 自定义指标示例
├── tick_archive/                # DuckDB tick 归档与历史下载
├── scripts/                     # 独立脚本，例如 DuckDB 回灌
├── templates/
│   └── index.html               # 页面模板
├── static/
│   ├── app.js                   # 前端图表逻辑
│   └── styles.css               # 页面样式
└── tq_app/
    ├── web.py                   # Flask API
    ├── service.py               # 服务层，组织数据/指标/配置
    ├── contracts.py             # 合约目录与名称映射
    ├── models.py                # 指标与序列模型
    ├── data_sources/            # 数据源抽象层
    └── indicators/              # 内置指标与加载器
```

## 环境要求

- Python 3.11+
- 有效的天勤账号

## 安装

```bash
python -m venv myvenv
source myvenv/bin/activate
pip install -r requirements.txt
```

## 环境变量

项目根目录创建 `.env`：

```env
TQ_USER=你的天勤账号
TQ_PASSWORD=你的天勤密码
```

如果没有这两个变量，后端会在启动取数时报错。

如果你要使用本地 DuckDB 数据源，还可以额外配置：

```env
DUCKDB_TICK_DB_PATH=data/duckdb/ticks.duckdb
DUCKDB_SOURCE_PROVIDER=tq
```

默认情况下，不配置也会直接使用项目里的 [data/duckdb/ticks.duckdb](/home/bs/code/qh/tq/data/duckdb/ticks.duckdb)。

## 启动

默认启动：

```bash
./myvenv/bin/python web_tq_chart.py
```

默认地址：

```text
http://127.0.0.1:8050
```

如果你在 VS Code Remote / SSH / 容器环境里，希望更容易自动端口转发，可以显式监听所有地址：

```bash
./myvenv/bin/python web_tq_chart.py --host 0.0.0.0 --port 8050
```

启动后自动打开浏览器：

```bash
./myvenv/bin/python web_tq_chart.py --open-browser
```

## 常用启动参数

```bash
./myvenv/bin/python web_tq_chart.py \
  --symbol DCE.v2609 \
  --duration 300 \
  --length 800 \
  --refresh-ms 800 \
  --host 0.0.0.0 \
  --port 8050
```

参数说明：

- `--provider` 数据源名称，当前支持 `tq`、`duckdb`
- `--symbol` 默认合约，例如 `DCE.v2609`
- `--duration` K 线周期，单位秒
- `--length` 拉取 K 线数量
- `--refresh-ms` 刷新间隔，单位毫秒
- `--host` 监听地址
- `--port` 监听端口
- `--open-browser` 启动后自动打开浏览器

## 前端能力

页面左侧支持：

- 数据源切换 `tq / duckdb`
- 合约切换
- 周期切换
- 图表类型切换 `时间 K 线 / Tick / Range / Renko`
- 指标勾选
- 指标参数输入

页面右侧支持：

- K 线主图
- 成交量
- 多个指标副图
- 十字光标联动
- 光标时间显示
- `duckdb` 下显示本地数据覆盖范围、tick 数量、最小变动和合约月

## 内置指标

当前内置指标在 [tq_app/indicators/builtin.py](/home/bs/code/qh/tq/tq_app/indicators/builtin.py)：

- `ATR Bands`
- `MACD`
- `SMA 20`

项目根目录的 [custom_indicators.py](/home/bs/code/qh/tq/custom_indicators.py) 目前已经有：

- `EMA55`
- `STC`

## 自定义指标

系统会在启动时自动加载项目根目录的 `custom_indicators.py`。

要求：

- 文件里要有 `register_indicators(registry)` 函数
- 每个指标继承 `Indicator`
- 返回 `IndicatorResult`

最小示例可以参考 [custom_indicators.example.py](/home/bs/code/qh/tq/custom_indicators.example.py)。

### 带参数的自定义指标

如果你希望前端自动生成参数输入框，需要在 `meta.params` 里声明参数，例如：

```python
meta = IndicatorMeta(
    id="my_indicator",
    name="My Indicator",
    pane="indicator",
    description="示例指标",
    enabled_by_default=False,
    params=[
        {"key": "period", "label": "周期", "type": "int", "default": 20, "min": 1, "step": 1},
    ],
)
```

然后在 `build(self, bars, params=None)` 里读取：

```python
resolved = self.resolve_params(params)
period = resolved["period"]
```

前端会把参数发到后端，后端实时重新计算。

## 数据源抽象

当前数据源注册在 [tq_app/data_sources/registry.py](/home/bs/code/qh/tq/tq_app/data_sources/registry.py)。

当前内置数据源：

- `tq`
  使用天勤在线行情，适合实时刷新
- `duckdb`
  使用本地 DuckDB tick 库回放，适合历史分析、Range Bar、Renko 和脱机使用

如果后面要接别的数据源，思路是：

1. 在 `tq_app/data_sources/` 下新增一个 `DataSource` 实现
2. 在 `registry.py` 里注册新的 factory
3. 启动时通过 `--provider` 或默认配置切换

这样前端和指标层都不需要跟着改。

## DuckDB 本地存储

当前项目已经把本地持久化方案收口到 `DuckDB`，不再使用单独的 CSV 下载目录作为主存储。

主库位置：

- [data/duckdb/ticks.duckdb](/home/bs/code/qh/tq/data/duckdb/ticks.duckdb)

核心表：

- `market_ticks`
  存所有 tick 明细，按 `provider + symbol + ts_nano` 去重
- `contract_metadata`
  存合约映射、名称、到期月、最小变动、乘数等元信息

历史回灌脚本：

```bash
./myvenv/bin/python scripts/archive_ticks_duckdb.py \
  --start-date 2024-01-01 \
  --end-date 2026-03-20 \
  --exchange-id DCE \
  --product-id v
```

如果要直接从本地库启动页面：

```bash
./myvenv/bin/python web_tq_chart.py --provider duckdb --symbol DCE.v2405 --bar-mode tick
```

### DuckDB 常用查询

查看某个合约已经回灌到哪里：

```sql
SELECT
  symbol,
  MIN(ts) AS first_tick_at,
  MAX(ts) AS last_tick_at,
  COUNT(*) AS tick_count
FROM market_ticks
WHERE provider = 'tq' AND symbol = 'DCE.v2405'
GROUP BY symbol;
```

查看 `PVC` 合约当前有哪些本地数据：

```sql
SELECT
  symbol,
  first_tick_at,
  last_tick_at,
  tick_count
FROM (
  SELECT
    symbol,
    MIN(ts) AS first_tick_at,
    MAX(ts) AS last_tick_at,
    COUNT(*) AS tick_count
  FROM market_ticks
  WHERE provider = 'tq' AND symbol LIKE 'DCE.v%'
  GROUP BY symbol
)
ORDER BY symbol;
```

查看最近 10 条 tick：

```sql
SELECT
  ts,
  last_price,
  volume, 
  open_interest,
  bid_price1,
  bid_volume1,
  ask_price1,
  ask_volume1
FROM market_ticks
WHERE provider = 'tq' AND symbol = 'DCE.v2405'
ORDER BY ts DESC
LIMIT 10;
```

查看合约元信息：

```sql
SELECT
  symbol,
  instrument_name,
  label,
  exchange_id,
  product_id,
  price_tick,
  volume_multiple,
  contract_month,
  expire_datetime
FROM contract_metadata
WHERE provider = 'tq' AND symbol = 'DCE.v2405';
```

如果你想在命令行里直接查，可以这样：

```bash
./myvenv/bin/python - <<'PY'
import duckdb

conn = duckdb.connect("data/duckdb/ticks.duckdb", read_only=True)
try:
    rows = conn.execute("""
        SELECT symbol, MIN(ts), MAX(ts), COUNT(*)
        FROM market_ticks
        WHERE provider = 'tq' AND symbol LIKE 'DCE.v%'
        GROUP BY symbol
        ORDER BY symbol
    """).fetchall()
    for row in rows:
        print(row)
finally:
    conn.close()
PY
```

## API

### `GET /api/config`

返回：

- 当前数据源
- 默认合约
- 周期选项
- 合约列表
- 指标元信息
- 默认启用指标

### `GET /api/snapshot`

常用参数：

- `symbol`
- `duration_seconds`
- `indicators`
- `indicator_params`

示例：

```text
/api/snapshot?symbol=DCE.v2609&duration_seconds=300&indicators=macd,stc
```

## 显示策略说明

为了避免指标预热阶段的异常值影响显示，目前图表做了这些处理：

- 默认请求更长的历史数据用于指标计算
- 不再固定隐藏前 200 根，而是优先从指标已经形成有效值之后开始显示
- 初始视图优先显示指标已经算稳定后的区间
- 价格轴上下会额外留白

这能减少 ATR、STC 一类指标在预热阶段把主图压成一条横线的问题。

## 常见问题

### 1. 启动了但没有页面

先确认服务是否真的启动成功，再访问：

```text
http://127.0.0.1:8050
```

如果你在远程开发环境中：

- 检查 VS Code 的 `PORTS` 面板
- 优先使用 `--host 0.0.0.0`

### 2. 提示没有天勤账号

检查 `.env` 是否存在，并确认：

- `TQ_USER`
- `TQ_PASSWORD`

### 3. 页面刷新后看不到自定义指标

检查 `custom_indicators.py`：

- 是否存在 `register_indicators(registry)`
- 是否有语法错误
- 是否已经重启服务

## 开发建议

- 指标计算尽量保留预热阶段的 `NaN`，不要轻易 `fillna(0)`
- 新增价格类指标时，优先放 `pane="price"`
- 新增振荡类指标时，优先放 `pane="indicator"`
- 如果一个指标需要前端可调参数，统一通过 `meta.params` 描述

## 验证

前端语法检查：

```bash
node --check static/app.js
```

Python 编译检查：

```bash
./myvenv/bin/python -m compileall web_tq_chart.py tq_app custom_indicators.py
```

## 后续可扩展方向

- 增加新的数据源实现
- 合约搜索和分组筛选
- 指标参数防抖刷新
- 十字光标处显示 OHLC、成交量和指标值
- 指标信号标记
- WebSocket 推送而不是轮询

## License

本项目使用 MIT License，详见 [LICENSE](/home/bs/code/qh/tq/LICENSE)。
