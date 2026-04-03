from __future__ import annotations

import numpy as np
import pandas as pd

from tq_app.indicators import Indicator
from tq_app.models import IndicatorMeta, IndicatorResult, SeriesDefinition


def _line_point(time_value: int, value: float | None) -> dict[str, float | int]:
    if value is None or pd.isna(value):
        return {"time": int(time_value)}
    return {"time": int(time_value), "value": float(value)}


def _line_data(df: pd.DataFrame, column: str) -> list[dict[str, float | int | None]]:
    return [
        _line_point(int(row.time), row.value)
        for row in df[["time", column]].rename(columns={column: "value"}).itertuples(index=False)
    ]


def _colored_line_data(
    df: pd.DataFrame,
    value_column: str,
    trend_column: str,
    up_color: str,
    down_color: str,
) -> list[dict[str, float | int | str]]:
    points: list[dict[str, float | int | str]] = []

    for row in df[["time", value_column, trend_column]].itertuples(index=False):
        value = getattr(row, value_column)
        trend = getattr(row, trend_column)
        if pd.isna(value):
            points.append({"time": int(row.time)})
            continue

        color = up_color
        if not pd.isna(trend) and not bool(trend):
            color = down_color

        points.append(
            {
                "time": int(row.time),
                "value": float(value),
                "color": color,
            }
        )

    return points


def _state_colored_line_data(
    df: pd.DataFrame,
    value_column: str,
    up_color: str,
    down_color: str,
    neutral_color: str,
) -> list[dict[str, float | int | str]]:
    points: list[dict[str, float | int | str]] = []

    for row in df[["time", value_column]].itertuples(index=False):
        value = getattr(row, value_column)
        if pd.isna(value):
            points.append({"time": int(row.time)})
            continue

        if float(value) > 0:
            color = up_color
        elif float(value) < 0:
            color = down_color
        else:
            color = neutral_color

        points.append(
            {
                "time": int(row.time),
                "value": float(value),
                "color": color,
            }
        )

    return points


def _trend_line_data(
    df: pd.DataFrame,
    value_column: str,
    trend_column: str,
    bullish: bool,
) -> list[dict[str, float | int | None]]:
    points: list[dict[str, float | int | None]] = []

    for row in df[["time", value_column, trend_column]].itertuples(index=False):
        value = None if pd.isna(getattr(row, value_column)) else float(getattr(row, value_column))
        trend = bool(getattr(row, trend_column)) if not pd.isna(getattr(row, trend_column)) else None

        if value is None or trend is None:
            points.append(_line_point(int(row.time), None))
            continue

        points.append(_line_point(int(row.time), value if trend is bullish else None))

    return points

def _wma(series: pd.Series, period: int) -> pd.Series:
    safe_period = max(int(period), 1)
    weights = np.arange(1, safe_period + 1, dtype="float64")
    return series.rolling(safe_period).apply(
        lambda values: float(np.dot(values, weights) / weights.sum()),
        raw=True,
    )


def _ema(series: pd.Series, period: int) -> pd.Series:
    safe_period = max(int(period), 1)
    return series.ewm(span=safe_period, adjust=False).mean()


def _hma(series: pd.Series, period: int) -> pd.Series:
    safe_period = max(int(period), 1)
    half_length = max(safe_period // 2, 1)
    sqrt_length = max(int(safe_period**0.5), 1)
    base = 2 * _wma(series, half_length) - _wma(series, safe_period)
    return _wma(base, sqrt_length)


def _ehma(series: pd.Series, period: int) -> pd.Series:
    safe_period = max(int(period), 1)
    half_length = max(safe_period // 2, 1)
    sqrt_length = max(int(safe_period**0.5), 1)
    base = 2 * _ema(series, half_length) - _ema(series, safe_period)
    return _ema(base, sqrt_length)


def _thma(series: pd.Series, period: int) -> pd.Series:
    safe_period = max(int(period), 1)
    third_length = max(safe_period // 3, 1)
    half_length = max(safe_period // 2, 1)
    return _wma(3 * _wma(series, third_length) - _wma(series, half_length) - _wma(series, safe_period), safe_period)


def _source_series(df: pd.DataFrame, source: str) -> pd.Series:
    source_key = (source or "close").lower()
    if source_key in df.columns:
        return df[source_key]
    if source_key == "hl2":
        return (df["high"] + df["low"]) / 2
    if source_key == "hlc3":
        return (df["high"] + df["low"] + df["close"]) / 3
    if source_key == "ohlc4":
        return (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    return df["close"]


def _hull_mode(series: pd.Series, mode: str, period: int) -> pd.Series:
    normalized = (mode or "Hma").lower()
    safe_period = max(int(period), 1)
    if normalized == "ehma":
        return _ehma(series, safe_period)
    if normalized == "thma":
        return _thma(series, max(int(safe_period / 2), 1))
    return _hma(series, safe_period)


class Ema55Indicator(Indicator):
    meta = IndicatorMeta(
        id="ema55",
        name="EMA 55",
        pane="price",
        description="示例自定义指标: 55 周期指数均线。",
        enabled_by_default=False,
        params=[
            {"key": "period", "label": "周期", "type": "int", "default": 55, "min": 1, "step": 1},
        ],
    )

    def build(self, bars: pd.DataFrame, params: dict | None = None) -> IndicatorResult:
        resolved = self.resolve_params(params)
        period = resolved["period"]
        df = bars.copy()
        df["ema55"] = df["close"].ewm(span=period, adjust=False).mean()
        return IndicatorResult(
            id=self.meta.id,
            name=self.meta.name,
            pane=self.meta.pane,
            series=[
                SeriesDefinition(
                    id="ema55_line",
                    name=f"EMA({period})",
                    pane="price",
                    series_type="line",
                    data=[
                        {
                            "time": int(row.time),
                            "value": None if pd.isna(row.ema55) else float(row.ema55),
                        }
                        for row in df[["time", "ema55"]].itertuples(index=False)
                    ],
                    options={"color": "#7a5cff", "lineWidth": 2, "priceLineVisible": False},
                )
            ],
        )


class StcIndicator(Indicator):
    meta = IndicatorMeta(
        id="stc",
        name="STC",
        pane="indicator",
        description="Schaff Trend Cycle，默认参数 23/50/10，带 25/75 阈值线。",
        enabled_by_default=False,
        params=[
            {"key": "fast_period", "label": "快线", "type": "int", "default": 23, "min": 1, "step": 1},
            {"key": "slow_period", "label": "慢线", "type": "int", "default": 50, "min": 2, "step": 1},
            {"key": "cycle_period", "label": "周期", "type": "int", "default": 10, "min": 1, "step": 1},
            {"key": "smoothing_factor", "label": "平滑", "type": "float", "default": 0.5, "min": 0.01, "max": 1, "step": 0.01},
        ],
    )

    def build(self, bars: pd.DataFrame, params: dict | None = None) -> IndicatorResult:
        resolved = self.resolve_params(params)
        fast_period = resolved["fast_period"]
        slow_period = resolved["slow_period"]
        cycle_period = resolved["cycle_period"]
        smoothing_factor = resolved["smoothing_factor"]
        df = bars.copy()

        ema_fast = df["close"].ewm(span=fast_period, adjust=False).mean()
        ema_slow = df["close"].ewm(span=slow_period, adjust=False).mean()
        macd = ema_fast - ema_slow

        macd_low = macd.rolling(cycle_period).min()
        macd_high = macd.rolling(cycle_period).max()
        macd_range = (macd_high - macd_low).replace(0, pd.NA)
        stochastic_macd = ((macd - macd_low) / macd_range) * 100

        smooth_stochastic = stochastic_macd.ewm(alpha=smoothing_factor, adjust=False).mean()

        smooth_low = smooth_stochastic.rolling(cycle_period).min()
        smooth_high = smooth_stochastic.rolling(cycle_period).max()
        smooth_range = (smooth_high - smooth_low).replace(0, pd.NA)
        second_stochastic = ((smooth_stochastic - smooth_low) / smooth_range) * 100

        df["stc"] = second_stochastic.ewm(alpha=smoothing_factor, adjust=False).mean().clip(0, 100)
        df["stc_upper"] = 75.0
        df["stc_lower"] = 25.0

        return IndicatorResult(
            id=self.meta.id,
            name=self.meta.name,
            pane=self.meta.pane,
            series=[
                SeriesDefinition(
                    id="stc_line",
                    name=f"STC({fast_period},{slow_period},{cycle_period})",
                    pane="indicator",
                    series_type="line",
                    data=_line_data(df, "stc"),
                    options={"color": "#5b8def", "lineWidth": 2, "priceLineVisible": False},
                ),
                SeriesDefinition(
                    id="stc_upper",
                    name="Upper 75",
                    pane="indicator",
                    series_type="line",
                    data=_line_data(df, "stc_upper"),
                    options={
                        "color": "#d66a4e",
                        "lineWidth": 1,
                        "lineStyle": 2,
                        "priceLineVisible": False,
                        "lastValueVisible": False,
                    },
                ),
                SeriesDefinition(
                    id="stc_lower",
                    name="Lower 25",
                    pane="indicator",
                    series_type="line",
                    data=_line_data(df, "stc_lower"),
                    options={
                        "color": "#3aa675",
                        "lineWidth": 1,
                        "lineStyle": 2,
                        "priceLineVisible": False,
                        "lastValueVisible": False,
                    },
                ),
            ],
        )


class HullSuiteIndicator(Indicator):
    meta = IndicatorMeta(
        id="hull_suite",
        name="Hull Suite",
        pane="price",
        description="单线 Hull 指标，支持 HMA/EHMA/THMA，按趋势切换红绿颜色。",
        enabled_by_default=False,
        params=[
            {
                "key": "mode",
                "label": "变体",
                "type": "string",
                "default": "Hma",
                "options": ["Hma", "Ehma", "Thma"],
            },
            {"key": "length", "label": "周期", "type": "int", "default": 55, "min": 2, "step": 1},
            {"key": "length_mult", "label": "倍数", "type": "float", "default": 1.0, "min": 0.1, "step": 0.1},
            {
                "key": "source",
                "label": "价格源",
                "type": "string",
                "default": "close",
                "options": ["close", "open", "high", "low", "hl2", "hlc3", "ohlc4"],
            },
            {"key": "switch_color", "label": "趋势着色", "type": "bool", "default": True},
            {"key": "line_width", "label": "线宽", "type": "int", "default": 2, "min": 1, "max": 6, "step": 1},
        ],
    )

    def build(self, bars: pd.DataFrame, params: dict | None = None) -> IndicatorResult:
        resolved = self.resolve_params(params)
        mode = resolved["mode"]
        length = resolved["length"]
        length_mult = resolved["length_mult"]
        source = resolved["source"]
        switch_color = resolved["switch_color"]
        line_width = resolved["line_width"]
        df = bars.copy()
        hull_length = max(int(length * length_mult), 1)
        hull_source = _source_series(df, source)
        df["hull"] = _hull_mode(hull_source, mode, hull_length)
        df["hull_trend_up"] = df["hull"] >= df["hull"].shift(1)

        bullish_color = "#ff4d4f" if switch_color else "#ffffff"
        bearish_color = "#00a86b" if switch_color else "#ffffff"

        series: list[SeriesDefinition] = [
            SeriesDefinition(
                id="hull_suite_up",
                name=f"Hull Up({mode},{hull_length})",
                pane="price",
                series_type="line",
                data=_trend_line_data(df, "hull", "hull_trend_up", True),
                options={
                    "color": bullish_color,
                    "lineWidth": max(line_width + 1, 2),
                    "priceLineVisible": False,
                    "lastValueVisible": False,
                },
            ),
            SeriesDefinition(
                id="hull_suite_down",
                name=f"Hull Down({mode},{hull_length})",
                pane="price",
                series_type="line",
                data=_trend_line_data(df, "hull", "hull_trend_up", False),
                options={
                    "color": bearish_color,
                    "lineWidth": max(line_width + 1, 2),
                    "priceLineVisible": False,
                    "lastValueVisible": False,
                },
            ),
        ]

        return IndicatorResult(
            id=self.meta.id,
            name=self.meta.name,
            pane=self.meta.pane,
            series=series,
        )


class DuoKongLineIndicator(Indicator):
    meta = IndicatorMeta(
        id="duo_kong_line",
        name="多空线",
        pane="price",
        description="通达信风格 HULL 多空线，白色主线叠加红绿趋势段，并标注 多 / 空 信号。",
        enabled_by_default=True,
        params=[
            {
                "key": "mode",
                "label": "模式",
                "type": "int",
                "default": 1,
                "min": 1,
                "max": 3,
                "step": 1,
                "options": [1, 2, 3],
            },
            {"key": "length", "label": "周期", "type": "int", "default": 55, "min": 2, "step": 1},
            {
                "key": "source",
                "label": "价格源",
                "type": "string",
                "default": "close",
                "options": ["close", "open", "high", "low", "hl2", "hlc3", "ohlc4"],
            },
            {"key": "line_width", "label": "线宽", "type": "int", "default": 3, "min": 1, "max": 6, "step": 1},
            {"key": "show_signals", "label": "显示信号", "type": "bool", "default": True},
        ],
    )

    def build(self, bars: pd.DataFrame, params: dict | None = None) -> IndicatorResult:
        resolved = self.resolve_params(params)
        mode = resolved["mode"]
        length = resolved["length"]
        source = resolved["source"]
        line_width = resolved["line_width"]
        show_signals = resolved["show_signals"]

        df = bars.copy()
        hull_source = _source_series(df, source)
        if mode == 2:
            df["hull"] = _ehma(hull_source, length)
        elif mode == 3:
            df["hull"] = _thma(hull_source, length)
        else:
            df["hull"] = _hma(hull_source, length)

        previous_hull = df["hull"].shift(1)
        slope = df["hull"] - previous_hull
        df["trend_up"] = df["hull"] >= previous_hull
        df["buy_signal"] = (slope > 0) & (slope.shift(1) <= 0)
        df["sell_signal"] = (slope < 0) & (slope.shift(1) >= 0)

        markers: list[dict[str, str | int]] = []
        if show_signals:
            markers.extend(
                [
                    {
                        "time": int(row.time),
                        "position": "belowBar",
                        "color": "#ff4d4f",
                        "shape": "circle",
                        "size": 1,
                        "text": "多",
                    }
                    for row in df.loc[df["buy_signal"], ["time"]].itertuples(index=False)
                ]
            )
            markers.extend(
                [
                    {
                        "time": int(row.time),
                        "position": "aboveBar",
                        "color": "#00a86b",
                        "shape": "circle",
                        "size": 1,
                        "text": "空",
                    }
                    for row in df.loc[df["sell_signal"], ["time"]].itertuples(index=False)
                ]
            )

        series = [
            SeriesDefinition(
                id="duo_kong_line",
                name=f"多空线({length})",
                pane="price",
                series_type="line",
                data=_colored_line_data(df, "hull", "trend_up", "#e53935", "#00c853"),
                options={
                    "color": "#e53935",
                    "lineWidth": line_width,
                    "priceLineVisible": False,
                    "lastValueVisible": False,
                    "markers": markers,
                },
            )
        ]

        return IndicatorResult(
            id=self.meta.id,
            name=self.meta.name,
            pane=self.meta.pane,
            series=series,
        )


class OpenPermissionFilterIndicator(Indicator):
    meta = IndicatorMeta(
        id="open_permission_filter",
        name="开仓许可线",
        pane="indicator",
        description="基于波动率、趋势强度和位置过滤的开仓许可状态线。",
        enabled_by_default=False,
        params=[
            {"key": "vol_len", "label": "波动率周期", "type": "int", "default": 20, "min": 2, "step": 1},
            {"key": "vol_base_len", "label": "波动率基准", "type": "int", "default": 100, "min": 2, "step": 1},
            {"key": "vol_mult", "label": "波动率倍数", "type": "float", "default": 1.0, "min": 0.1, "step": 0.1},
            {"key": "fast_len", "label": "快均线", "type": "int", "default": 5, "min": 1, "step": 1},
            {"key": "slow_len", "label": "慢均线", "type": "int", "default": 20, "min": 2, "step": 1},
            {"key": "trend_threshold", "label": "趋势阈值", "type": "float", "default": 0.002, "min": 0.0, "step": 0.0001},
            {"key": "pos_len", "label": "位置窗口", "type": "int", "default": 50, "min": 2, "step": 1},
            {"key": "pos_upper", "label": "做多高位过滤", "type": "float", "default": 0.9, "min": 0.0, "max": 1.0, "step": 0.01},
            {"key": "pos_lower", "label": "做空低位过滤", "type": "float", "default": 0.1, "min": 0.0, "max": 1.0, "step": 0.01},
            {
                "key": "mode",
                "label": "过滤模式",
                "type": "string",
                "default": "both",
                "options": ["both", "long_only", "short_only"],
            },
        ],
    )

    def build(self, bars: pd.DataFrame, params: dict | None = None) -> IndicatorResult:
        resolved = self.resolve_params(params)
        vol_len = resolved["vol_len"]
        vol_base_len = resolved["vol_base_len"]
        vol_mult = resolved["vol_mult"]
        fast_len = resolved["fast_len"]
        slow_len = resolved["slow_len"]
        trend_threshold = resolved["trend_threshold"]
        pos_len = resolved["pos_len"]
        pos_upper = resolved["pos_upper"]
        pos_lower = resolved["pos_lower"]
        mode = resolved["mode"]

        df = bars.copy()
        df["ret"] = df["close"].pct_change()
        df["volatility"] = df["ret"].rolling(vol_len).std(ddof=0)
        df["vol_base"] = df["volatility"].rolling(vol_base_len).mean()
        df["vol_ok"] = df["volatility"] > (df["vol_base"] * vol_mult)

        df["ma_fast"] = df["close"].rolling(fast_len).mean()
        df["ma_slow"] = df["close"].rolling(slow_len).mean()
        df["trend_strength"] = (df["ma_fast"] - df["ma_slow"]).abs() / df["close"].replace(0, np.nan)
        df["trend_ok"] = df["trend_strength"] > trend_threshold

        df["hh"] = df["close"].rolling(pos_len).max()
        df["ll"] = df["close"].rolling(pos_len).min()
        price_span = (df["hh"] - df["ll"]).replace(0, np.nan)
        df["pos"] = (df["close"] - df["ll"]) / price_span
        df["pos"] = df["pos"].fillna(0.5)
        df["pos_ok_long"] = df["pos"] < pos_upper
        df["pos_ok_short"] = df["pos"] > pos_lower

        df["allow_long"] = df["vol_ok"] & df["trend_ok"] & df["pos_ok_long"]
        df["allow_short"] = df["vol_ok"] & df["trend_ok"] & df["pos_ok_short"]

        normalized_mode = (mode or "both").lower()
        if normalized_mode == "long_only":
            df["allow_open"] = df["allow_long"]
        elif normalized_mode == "short_only":
            df["allow_open"] = df["allow_short"]
        else:
            df["allow_open"] = df["allow_long"] | df["allow_short"]

        df["state_line"] = np.where(df["allow_open"], 1.0, 0.0)
        df["allow_level"] = 1.0
        df["deny_level"] = 0.0

        return IndicatorResult(
            id=self.meta.id,
            name=self.meta.name,
            pane=self.meta.pane,
            series=[
                SeriesDefinition(
                    id="open_permission_filter_state",
                    name="开仓许可线",
                    pane="indicator",
                    series_type="line",
                    data=_colored_line_data(df, "state_line", "allow_open", "#32d74b", "#ff4d4f"),
                    options={
                        "color": "#32d74b",
                        "lineWidth": 3,
                        "priceLineVisible": False,
                    },
                ),
                SeriesDefinition(
                    id="open_permission_filter_allow_level",
                    name="允许",
                    pane="indicator",
                    series_type="line",
                    data=_line_data(df, "allow_level"),
                    options={
                        "color": "#32d74b",
                        "lineWidth": 1,
                        "lineStyle": 2,
                        "priceLineVisible": False,
                        "lastValueVisible": False,
                    },
                ),
                SeriesDefinition(
                    id="open_permission_filter_deny_level",
                    name="禁止",
                    pane="indicator",
                    series_type="line",
                    data=_line_data(df, "deny_level"),
                    options={
                        "color": "#ff4d4f",
                        "lineWidth": 1,
                        "lineStyle": 2,
                        "priceLineVisible": False,
                        "lastValueVisible": False,
                    },
                ),
            ],
        )


class BreakoutCaptureIndicator(Indicator):
    meta = IndicatorMeta(
        id="breakout_capture",
        name="起爆捕捉逻辑",
        pane="indicator",
        description="EMA 快慢线 + 起爆多空信号 + 副图启动状态线。",
        enabled_by_default=False,
        params=[
            {
                "key": "source",
                "label": "价格源",
                "type": "string",
                "default": "close",
                "options": ["close", "open", "high", "low", "hl2", "hlc3", "ohlc4"],
            },
            {"key": "fast_len", "label": "快线周期", "type": "int", "default": 5, "min": 1, "step": 1},
            {"key": "slow_len", "label": "慢线周期", "type": "int", "default": 20, "min": 2, "step": 1},
            {"key": "vol_len", "label": "波动率周期", "type": "int", "default": 20, "min": 5, "step": 1},
            {"key": "vol_base_len", "label": "波动率基准", "type": "int", "default": 60, "min": 20, "step": 1},
            {"key": "vol_expand_mult", "label": "波动扩张倍数", "type": "float", "default": 1.2, "min": 0.1, "step": 0.05},
            {"key": "trend_lookback", "label": "趋势扩张回看", "type": "int", "default": 3, "min": 1, "step": 1},
            {"key": "trend_expand_mult", "label": "趋势扩张倍数", "type": "float", "default": 1.2, "min": 0.1, "step": 0.05},
            {"key": "slope_lookback", "label": "拐头回看", "type": "int", "default": 3, "min": 1, "step": 1},
            {"key": "pos_len", "label": "位置窗口", "type": "int", "default": 50, "min": 10, "step": 1},
            {"key": "long_pos_max", "label": "做多最高位置", "type": "float", "default": 0.80, "min": 0.0, "max": 1.0, "step": 0.01},
            {"key": "short_pos_min", "label": "做空最低位置", "type": "float", "default": 0.20, "min": 0.0, "max": 1.0, "step": 0.01},
            {"key": "use_chop_filter", "label": "启用震荡过滤", "type": "bool", "default": True},
            {"key": "ma_chop_threshold", "label": "均线缠绕阈值", "type": "float", "default": 0.0015, "min": 0.0, "step": 0.0001},
            {"key": "flip_lookback", "label": "切换回看", "type": "int", "default": 10, "min": 2, "step": 1},
            {"key": "flip_threshold", "label": "切换阈值", "type": "int", "default": 6, "min": 1, "step": 1},
            {"key": "range_window", "label": "压缩窗口", "type": "int", "default": 20, "min": 2, "step": 1},
            {"key": "range_small_threshold", "label": "压缩阈值", "type": "float", "default": 0.01, "min": 0.0, "step": 0.001},
            {"key": "use_vol_filter", "label": "启用波动过滤", "type": "bool", "default": True},
            {"key": "use_trend_filter", "label": "启用趋势过滤", "type": "bool", "default": True},
            {"key": "use_turn_filter", "label": "启用拐头过滤", "type": "bool", "default": True},
            {"key": "use_pos_filter", "label": "启用位置过滤", "type": "bool", "default": True},
            {"key": "show_signal", "label": "显示信号", "type": "bool", "default": True},
            {"key": "show_panel", "label": "显示副图", "type": "bool", "default": True},
            {"key": "show_debug", "label": "显示调试线", "type": "bool", "default": True},
        ],
    )

    def build(self, bars: pd.DataFrame, params: dict | None = None) -> IndicatorResult:
        resolved = self.resolve_params(params)
        source = resolved["source"]
        fast_len = resolved["fast_len"]
        slow_len = resolved["slow_len"]
        vol_len = resolved["vol_len"]
        vol_base_len = resolved["vol_base_len"]
        vol_expand_mult = resolved["vol_expand_mult"]
        trend_lookback = resolved["trend_lookback"]
        trend_expand_mult = resolved["trend_expand_mult"]
        slope_lookback = resolved["slope_lookback"]
        pos_len = resolved["pos_len"]
        long_pos_max = resolved["long_pos_max"]
        short_pos_min = resolved["short_pos_min"]
        use_chop_filter = resolved["use_chop_filter"]
        ma_chop_threshold = resolved["ma_chop_threshold"]
        flip_lookback = resolved["flip_lookback"]
        flip_threshold = resolved["flip_threshold"]
        range_window = resolved["range_window"]
        range_small_threshold = resolved["range_small_threshold"]
        use_vol_filter = resolved["use_vol_filter"]
        use_trend_filter = resolved["use_trend_filter"]
        use_turn_filter = resolved["use_turn_filter"]
        use_pos_filter = resolved["use_pos_filter"]
        show_signal = resolved["show_signal"]
        show_panel = resolved["show_panel"]
        show_debug = resolved["show_debug"]

        df = bars.copy()
        src = _source_series(df, source)
        df["ma_fast"] = _ema(src, fast_len)
        df["ma_slow"] = _ema(src, slow_len)

        df["ret"] = df["close"].pct_change()
        df["vol_now"] = df["ret"].rolling(vol_len).std(ddof=0)
        df["vol_base"] = df["vol_now"].rolling(vol_base_len).mean()
        df["vol_expand"] = df["vol_now"] > (df["vol_base"] * vol_expand_mult)

        df["trend_now"] = (df["ma_fast"] - df["ma_slow"]).abs()
        df["trend_prev"] = (df["ma_fast"].shift(trend_lookback) - df["ma_slow"].shift(trend_lookback)).abs()
        df["trend_expand"] = df["trend_now"] > (df["trend_prev"] * trend_expand_mult)

        df["fast_slope"] = df["ma_fast"] - df["ma_fast"].shift(slope_lookback)
        df["slow_slope"] = df["ma_slow"] - df["ma_slow"].shift(slope_lookback)
        df["turn_up"] = (df["fast_slope"] > 0) & (df["slow_slope"] >= 0)
        df["turn_down"] = (df["fast_slope"] < 0) & (df["slow_slope"] <= 0)

        df["dir_up"] = df["ma_fast"] > df["ma_slow"]
        df["dir_down"] = df["ma_fast"] < df["ma_slow"]

        df["ma_gap"] = (df["ma_fast"] - df["ma_slow"]).abs() / df["close"].replace(0, np.nan)
        df["ma_chop"] = df["ma_gap"] < ma_chop_threshold

        df["dir"] = np.where(df["close"] > df["close"].shift(1), 1, -1)
        df["flip"] = (df["dir"] != df["dir"].shift(1)).astype(int)
        df["flip_freq"] = df["flip"].rolling(flip_lookback).sum()
        df["choppy"] = df["flip_freq"] > flip_threshold

        df["range_high"] = df["high"].rolling(range_window).max()
        df["range_low"] = df["low"].rolling(range_window).min()
        df["range_small"] = ((df["range_high"] - df["range_low"]) / df["close"].replace(0, np.nan)) < range_small_threshold
        df["no_trade"] = df["ma_chop"] | df["choppy"] | df["range_small"]

        df["hh"] = df["close"].rolling(pos_len).max()
        df["ll"] = df["close"].rolling(pos_len).min()
        pos_span = (df["hh"] - df["ll"]).replace(0, np.nan)
        df["pos"] = ((df["close"] - df["ll"]) / pos_span).fillna(0.5)
        df["pos_ok_long"] = df["pos"] < long_pos_max
        df["pos_ok_short"] = df["pos"] > short_pos_min

        df["vol_ok_long"] = (~use_vol_filter) | df["vol_expand"]
        df["vol_ok_short"] = (~use_vol_filter) | df["vol_expand"]
        df["trend_ok_long"] = (~use_trend_filter) | df["trend_expand"]
        df["trend_ok_short"] = (~use_trend_filter) | df["trend_expand"]
        df["turn_ok_long"] = (~use_turn_filter) | df["turn_up"]
        df["turn_ok_short"] = (~use_turn_filter) | df["turn_down"]
        df["pos_ok_l"] = (~use_pos_filter) | df["pos_ok_long"]
        df["pos_ok_s"] = (~use_pos_filter) | df["pos_ok_short"]
        df["chop_ok"] = (~use_chop_filter) | (~df["no_trade"].fillna(False))

        df["setup_long"] = df["dir_up"] & df["vol_ok_long"] & df["trend_ok_long"] & df["turn_ok_long"] & df["pos_ok_l"] & df["chop_ok"]
        df["setup_short"] = df["dir_down"] & df["vol_ok_short"] & df["trend_ok_short"] & df["turn_ok_short"] & df["pos_ok_s"] & df["chop_ok"]
        df["long_signal"] = df["setup_long"] & ~df["setup_long"].shift(1).fillna(False)
        df["short_signal"] = df["setup_short"] & ~df["setup_short"].shift(1).fillna(False)
        df["state"] = np.where(df["setup_long"], 1.0, np.where(df["setup_short"], -1.0, 0.0))

        df["panel_up"] = 1.0
        df["panel_mid"] = 0.0
        df["panel_down"] = -1.0
        df["debug_vol_expand"] = np.where(df["vol_expand"], 0.6, 0.0)
        df["debug_trend_expand"] = np.where(df["trend_expand"], 0.3, 0.0)
        df["debug_turn"] = np.where(df["turn_up"], 0.15, np.where(df["turn_down"], -0.15, 0.0))
        df["debug_no_trade"] = np.where(df["no_trade"], -0.6, 0.0)

        markers: list[dict[str, str | int]] = []
        if show_signal:
            markers.extend(
                [
                    {
                        "time": int(row.time),
                        "position": "belowBar",
                        "color": "#32d74b",
                        "shape": "circle",
                        "size": 1,
                        "text": "多",
                    }
                    for row in df.loc[df["long_signal"], ["time"]].itertuples(index=False)
                ]
            )
            markers.extend(
                [
                    {
                        "time": int(row.time),
                        "position": "aboveBar",
                        "color": "#ff4d4f",
                        "shape": "circle",
                        "size": 1,
                        "text": "空",
                    }
                    for row in df.loc[df["short_signal"], ["time"]].itertuples(index=False)
                ]
            )

        series: list[SeriesDefinition] = [
            SeriesDefinition(
                id="breakout_capture_fast",
                name=f"快线 EMA({fast_len})",
                pane="price",
                series_type="line",
                data=_line_data(df, "ma_fast"),
                options={
                    "color": "#00c853",
                    "lineWidth": 2,
                    "priceLineVisible": False,
                    "markers": markers,
                },
            ),
            SeriesDefinition(
                id="breakout_capture_slow",
                name=f"慢线 EMA({slow_len})",
                pane="price",
                series_type="line",
                data=_line_data(df, "ma_slow"),
                options={
                    "color": "#ff4d4f",
                    "lineWidth": 2,
                    "priceLineVisible": False,
                },
            ),
        ]

        if show_panel:
            series.extend(
                [
                    SeriesDefinition(
                        id="breakout_capture_state",
                        name="启动状态",
                        pane="indicator",
                        series_type="line",
                        data=_state_colored_line_data(df, "state", "#32d74b", "#ff4d4f", "#888888"),
                        options={
                            "color": "#808080",
                            "lineWidth": 3,
                            "priceLineVisible": False,
                        },
                    ),
                    SeriesDefinition(
                        id="breakout_capture_state_up",
                        name="多启动",
                        pane="indicator",
                        series_type="line",
                        data=_line_data(df, "panel_up"),
                        options={"color": "#32d74b", "lineWidth": 1, "lineStyle": 2, "priceLineVisible": False, "lastValueVisible": False},
                    ),
                    SeriesDefinition(
                        id="breakout_capture_state_mid",
                        name="中性",
                        pane="indicator",
                        series_type="line",
                        data=_line_data(df, "panel_mid"),
                        options={"color": "#888888", "lineWidth": 1, "lineStyle": 1, "priceLineVisible": False, "lastValueVisible": False},
                    ),
                    SeriesDefinition(
                        id="breakout_capture_state_down",
                        name="空启动",
                        pane="indicator",
                        series_type="line",
                        data=_line_data(df, "panel_down"),
                        options={"color": "#ff4d4f", "lineWidth": 1, "lineStyle": 2, "priceLineVisible": False, "lastValueVisible": False},
                    ),
                ]
            )

            if show_debug:
                series.extend(
                    [
                        SeriesDefinition(
                            id="breakout_capture_debug_vol",
                            name="波动扩张",
                            pane="indicator",
                            series_type="line",
                            data=_line_data(df, "debug_vol_expand"),
                            options={"color": "#3b82f6", "lineWidth": 1, "priceLineVisible": False, "lastValueVisible": False},
                        ),
                        SeriesDefinition(
                            id="breakout_capture_debug_trend",
                            name="趋势扩张",
                            pane="indicator",
                            series_type="line",
                            data=_line_data(df, "debug_trend_expand"),
                            options={"color": "#f59e0b", "lineWidth": 1, "priceLineVisible": False, "lastValueVisible": False},
                        ),
                        SeriesDefinition(
                            id="breakout_capture_debug_turn",
                            name="均线拐头",
                            pane="indicator",
                            series_type="line",
                            data=_line_data(df, "debug_turn"),
                            options={"color": "#8b5cf6", "lineWidth": 1, "priceLineVisible": False, "lastValueVisible": False},
                        ),
                        SeriesDefinition(
                            id="breakout_capture_debug_no_trade",
                            name="震荡禁止区",
                            pane="indicator",
                            series_type="line",
                            data=_line_data(df, "debug_no_trade"),
                            options={"color": "#6b7280", "lineWidth": 1, "priceLineVisible": False, "lastValueVisible": False},
                        ),
                    ]
                )

        return IndicatorResult(
            id=self.meta.id,
            name=self.meta.name,
            pane=self.meta.pane,
            series=series,
        )


class ChopBreakoutStartIndicator(Indicator):
    meta = IndicatorMeta(
        id="chop_breakout_start",
        name="震荡破裂启动识别",
        pane="indicator",
        description="震荡区识别 + 突破后不回确认，副图用 1/0/-1 标识多空启动状态。",
        enabled_by_default=False,
        params=[
            {"key": "ma_fast_len", "label": "快线周期", "type": "int", "default": 5, "min": 1, "step": 1},
            {"key": "ma_slow_len", "label": "慢线周期", "type": "int", "default": 20, "min": 2, "step": 1},
            {"key": "box_len", "label": "震荡窗口", "type": "int", "default": 20, "min": 10, "step": 1},
            {"key": "range_thresh", "label": "区间阈值", "type": "float", "default": 0.012, "min": 0.0, "step": 0.001},
            {"key": "ma_gap_thresh", "label": "均线缠绕阈值", "type": "float", "default": 0.0015, "min": 0.0, "step": 0.0001},
            {"key": "break_buf", "label": "突破缓冲", "type": "float", "default": 0.0005, "min": 0.0, "step": 0.0001},
            {"key": "confirm_bars", "label": "确认根数", "type": "int", "default": 3, "min": 1, "step": 1},
            {"key": "use_body_break", "label": "收盘突破确认", "type": "bool", "default": True},
            {"key": "reset_bars", "label": "边界重置", "type": "int", "default": 40, "min": 1, "step": 1},
            {"key": "show_box", "label": "显示震荡边界", "type": "bool", "default": True},
            {"key": "show_signals", "label": "显示启动信号", "type": "bool", "default": True},
            {"key": "show_bg", "label": "显示震荡背景", "type": "bool", "default": True},
            {"key": "show_panel", "label": "显示副图状态线", "type": "bool", "default": True},
        ],
    )

    def build(self, bars: pd.DataFrame, params: dict | None = None) -> IndicatorResult:
        resolved = self.resolve_params(params)
        ma_fast_len = resolved["ma_fast_len"]
        ma_slow_len = resolved["ma_slow_len"]
        box_len = resolved["box_len"]
        range_thresh = resolved["range_thresh"]
        ma_gap_thresh = resolved["ma_gap_thresh"]
        break_buf = resolved["break_buf"]
        confirm_bars = resolved["confirm_bars"]
        use_body_break = resolved["use_body_break"]
        reset_bars = resolved["reset_bars"]
        show_box = resolved["show_box"]
        show_signals = resolved["show_signals"]
        show_panel = resolved["show_panel"]

        df = bars.copy()
        df["ma_fast"] = _ema(df["close"], ma_fast_len)
        df["ma_slow"] = _ema(df["close"], ma_slow_len)

        df["hh"] = df["high"].rolling(box_len).max()
        df["ll"] = df["low"].rolling(box_len).min()
        df["box_range"] = (df["hh"] - df["ll"]) / df["close"].replace(0, np.nan)
        df["ma_gap"] = (df["ma_fast"] - df["ma_slow"]).abs() / df["close"].replace(0, np.nan)
        df["is_range_small"] = df["box_range"] < range_thresh
        df["is_ma_tight"] = df["ma_gap"] < ma_gap_thresh
        df["in_chop"] = df["is_range_small"] & df["is_ma_tight"]

        chop_high_values: list[float | None] = []
        chop_low_values: list[float | None] = []
        had_chop_values: list[bool] = []
        upper_break_values: list[float | None] = []
        lower_break_values: list[float | None] = []
        up_count_values: list[int] = []
        dn_count_values: list[int] = []
        long_ready_values: list[bool] = []
        short_ready_values: list[bool] = []

        chop_high: float | None = None
        chop_low: float | None = None
        had_chop = False
        up_count = 0
        dn_count = 0
        bars_since_chop: int | None = None

        for row in df[
            ["in_chop", "hh", "ll", "high", "low", "close", "ma_fast", "ma_slow"]
        ].itertuples(index=False):
            in_chop = bool(row.in_chop) if not pd.isna(row.in_chop) else False
            hh = None if pd.isna(row.hh) else float(row.hh)
            ll = None if pd.isna(row.ll) else float(row.ll)
            high = None if pd.isna(row.high) else float(row.high)
            low = None if pd.isna(row.low) else float(row.low)
            close = None if pd.isna(row.close) else float(row.close)
            ma_fast = None if pd.isna(row.ma_fast) else float(row.ma_fast)
            ma_slow = None if pd.isna(row.ma_slow) else float(row.ma_slow)

            if in_chop:
                chop_high = hh if chop_high is None else max(chop_high, high if high is not None else chop_high)
                chop_low = ll if chop_low is None else min(chop_low, low if low is not None else chop_low)
                had_chop = True
                bars_since_chop = 0
            else:
                if bars_since_chop is None:
                    bars_since_chop = 1
                else:
                    bars_since_chop += 1
                if bars_since_chop > reset_bars:
                    chop_high = None
                    chop_low = None
                    had_chop = False

            upper_break_line = (chop_high * (1 + break_buf)) if had_chop and chop_high is not None else None
            lower_break_line = (chop_low * (1 - break_buf)) if had_chop and chop_low is not None else None

            if use_body_break:
                break_up_now = upper_break_line is not None and close is not None and close > upper_break_line
                break_down_now = lower_break_line is not None and close is not None and close < lower_break_line
            else:
                break_up_now = upper_break_line is not None and high is not None and high > upper_break_line
                break_down_now = lower_break_line is not None and low is not None and low < lower_break_line

            if break_up_now and had_chop:
                up_count += 1
            elif chop_high is not None and close is not None and close <= chop_high:
                up_count = 0

            if break_down_now and had_chop:
                dn_count += 1
            elif chop_low is not None and close is not None and close >= chop_low:
                dn_count = 0

            long_ready = bool(had_chop and chop_high is not None and up_count >= confirm_bars and ma_fast is not None and ma_slow is not None and ma_fast > ma_slow)
            short_ready = bool(had_chop and chop_low is not None and dn_count >= confirm_bars and ma_fast is not None and ma_slow is not None and ma_fast < ma_slow)

            chop_high_values.append(chop_high)
            chop_low_values.append(chop_low)
            had_chop_values.append(had_chop)
            upper_break_values.append(upper_break_line)
            lower_break_values.append(lower_break_line)
            up_count_values.append(up_count)
            dn_count_values.append(dn_count)
            long_ready_values.append(long_ready)
            short_ready_values.append(short_ready)

        df["chop_high"] = chop_high_values
        df["chop_low"] = chop_low_values
        df["had_chop"] = had_chop_values
        df["upper_break_line"] = upper_break_values
        df["lower_break_line"] = lower_break_values
        df["up_count"] = up_count_values
        df["dn_count"] = dn_count_values
        df["long_ready"] = long_ready_values
        df["short_ready"] = short_ready_values
        df["long_signal"] = df["long_ready"] & ~df["long_ready"].shift(1).fillna(False)
        df["short_signal"] = df["short_ready"] & ~df["short_ready"].shift(1).fillna(False)
        df["state"] = np.where(df["long_ready"], 1.0, np.where(df["short_ready"], -1.0, 0.0))
        df["panel_up"] = 1.0
        df["panel_mid"] = 0.0
        df["panel_down"] = -1.0

        markers: list[dict[str, str | int]] = []
        if show_signals:
            markers.extend(
                [
                    {
                        "time": int(row.time),
                        "position": "belowBar",
                        "color": "#32d74b",
                        "shape": "circle",
                        "size": 1,
                        "text": "多",
                    }
                    for row in df.loc[df["long_signal"], ["time"]].itertuples(index=False)
                ]
            )
            markers.extend(
                [
                    {
                        "time": int(row.time),
                        "position": "aboveBar",
                        "color": "#ff4d4f",
                        "shape": "circle",
                        "size": 1,
                        "text": "空",
                    }
                    for row in df.loc[df["short_signal"], ["time"]].itertuples(index=False)
                ]
            )

        series: list[SeriesDefinition] = [
            SeriesDefinition(
                id="chop_breakout_start_fast",
                name=f"快线 EMA({ma_fast_len})",
                pane="price",
                series_type="line",
                data=_line_data(df, "ma_fast"),
                options={
                    "color": "#00c853",
                    "lineWidth": 2,
                    "priceLineVisible": False,
                    "markers": markers,
                },
            ),
            SeriesDefinition(
                id="chop_breakout_start_slow",
                name=f"慢线 EMA({ma_slow_len})",
                pane="price",
                series_type="line",
                data=_line_data(df, "ma_slow"),
                options={
                    "color": "#ff4d4f",
                    "lineWidth": 2,
                    "priceLineVisible": False,
                },
            ),
        ]

        if show_box:
            series.extend(
                [
                    SeriesDefinition(
                        id="chop_breakout_start_box_high",
                        name="震荡上沿",
                        pane="price",
                        series_type="line",
                        data=_line_data(df, "chop_high"),
                        options={
                            "color": "#f59e0b",
                            "lineWidth": 2,
                            "lineStyle": 1,
                            "priceLineVisible": False,
                        },
                    ),
                    SeriesDefinition(
                        id="chop_breakout_start_box_low",
                        name="震荡下沿",
                        pane="price",
                        series_type="line",
                        data=_line_data(df, "chop_low"),
                        options={
                            "color": "#22d3ee",
                            "lineWidth": 2,
                            "lineStyle": 1,
                            "priceLineVisible": False,
                        },
                    ),
                ]
            )

        if show_panel:
            series.extend(
                [
                    SeriesDefinition(
                        id="chop_breakout_start_state",
                        name="1/0/-1 启动状态",
                        pane="indicator",
                        series_type="line",
                        data=_state_colored_line_data(df, "state", "#32d74b", "#ff4d4f", "#888888"),
                        options={
                            "color": "#808080",
                            "lineWidth": 3,
                            "priceLineVisible": False,
                        },
                    ),
                    SeriesDefinition(
                        id="chop_breakout_start_state_up",
                        name="1 多启动",
                        pane="indicator",
                        series_type="line",
                        data=_line_data(df, "panel_up"),
                        options={"color": "#32d74b", "lineWidth": 1, "lineStyle": 2, "priceLineVisible": False, "lastValueVisible": False},
                    ),
                    SeriesDefinition(
                        id="chop_breakout_start_state_mid",
                        name="0 中性",
                        pane="indicator",
                        series_type="line",
                        data=_line_data(df, "panel_mid"),
                        options={"color": "#888888", "lineWidth": 1, "lineStyle": 1, "priceLineVisible": False, "lastValueVisible": False},
                    ),
                    SeriesDefinition(
                        id="chop_breakout_start_state_down",
                        name="-1 空启动",
                        pane="indicator",
                        series_type="line",
                        data=_line_data(df, "panel_down"),
                        options={"color": "#ff4d4f", "lineWidth": 1, "lineStyle": 2, "priceLineVisible": False, "lastValueVisible": False},
                    ),
                ]
            )

        return IndicatorResult(
            id=self.meta.id,
            name=self.meta.name,
            pane=self.meta.pane,
            series=series,
        )


def register_indicators(registry) -> None:
    registry.register(Ema55Indicator())
    registry.register(StcIndicator())
    registry.register(HullSuiteIndicator())
    registry.register(DuoKongLineIndicator())
    registry.register(OpenPermissionFilterIndicator())
    registry.register(BreakoutCaptureIndicator())
    registry.register(ChopBreakoutStartIndicator())
