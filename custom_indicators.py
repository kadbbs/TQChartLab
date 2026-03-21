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
        enabled_by_default=False,
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


def register_indicators(registry) -> None:
    registry.register(Ema55Indicator())
    registry.register(StcIndicator())
    registry.register(HullSuiteIndicator())
    registry.register(DuoKongLineIndicator())
