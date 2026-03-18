from __future__ import annotations

import pandas as pd

from tq_app.indicators import Indicator
from tq_app.models import IndicatorMeta, IndicatorResult, SeriesDefinition


def _line_data(df: pd.DataFrame, column: str) -> list[dict[str, float | int | None]]:
    return [
        {
            "time": int(row.time),
            "value": None if pd.isna(row.value) else float(row.value),
        }
        for row in df[["time", column]].rename(columns={column: "value"}).itertuples(index=False)
    ]


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


def register_indicators(registry) -> None:
    registry.register(Ema55Indicator())
    registry.register(StcIndicator())
