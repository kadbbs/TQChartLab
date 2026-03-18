from __future__ import annotations

import pandas as pd

from tq_app.indicators import Indicator
from tq_app.models import IndicatorMeta, IndicatorResult, SeriesDefinition


class Ema55Indicator(Indicator):
    meta = IndicatorMeta(
        id="ema55",
        name="EMA 55",
        pane="price",
        description="示例自定义指标: 55 周期指数均线。",
        enabled_by_default=False,
    )

    def build(self, bars: pd.DataFrame) -> IndicatorResult:
        df = bars.copy()
        df["ema55"] = df["close"].ewm(span=55, adjust=False).mean()
        return IndicatorResult(
            id=self.meta.id,
            name=self.meta.name,
            pane=self.meta.pane,
            series=[
                SeriesDefinition(
                    id="ema55_line",
                    name="EMA(55)",
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


def register_indicators(registry) -> None:
    registry.register(Ema55Indicator())
