from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from tq_app.data_sources import DataSource, create_data_source, get_available_data_sources
from tq_app.indicators import build_indicator_registry
from tq_app.models import IndicatorMeta, IndicatorResult

TV_UP = "#089981"
TV_DOWN = "#f23645"


class MarketDataService:
    def __init__(
        self,
        provider: str,
        symbol: str,
        duration_seconds: int,
        data_length: int,
        refresh_ms: int,
        project_root: Path,
    ) -> None:
        self.provider = provider
        self.symbol = symbol
        self.duration_seconds = duration_seconds
        self.data_length = data_length
        self.refresh_ms = refresh_ms
        self.project_root = project_root
        self.data_source: DataSource = create_data_source(
            provider=provider,
            symbol=symbol,
            duration_seconds=duration_seconds,
            data_length=data_length,
            refresh_ms=refresh_ms,
        )
        self.indicators = build_indicator_registry(project_root)

    def start(self) -> None:
        self.data_source.start()

    def stop(self) -> None:
        self.data_source.stop()

    def get_config(self) -> dict[str, Any]:
        indicator_meta = [asdict(meta) for meta in self.indicators.list_meta()]
        return {
            "provider": self.provider,
            "providers": get_available_data_sources(),
            "symbol": self.symbol,
            "duration_seconds": self.duration_seconds,
            "data_length": self.data_length,
            "refresh_ms": self.refresh_ms,
            "indicators": indicator_meta,
            "default_indicator_ids": self.indicators.default_ids(),
        }

    def get_snapshot(
        self,
        indicator_ids: list[str] | None = None,
        indicator_params: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        bars = self.data_source.get_bars()
        normalized = self._with_chart_time(bars)
        selected = indicator_ids or self.indicators.default_ids()
        all_params = indicator_params or {}

        results: list[IndicatorResult] = []
        for indicator_id in selected:
            indicator = self.indicators.get(indicator_id)
            resolved_params = indicator.resolve_params(all_params.get(indicator_id))
            results.append(indicator.build(normalized, resolved_params))

        last_close = float(normalized.iloc[-1]["close"])
        prev_close = float(normalized.iloc[-2]["close"]) if len(normalized) > 1 else last_close

        return {
            "symbol": self.symbol,
            "provider": self.provider,
            "candles": self._serialize_candles(normalized),
            "volume": self._serialize_volume(normalized),
            "indicators": [self._serialize_indicator(item) for item in results],
            "last_close": last_close,
            "last_color": TV_UP if last_close >= prev_close else TV_DOWN,
            "last_time": normalized.iloc[-1]["datetime"].strftime("%Y-%m-%d %H:%M:%S"),
        }

    @staticmethod
    def _with_chart_time(bars: pd.DataFrame) -> pd.DataFrame:
        normalized = bars.copy()
        normalized["time"] = normalized["datetime"].astype("int64") // 10**9
        return normalized

    @staticmethod
    def _serialize_candles(df: pd.DataFrame) -> list[dict[str, Any]]:
        return [
            {
                "time": int(row.time),
                "open": float(row.open),
                "high": float(row.high),
                "low": float(row.low),
                "close": float(row.close),
            }
            for row in df[["time", "open", "high", "low", "close"]].itertuples(index=False)
        ]

    @staticmethod
    def _serialize_volume(df: pd.DataFrame) -> list[dict[str, Any]]:
        return [
            {
                "time": int(row.time),
                "value": float(row.volume),
                "color": TV_UP if row.close >= row.open else TV_DOWN,
            }
            for row in df[["time", "open", "close", "volume"]].itertuples(index=False)
        ]

    @staticmethod
    def _serialize_indicator(result: IndicatorResult) -> dict[str, Any]:
        return {
            "id": result.id,
            "name": result.name,
            "pane": result.pane,
            "series": [
                {
                    "id": series.id,
                    "name": series.name,
                    "pane": series.pane,
                    "series_type": series.series_type,
                    "data": series.data,
                    "options": series.options,
                }
                for series in result.series
            ],
        }
