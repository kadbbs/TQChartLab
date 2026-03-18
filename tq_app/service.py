from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import threading
from typing import Any

import pandas as pd

from tq_app.contracts import format_contract_label, load_tq_contract_catalog
from tq_app.data_sources import DataSource, create_data_source, get_available_data_sources
from tq_app.indicators import build_indicator_registry
from tq_app.models import IndicatorMeta, IndicatorResult

TV_UP = "#089981"
TV_DOWN = "#f23645"
DEFAULT_DURATION_OPTIONS = [60, 300, 900, 1800, 3600, 86400]


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
        self._source_lock = threading.Lock()
        self._data_sources: dict[tuple[str, int], DataSource] = {}
        self.contracts = self._load_contracts()
        self._contract_map = {item["symbol"]: item for item in self.contracts}
        self.indicators = build_indicator_registry(project_root)

    def start(self) -> None:
        self._get_data_source(self.symbol, self.duration_seconds)

    def stop(self) -> None:
        with self._source_lock:
            data_sources = list(self._data_sources.values())
            self._data_sources.clear()
        for data_source in data_sources:
            data_source.stop()

    def get_config(self) -> dict[str, Any]:
        indicator_meta = [asdict(meta) for meta in self.indicators.list_meta()]
        return {
            "provider": self.provider,
            "providers": get_available_data_sources(),
            "symbol": self.symbol,
            "symbol_label": self._symbol_label(self.symbol),
            "duration_seconds": self.duration_seconds,
            "duration_options": DEFAULT_DURATION_OPTIONS,
            "data_length": self.data_length,
            "refresh_ms": self.refresh_ms,
            "contracts": self.contracts,
            "indicators": indicator_meta,
            "default_indicator_ids": self.indicators.default_ids(),
        }

    def get_snapshot(
        self,
        indicator_ids: list[str] | None = None,
        indicator_params: dict[str, dict[str, Any]] | None = None,
        symbol: str | None = None,
        duration_seconds: int | None = None,
    ) -> dict[str, Any]:
        effective_symbol = (symbol or self.symbol).strip()
        effective_duration = duration_seconds or self.duration_seconds
        bars = self._get_data_source(effective_symbol, effective_duration).get_bars()
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
            "symbol": effective_symbol,
            "symbol_label": self._symbol_label(effective_symbol),
            "provider": self.provider,
            "duration_seconds": effective_duration,
            "candles": self._serialize_candles(normalized),
            "volume": self._serialize_volume(normalized),
            "indicators": [self._serialize_indicator(item) for item in results],
            "last_close": last_close,
            "last_color": TV_UP if last_close >= prev_close else TV_DOWN,
            "last_time": normalized.iloc[-1]["datetime"].strftime("%Y-%m-%d %H:%M:%S"),
        }

    def _load_contracts(self) -> list[dict[str, Any]]:
        if self.provider != "tq":
            return []
        try:
            contracts = load_tq_contract_catalog(self.project_root)
        except Exception:
            contracts = []

        if any(item["symbol"] == self.symbol for item in contracts):
            return contracts

        return [
            {
                "symbol": self.symbol,
                "name": self.symbol,
                "label": format_contract_label(self.symbol),
                "exchange_id": "",
                "product_id": "",
            },
            *contracts,
        ]

    def _symbol_label(self, symbol: str) -> str:
        contract = self._contract_map.get(symbol)
        if contract:
            return str(contract["label"])
        return format_contract_label(symbol)

    def _get_data_source(self, symbol: str, duration_seconds: int) -> DataSource:
        if not symbol:
            raise ValueError("合约不能为空。")
        if duration_seconds <= 0:
            raise ValueError("周期必须大于 0 秒。")

        key = (symbol, duration_seconds)
        with self._source_lock:
            data_source = self._data_sources.get(key)
            if data_source is None:
                data_source = create_data_source(
                    provider=self.provider,
                    symbol=symbol,
                    duration_seconds=duration_seconds,
                    data_length=self.data_length,
                    refresh_ms=self.refresh_ms,
                )
                data_source.start()
                self._data_sources[key] = data_source
            return data_source

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
