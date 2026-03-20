from __future__ import annotations

from typing import Callable

from .base import DataSource
from .duckdb import DuckDBDataSource
from .tq import TqDataSource


DataSourceFactory = Callable[[str, int, int, int, int, str, int], DataSource]


def _build_tq(
    symbol: str,
    duration_seconds: int,
    data_length: int,
    brick_length: int,
    refresh_ms: int,
    bar_mode: str,
    range_ticks: int,
) -> DataSource:
    return TqDataSource(symbol, duration_seconds, data_length, brick_length, refresh_ms, bar_mode, range_ticks)


def _build_duckdb(
    symbol: str,
    duration_seconds: int,
    data_length: int,
    brick_length: int,
    refresh_ms: int,
    bar_mode: str,
    range_ticks: int,
) -> DataSource:
    return DuckDBDataSource(symbol, duration_seconds, data_length, brick_length, refresh_ms, bar_mode, range_ticks)


DATA_SOURCE_FACTORIES: dict[str, DataSourceFactory] = {
    "duckdb": _build_duckdb,
    "tq": _build_tq,
}


def create_data_source(
    provider: str,
    symbol: str,
    duration_seconds: int,
    data_length: int,
    brick_length: int,
    refresh_ms: int,
    bar_mode: str,
    range_ticks: int,
) -> DataSource:
    try:
        factory = DATA_SOURCE_FACTORIES[provider]
    except KeyError as exc:
        names = ", ".join(sorted(DATA_SOURCE_FACTORIES))
        raise ValueError(f"未知数据源: {provider}，可选值: {names}") from exc
    return factory(symbol, duration_seconds, data_length, brick_length, refresh_ms, bar_mode, range_ticks)


def get_available_data_sources() -> list[str]:
    return sorted(DATA_SOURCE_FACTORIES)
