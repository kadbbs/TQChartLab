from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from orderflow import merge_5m_pseudo_orderflow_into_bars

from .base import DataSource
from .transforms import build_range_bars, build_renko_bars, build_tick_bars, build_time_bars_from_ticks, normalize_ticks

DEFAULT_STORAGE_PROVIDER = "tq"
DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "data" / "duckdb" / "ticks.duckdb"
NATIVE_BAR_TABLES = {
    60: "market_bars_1m",
    300: "market_bars_5m",
    600: "market_bars_10m",
    900: "market_bars_15m",
}


class DuckDBDataSource(DataSource):
    provider_name = "duckdb"

    def __init__(
        self,
        symbol: str,
        duration_seconds: int,
        data_length: int,
        brick_length: int,
        refresh_ms: int,
        bar_mode: str,
        range_ticks: int,
    ) -> None:
        self.symbol = symbol
        self.duration_seconds = duration_seconds
        self.data_length = data_length
        self.brick_length = brick_length
        self.refresh_ms = refresh_ms
        self.bar_mode = bar_mode
        self.range_ticks = range_ticks
        self.enable_pseudo_orderflow = False
        self.source_provider = os.getenv("DUCKDB_SOURCE_PROVIDER", DEFAULT_STORAGE_PROVIDER).strip() or DEFAULT_STORAGE_PROVIDER
        configured_path = os.getenv("DUCKDB_TICK_DB_PATH", "").strip()
        self.db_path = Path(configured_path).expanduser() if configured_path else DEFAULT_DB_PATH
        self.conn: duckdb.DuckDBPyConnection | None = None
        self._cached_bars: pd.DataFrame | None = None
        self._cached_signature: tuple[Any, ...] | None = None

    def start(self) -> None:
        if self.conn is not None:
            return
        if not self.db_path.exists():
            raise RuntimeError(f"DuckDB 数据库不存在: {self.db_path}")
        self.conn = duckdb.connect(str(self.db_path), read_only=True)

    def stop(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def get_bars(self) -> pd.DataFrame:
        self.start()
        if self.conn is None:
            raise RuntimeError("DuckDB 连接未初始化。")

        signature = self._cache_signature()
        if self._cached_bars is not None and signature == self._cached_signature:
            return self._cached_bars.copy()

        if self.bar_mode == "time":
            bars = self._load_time_bars()
        else:
            ticks = self._load_ticks()
            if self.bar_mode == "tick":
                bars = build_tick_bars(ticks, data_length=self.data_length)
            else:
                price_tick = self._get_price_tick()
                if price_tick <= 0:
                    raise RuntimeError(f"{self.symbol} 缺少有效的 price_tick，无法构建 {self.bar_mode} 图。")
                if self.bar_mode == "range":
                    bars = build_range_bars(ticks, price_tick, self.range_ticks, self.brick_length)
                else:
                    bars = build_renko_bars(ticks, price_tick, self.range_ticks, self.brick_length)

        if bars.empty:
            raise RuntimeError(f"DuckDB 中暂无 {self.symbol} 的可用 {self.bar_mode} 数据。")
        normalized = bars.reset_index(drop=True)
        self._cached_bars = normalized.copy()
        self._cached_signature = signature
        return normalized.copy()

    def configure(self, **kwargs) -> None:
        enabled = kwargs.get("enable_pseudo_orderflow")
        if enabled is not None:
            self.enable_pseudo_orderflow = bool(enabled)

    def _preferred_time_source(self) -> str:
        if self._has_native_time_bars():
            return f"native_{self.duration_seconds}s"
        return "ticks"

    def _load_ticks(self) -> pd.DataFrame:
        if self.conn is None:
            raise RuntimeError("DuckDB 连接未初始化。")

        if self.bar_mode in {"range", "renko"}:
            frame = self.conn.execute(
                """
                SELECT ts AS datetime, last_price, volume
                FROM market_ticks
                WHERE provider = ? AND symbol = ?
                ORDER BY ts
                """,
                [self.source_provider, self.symbol],
            ).fetchdf()
        else:
            limit = self._raw_tick_limit()
            frame = self.conn.execute(
                """
                SELECT ts AS datetime, last_price, volume
                FROM market_ticks
                WHERE provider = ? AND symbol = ?
                ORDER BY ts DESC
                LIMIT ?
                """,
                [self.source_provider, self.symbol, limit],
            ).fetchdf()
        if frame.empty:
            return pd.DataFrame(columns=["datetime", "last_price", "volume"])
        if self.bar_mode != "range" and self.bar_mode != "renko":
            frame = frame.sort_values("datetime")
        frame = frame.reset_index(drop=True)
        return normalize_ticks(frame)

    def _load_time_bars(self) -> pd.DataFrame:
        if self.conn is None:
            raise RuntimeError("DuckDB 连接未初始化。")

        if self._has_native_time_bars():
            frame = self._load_native_time_bars()
        else:
            frame = self.conn.execute(
                """
                WITH ordered AS (
                    SELECT
                        ts,
                        last_price,
                        CASE
                            WHEN volume IS NULL THEN 0
                            ELSE GREATEST(volume - COALESCE(LAG(volume) OVER (ORDER BY ts), volume), 0)
                        END AS volume_delta,
                        FLOOR(epoch(ts) / ?) * ? AS bucket_epoch
                    FROM market_ticks
                    WHERE provider = ? AND symbol = ?
                ),
                bucketed AS (
                    SELECT
                        to_timestamp(bucket_epoch) AS datetime,
                        arg_min(last_price, ts) AS open,
                        max(last_price) AS high,
                        min(last_price) AS low,
                        arg_max(last_price, ts) AS close,
                        sum(volume_delta) AS volume
                    FROM ordered
                    GROUP BY bucket_epoch
                )
                SELECT datetime, open, high, low, close, volume
                FROM bucketed
                ORDER BY datetime DESC
                LIMIT ?
                """,
                [
                    self.duration_seconds,
                    self.duration_seconds,
                    self.source_provider,
                    self.symbol,
                    self.data_length,
                ],
            ).fetchdf()
            if frame.empty:
                return frame
            frame = frame.sort_values("datetime").reset_index(drop=True)
            frame["datetime"] = pd.to_datetime(frame["datetime"])

        if self.enable_pseudo_orderflow and self.duration_seconds == 300:
            ticks = self._load_ticks_for_time_window(
                frame.iloc[0]["datetime"],
                frame.iloc[-1]["datetime"] + pd.Timedelta(seconds=self.duration_seconds),
            )
            if not ticks.empty:
                frame = merge_5m_pseudo_orderflow_into_bars(frame, ticks)

        return frame

    def _native_bar_table_name(self) -> str | None:
        return NATIVE_BAR_TABLES.get(self.duration_seconds)

    def _has_native_time_bars(self) -> bool:
        if self.conn is None:
            raise RuntimeError("DuckDB 连接未初始化。")
        table_name = self._native_bar_table_name()
        if table_name is None:
            return False
        row = self.conn.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = ?
            LIMIT 1
            """,
            [table_name],
        ).fetchone()
        if row is None:
            return False

        count_row = self.conn.execute(
            f"""
            SELECT count(*)
            FROM {table_name}
            WHERE provider = ? AND symbol = ? AND duration_seconds = ?
            """,
            [self.source_provider, self.symbol, self.duration_seconds],
        ).fetchone()
        return bool(count_row and int(count_row[0] or 0) > 0)

    def _load_native_time_bars(self) -> pd.DataFrame:
        if self.conn is None:
            raise RuntimeError("DuckDB 连接未初始化。")
        table_name = self._native_bar_table_name()
        if table_name is None:
            return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])

        frame = self.conn.execute(
            f"""
            SELECT
                bar_start AS datetime,
                open,
                high,
                low,
                close,
                volume
            FROM {table_name}
            WHERE provider = ? AND symbol = ? AND duration_seconds = ?
            ORDER BY bar_start DESC
            LIMIT ?
            """,
            [self.source_provider, self.symbol, self.duration_seconds, self.data_length],
        ).fetchdf()
        if frame.empty:
            return frame
        frame = frame.sort_values("datetime").reset_index(drop=True)
        frame["datetime"] = pd.to_datetime(frame["datetime"])
        return frame

    def _get_price_tick(self) -> float:
        if self.conn is None:
            raise RuntimeError("DuckDB 连接未初始化。")
        row = self.conn.execute(
            """
            SELECT price_tick
            FROM contract_metadata
            WHERE provider = ? AND symbol = ?
            """,
            [self.source_provider, self.symbol],
        ).fetchone()
        if row is None or row[0] is None:
            return 0.0
        return float(row[0])

    def _load_ticks_for_time_window(self, start_dt: pd.Timestamp, end_dt: pd.Timestamp) -> pd.DataFrame:
        if self.conn is None:
            raise RuntimeError("DuckDB 连接未初始化。")
        frame = self.conn.execute(
            """
            SELECT
                ts AS datetime,
                last_price,
                volume,
                open_interest,
                bid_price1,
                bid_volume1,
                ask_price1,
                ask_volume1
            FROM market_ticks
            WHERE provider = ? AND symbol = ? AND ts >= ? AND ts < ?
            ORDER BY ts
            """,
            [self.source_provider, self.symbol, pd.Timestamp(start_dt), pd.Timestamp(end_dt)],
        ).fetchdf()
        if frame.empty:
            return frame
        frame["datetime"] = pd.to_datetime(frame["datetime"])
        return frame.reset_index(drop=True)

    def _raw_tick_limit(self) -> int:
        if self.bar_mode == "tick":
            return max(self.data_length, 5000)
        return max(20_000, min(self.brick_length * max(self.range_ticks, 4) * 80, 1_000_000))

    def _cache_signature(self) -> tuple[Any, ...]:
        stat = self.db_path.stat()
        return (
            stat.st_mtime_ns,
            stat.st_size,
            self.symbol,
            self.duration_seconds,
            self.data_length,
            self.brick_length,
            self.bar_mode,
            self.range_ticks,
            self.source_provider,
            self._preferred_time_source() if self.bar_mode == "time" else "",
        )
