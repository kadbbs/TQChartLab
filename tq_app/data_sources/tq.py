from __future__ import annotations

import os
import threading
import time

import pandas as pd
from dotenv import load_dotenv
from tqsdk import TqApi, TqAuth

from .base import DataSource

TICK_LOOKBACK_DAYS = 3
APPROX_TICKS_PER_DAY = 20000
MIN_TICK_SERIAL_LENGTH = 60000
MAX_TICK_SERIAL_LENGTH = 120000


class TqDataSource(DataSource):
    provider_name = "tq"

    def __init__(
        self,
        symbol: str,
        duration_seconds: int,
        data_length: int,
        refresh_ms: int,
        bar_mode: str,
        range_ticks: int,
    ) -> None:
        self.symbol = symbol
        self.duration_seconds = duration_seconds
        self.data_length = data_length
        self.refresh_ms = refresh_ms
        self.bar_mode = bar_mode
        self.range_ticks = range_ticks
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._bars: pd.DataFrame | None = None
        self._error: str | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="tq-data-source", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)

    def get_bars(self) -> pd.DataFrame:
        self._ready.wait(timeout=10)
        with self._lock:
            if self._error:
                raise RuntimeError(self._error)
            if self._bars is None or self._bars.empty:
                raise RuntimeError("行情数据还没准备好，请稍后刷新页面。")
            return self._bars.copy()

    def _run(self) -> None:
        api = None
        try:
            load_dotenv()
            user = os.getenv("TQ_USER")
            password = os.getenv("TQ_PASSWORD")
            if not user or not password:
                raise RuntimeError("请先在 .env 文件中设置 TQ_USER 和 TQ_PASSWORD。")

            api = TqApi(auth=TqAuth(user, password))
            quote = api.get_quote(self.symbol)
            if self.bar_mode in {"tick", "range", "renko"}:
                tick_length = self._tick_data_length()
                tick_serial = api.get_tick_serial(self.symbol, data_length=tick_length)
            else:
                klines = api.get_kline_serial(
                    self.symbol,
                    duration_seconds=self.duration_seconds,
                    data_length=self.data_length,
                )

            while not self._stop_event.is_set():
                api.wait_update(deadline=time.time() + self.refresh_ms / 1000)
                if self.bar_mode in {"tick", "range", "renko"}:
                    price_tick = float(getattr(quote, "price_tick", 0) or 0)
                    if self.bar_mode in {"range", "renko"} and price_tick <= 0:
                        raise RuntimeError(f"{self.symbol} 缺少有效的最小变动价位，无法构建 Range Bar。")
                    ticks = self._limit_ticks_to_recent_days(self._normalize_ticks(tick_serial))
                    if self.bar_mode == "tick":
                        bars = self._build_tick_bars(ticks)
                    elif self.bar_mode == "range":
                        bars = self._build_range_bars(ticks, price_tick, self.range_ticks)
                    else:
                        bars = self._build_renko_bars(ticks, price_tick, self.range_ticks)
                else:
                    bars = self._normalize_bars(klines)
                if not bars.empty:
                    with self._lock:
                        self._bars = bars
                        self._error = None
                    self._ready.set()
        except Exception as exc:
            with self._lock:
                self._error = str(exc)
            self._ready.set()
        finally:
            if api is not None:
                api.close()

    @staticmethod
    def _normalize_bars(klines: pd.DataFrame) -> pd.DataFrame:
        bars = klines.copy()
        bars["datetime"] = pd.to_datetime(bars["datetime"], unit="ns")
        bars = bars.dropna(subset=["open", "high", "low", "close"])
        if bars.empty:
            return bars
        bars = bars.sort_values("datetime")
        return bars.reset_index(drop=True)

    @staticmethod
    def _normalize_ticks(ticks: pd.DataFrame) -> pd.DataFrame:
        normalized = ticks.copy()
        normalized["datetime"] = pd.to_datetime(normalized["datetime"], unit="ns")
        normalized = normalized.dropna(subset=["last_price"])
        if normalized.empty:
            return normalized
        normalized = normalized.sort_values("datetime")
        return normalized.reset_index(drop=True)

    def _tick_data_length(self) -> int:
        target_ticks = TICK_LOOKBACK_DAYS * APPROX_TICKS_PER_DAY
        if self.bar_mode == "tick":
            return max(self.data_length, target_ticks, MIN_TICK_SERIAL_LENGTH)
        estimated = self.data_length * max(self.range_ticks, 4) * 12
        return max(MIN_TICK_SERIAL_LENGTH, min(max(estimated, target_ticks), MAX_TICK_SERIAL_LENGTH))

    @staticmethod
    def _limit_ticks_to_recent_days(ticks: pd.DataFrame) -> pd.DataFrame:
        if ticks.empty:
            return ticks
        latest_time = ticks.iloc[-1]["datetime"]
        cutoff_time = latest_time - pd.Timedelta(days=TICK_LOOKBACK_DAYS)
        recent_ticks = ticks.loc[ticks["datetime"] >= cutoff_time]
        if recent_ticks.empty:
            return ticks
        return recent_ticks.reset_index(drop=True)

    def _build_range_bars(self, ticks: pd.DataFrame, price_tick: float, range_ticks: int) -> pd.DataFrame:
        if ticks.empty:
            return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])

        range_size = price_tick * range_ticks
        if range_size <= 0:
            raise RuntimeError("Range Bar 的 tick 大小必须大于 0。")

        rows: list[dict[str, float | pd.Timestamp]] = []
        current_bar: dict[str, float | pd.Timestamp] | None = None
        prev_total_volume: float | None = None

        for row in ticks.itertuples(index=False):
            price = float(row.last_price)
            timestamp = pd.Timestamp(row.datetime)
            total_volume = float(getattr(row, "volume", 0) or 0)
            volume_delta = max(total_volume - prev_total_volume, 0) if prev_total_volume is not None else 0.0
            prev_total_volume = total_volume

            if current_bar is None:
                current_bar = {
                    "datetime": timestamp,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": volume_delta,
                }
            else:
                current_bar["high"] = max(float(current_bar["high"]), price)
                current_bar["low"] = min(float(current_bar["low"]), price)
                current_bar["close"] = price
                current_bar["volume"] = float(current_bar["volume"]) + volume_delta

            current_range = float(current_bar["high"]) - float(current_bar["low"])
            if current_range >= range_size:
                rows.append(current_bar.copy())
                current_bar = None

        if current_bar is not None:
            rows.append(current_bar.copy())

        if not rows:
            return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])

        bars = pd.DataFrame(rows)
        bars = bars.sort_values("datetime").tail(self.data_length)
        return bars.reset_index(drop=True)

    def _build_tick_bars(self, ticks: pd.DataFrame) -> pd.DataFrame:
        if ticks.empty:
            return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])

        rows: list[dict[str, float | pd.Timestamp]] = []
        prev_total_volume: float | None = None
        for row in ticks.itertuples(index=False):
            price = float(row.last_price)
            total_volume = float(getattr(row, "volume", 0) or 0)
            volume_delta = max(total_volume - prev_total_volume, 0) if prev_total_volume is not None else 0.0
            prev_total_volume = total_volume
            rows.append(
                {
                    "datetime": pd.Timestamp(row.datetime),
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": volume_delta,
                }
            )

        if not rows:
            return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])
        return pd.DataFrame(rows).reset_index(drop=True)

    def _build_renko_bars(self, ticks: pd.DataFrame, price_tick: float, range_ticks: int) -> pd.DataFrame:
        if ticks.empty:
            return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])

        brick_size = price_tick * range_ticks
        if brick_size <= 0:
            raise RuntimeError("Renko 的 tick 大小必须大于 0。")

        rows: list[dict[str, float | pd.Timestamp]] = []
        last_close = float(ticks.iloc[0]["last_price"])
        prev_total_volume: float | None = None
        pending_volume = 0.0

        for row in ticks.itertuples(index=False):
            price = float(row.last_price)
            total_volume = float(getattr(row, "volume", 0) or 0)
            volume_delta = max(total_volume - prev_total_volume, 0) if prev_total_volume is not None else 0.0
            prev_total_volume = total_volume
            pending_volume += volume_delta

            diff = price - last_close
            brick_count = int(abs(diff) // brick_size)
            if brick_count <= 0:
                continue

            direction = 1 if diff > 0 else -1
            volume_per_brick = pending_volume / brick_count if brick_count else 0.0

            for _ in range(brick_count):
                open_price = last_close
                close_price = open_price + direction * brick_size
                rows.append(
                    {
                        "datetime": pd.Timestamp(row.datetime),
                        "open": open_price,
                        "high": max(open_price, close_price),
                        "low": min(open_price, close_price),
                        "close": close_price,
                        "volume": volume_per_brick,
                    }
                )
                last_close = close_price

            pending_volume = 0.0

        if not rows:
            return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])

        bars = pd.DataFrame(rows)
        bars = bars.sort_values("datetime").tail(self.data_length)
        return bars.reset_index(drop=True)
