from __future__ import annotations

import os
import threading
import time

import pandas as pd
from dotenv import load_dotenv
from tqsdk import TqApi, TqAuth

from orderflow import IncrementalPseudoOrderflow5m, merge_5m_pseudo_orderflow_into_bars

from .base import DataSource
from .transforms import build_range_bars, build_renko_bars, build_tick_bars, normalize_bars, normalize_ticks

TICK_LOOKBACK_DAYS = 10
APPROX_TICKS_PER_DAY = 80000
MIN_TICK_SERIAL_LENGTH = 120000
MAX_TICK_SERIAL_LENGTH = 500000
PSEUDO_ORDERFLOW_MAX_BARS = 240
APPROX_TICKS_PER_5M_BAR = 800
PSEUDO_ORDERFLOW_TICK_LENGTH = 50000


class TqDataSource(DataSource):
    provider_name = "tq"

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
        self._orderflow_enabled_at: pd.Timestamp | None = None
        self._orderflow_state = IncrementalPseudoOrderflow5m(max_bars=PSEUDO_ORDERFLOW_MAX_BARS)
        self._orderflow_session_active = False
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

    def configure(self, **kwargs) -> None:
        enabled = kwargs.get("enable_pseudo_orderflow")
        if enabled is not None:
            enabled = bool(enabled)
            if enabled and not self.enable_pseudo_orderflow:
                self._orderflow_enabled_at = pd.Timestamp.now(tz="Asia/Shanghai").tz_localize(None)
                self._orderflow_session_active = False
            if not enabled:
                self._orderflow_enabled_at = None
                self._orderflow_session_active = False
                self._orderflow_state.reset()
            self.enable_pseudo_orderflow = enabled

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
            tick_serial = None
            orderflow_tick_serial = None
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
                if self._pseudo_orderflow_enabled() and orderflow_tick_serial is None:
                    orderflow_tick_serial = api.get_tick_serial(self.symbol, data_length=PSEUDO_ORDERFLOW_TICK_LENGTH)
                if (not self._pseudo_orderflow_enabled()) and orderflow_tick_serial is not None:
                    orderflow_tick_serial = None
                if self.bar_mode in {"tick", "range", "renko"}:
                    price_tick = float(getattr(quote, "price_tick", 0) or 0)
                    if self.bar_mode in {"range", "renko"} and price_tick <= 0:
                        raise RuntimeError(f"{self.symbol} 缺少有效的最小变动价位，无法构建 Range Bar。")
                    ticks = self._limit_ticks_to_recent_days(normalize_ticks(tick_serial))
                    if self.bar_mode == "tick":
                        bars = build_tick_bars(ticks, data_length=self.data_length)
                    elif self.bar_mode == "range":
                        bars = build_range_bars(ticks, price_tick, self.range_ticks, self.brick_length)
                    else:
                        bars = build_renko_bars(ticks, price_tick, self.range_ticks, self.brick_length)
                else:
                    bars = normalize_bars(klines)
                    if self._pseudo_orderflow_enabled() and orderflow_tick_serial is not None and not bars.empty:
                        bars = self._merge_incremental_orderflow(bars, orderflow_tick_serial)
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

    def _tick_data_length(self) -> int:
        target_ticks = TICK_LOOKBACK_DAYS * APPROX_TICKS_PER_DAY
        if self.bar_mode == "tick":
            return max(self.data_length, target_ticks, MIN_TICK_SERIAL_LENGTH)
        if self.bar_mode == "time" and self.duration_seconds == 300:
            target_bars = min(self.data_length, PSEUDO_ORDERFLOW_MAX_BARS)
            estimated = target_bars * APPROX_TICKS_PER_5M_BAR
            return max(40_000, min(estimated, 200_000))
        estimated = self.brick_length * max(self.range_ticks, 4) * 12
        return max(MIN_TICK_SERIAL_LENGTH, min(max(estimated, target_ticks), MAX_TICK_SERIAL_LENGTH))

    def _pseudo_orderflow_enabled(self) -> bool:
        return self.enable_pseudo_orderflow and self.bar_mode == "time" and self.duration_seconds == 300

    def _merge_incremental_orderflow(self, bars: pd.DataFrame, tick_serial: pd.DataFrame) -> pd.DataFrame:
        orderflow_bars = bars.tail(min(len(bars), PSEUDO_ORDERFLOW_MAX_BARS)).copy()
        if orderflow_bars.empty:
            return bars

        raw_ticks = tick_serial.copy()
        raw_ticks = raw_ticks.dropna(subset=["id", "datetime", "last_price"])
        raw_ticks["id"] = pd.to_numeric(raw_ticks["id"], errors="coerce")
        raw_ticks = raw_ticks.dropna(subset=["id"])

        if not self._orderflow_session_active:
            if raw_ticks.empty:
                return bars
            self._orderflow_state.reset()
            self._orderflow_state.set_last_tick_id(float(raw_ticks["id"].max()))
            self._orderflow_session_active = True
            return bars

        last_tick_id = self._orderflow_state.last_tick_id
        if last_tick_id is not None:
            raw_ticks = raw_ticks.loc[raw_ticks["id"] > last_tick_id]
        if raw_ticks.empty:
            return self._merge_orderflow_frame_into_bars(bars, self._orderflow_state.to_frame())

        ticks = normalize_ticks(raw_ticks)
        if self._orderflow_enabled_at is not None:
            ticks = ticks.loc[ticks["datetime"] >= self._orderflow_enabled_at]
        end_dt = pd.Timestamp(orderflow_bars.iloc[-1]["datetime"]) + pd.Timedelta(seconds=self.duration_seconds)
        start_dt = pd.Timestamp(orderflow_bars.iloc[0]["datetime"])
        ticks = self._filter_ticks_for_bars(ticks, start_dt, end_dt)
        if not ticks.empty:
            self._orderflow_state.update(ticks)
        return self._merge_orderflow_frame_into_bars(bars, self._orderflow_state.to_frame())

    @staticmethod
    def _merge_orderflow_frame_into_bars(bars: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
        if features.empty:
            return bars
        flag_columns = [column for column in features.columns if column not in bars.columns]
        if not flag_columns:
            return bars
        return bars.merge(features[["datetime", *flag_columns]], on="datetime", how="left")

    @staticmethod
    def _filter_ticks_for_bars(
        ticks: pd.DataFrame,
        start_dt: pd.Timestamp,
        end_dt: pd.Timestamp,
    ) -> pd.DataFrame:
        if ticks.empty:
            return ticks
        filtered = ticks.loc[(ticks["datetime"] >= start_dt) & (ticks["datetime"] < end_dt)]
        if filtered.empty:
            return filtered
        return filtered.reset_index(drop=True)

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
