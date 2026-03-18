from __future__ import annotations

import os
import threading
import time

import pandas as pd
from dotenv import load_dotenv
from tqsdk import TqApi, TqAuth

from .base import DataSource


class TqDataSource(DataSource):
    provider_name = "tq"

    def __init__(
        self,
        symbol: str,
        duration_seconds: int,
        data_length: int,
        refresh_ms: int,
    ) -> None:
        self.symbol = symbol
        self.duration_seconds = duration_seconds
        self.data_length = data_length
        self.refresh_ms = refresh_ms
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
            klines = api.get_kline_serial(
                self.symbol,
                duration_seconds=self.duration_seconds,
                data_length=self.data_length,
            )

            while not self._stop_event.is_set():
                api.wait_update(deadline=time.time() + self.refresh_ms / 1000)
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
