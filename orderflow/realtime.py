from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class BucketState:
    datetime: pd.Timestamp
    tick_first_time: pd.Timestamp
    tick_last_time: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    volume_5m: float
    buy_est_5m: float
    sell_est_5m: float
    tick_count_5m: int
    imbalance_sum: float
    imbalance_sq_sum: float
    imbalance_count: int
    imbalance_close_5m: float | None
    microprice_bias_sum: float
    microprice_bias_count: int
    oi_open_5m: float | None
    oi_close_5m: float | None
    bid_volume1_close_5m: float | None
    ask_volume1_close_5m: float | None

    def update(
        self,
        *,
        timestamp: pd.Timestamp,
        price: float,
        dvol: float,
        tick_sign: float,
        imbalance: float | None,
        microprice_bias: float | None,
        open_interest: float | None,
        bid_volume1: float | None,
        ask_volume1: float | None,
    ) -> None:
        self.tick_last_time = timestamp
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume_5m += dvol
        self.tick_count_5m += 1
        if tick_sign > 0:
            self.buy_est_5m += dvol
        elif tick_sign < 0:
            self.sell_est_5m += dvol

        if imbalance is not None and np.isfinite(imbalance):
            self.imbalance_sum += imbalance
            self.imbalance_sq_sum += imbalance * imbalance
            self.imbalance_count += 1
            self.imbalance_close_5m = imbalance

        if microprice_bias is not None and np.isfinite(microprice_bias):
            self.microprice_bias_sum += microprice_bias
            self.microprice_bias_count += 1

        if self.oi_open_5m is None and open_interest is not None and np.isfinite(open_interest):
            self.oi_open_5m = open_interest
        if open_interest is not None and np.isfinite(open_interest):
            self.oi_close_5m = open_interest

        if bid_volume1 is not None and np.isfinite(bid_volume1):
            self.bid_volume1_close_5m = bid_volume1
        if ask_volume1 is not None and np.isfinite(ask_volume1):
            self.ask_volume1_close_5m = ask_volume1

    def to_record(self) -> dict[str, object]:
        imbalance_mean = self.imbalance_sum / self.imbalance_count if self.imbalance_count else np.nan
        if self.imbalance_count > 1:
            variance = (self.imbalance_sq_sum / self.imbalance_count) - (imbalance_mean ** 2)
            imbalance_std = float(np.sqrt(max(variance, 0.0)))
        else:
            imbalance_std = np.nan

        microprice_bias_mean = (
            self.microprice_bias_sum / self.microprice_bias_count if self.microprice_bias_count else np.nan
        )
        return {
            "datetime": self.datetime,
            "tick_first_time": self.tick_first_time,
            "tick_last_time": self.tick_last_time,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume_5m": self.volume_5m,
            "buy_est_5m": self.buy_est_5m,
            "sell_est_5m": self.sell_est_5m,
            "tick_count_5m": self.tick_count_5m,
            "imbalance_mean_5m": imbalance_mean,
            "imbalance_std_5m": imbalance_std,
            "imbalance_close_5m": self.imbalance_close_5m,
            "microprice_bias_5m": microprice_bias_mean,
            "oi_open_5m": self.oi_open_5m,
            "oi_close_5m": self.oi_close_5m,
            "bid_volume1_close_5m": self.bid_volume1_close_5m,
            "ask_volume1_close_5m": self.ask_volume1_close_5m,
        }


class IncrementalPseudoOrderflow5m:
    def __init__(self, max_bars: int = 240) -> None:
        self.max_bars = max_bars
        self.reset()

    def reset(self) -> None:
        self._buckets: OrderedDict[pd.Timestamp, BucketState] = OrderedDict()
        self._last_tick_id: float | None = None
        self._last_price: float | None = None
        self._last_sign: float = 0.0
        self._last_volume: float | None = None
        self._last_open_interest: float | None = None

    @property
    def last_tick_id(self) -> float | None:
        return self._last_tick_id

    def set_last_tick_id(self, tick_id: float | None) -> None:
        self._last_tick_id = tick_id

    def update(self, ticks: pd.DataFrame) -> None:
        if ticks.empty:
            return

        frame = ticks.copy()
        frame["datetime"] = pd.to_datetime(frame["datetime"])
        frame = frame.sort_values("datetime").reset_index(drop=True)

        numeric_columns = [
            "id",
            "last_price",
            "volume",
            "open_interest",
            "bid_price1",
            "bid_volume1",
            "ask_price1",
            "ask_volume1",
        ]
        for column in numeric_columns:
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")

        for row in frame.itertuples(index=False):
            tick_id = getattr(row, "id", None)
            if tick_id is not None and np.isfinite(tick_id):
                if self._last_tick_id is not None and tick_id <= self._last_tick_id:
                    continue
                self._last_tick_id = float(tick_id)

            price = float(getattr(row, "last_price"))
            timestamp = pd.Timestamp(getattr(row, "datetime"))
            volume = float(getattr(row, "volume", 0.0) or 0.0)
            open_interest_raw = getattr(row, "open_interest", np.nan)
            open_interest = float(open_interest_raw) if pd.notna(open_interest_raw) else np.nan
            bid_price = float(getattr(row, "bid_price1", np.nan)) if pd.notna(getattr(row, "bid_price1", np.nan)) else np.nan
            ask_price = float(getattr(row, "ask_price1", np.nan)) if pd.notna(getattr(row, "ask_price1", np.nan)) else np.nan
            bid_volume = float(getattr(row, "bid_volume1", np.nan)) if pd.notna(getattr(row, "bid_volume1", np.nan)) else np.nan
            ask_volume = float(getattr(row, "ask_volume1", np.nan)) if pd.notna(getattr(row, "ask_volume1", np.nan)) else np.nan

            dvol = 0.0 if self._last_volume is None else max(volume - self._last_volume, 0.0)
            self._last_volume = volume

            if self._last_price is None:
                tick_sign = 0.0
            elif price > self._last_price:
                tick_sign = 1.0
            elif price < self._last_price:
                tick_sign = -1.0
            else:
                mid = (bid_price + ask_price) / 2.0 if np.isfinite(bid_price) and np.isfinite(ask_price) else np.nan
                if np.isfinite(ask_price) and price >= ask_price:
                    tick_sign = 1.0
                elif np.isfinite(bid_price) and price <= bid_price:
                    tick_sign = -1.0
                elif np.isfinite(mid) and price >= mid:
                    tick_sign = 1.0
                elif np.isfinite(mid) and price < mid:
                    tick_sign = -1.0
                else:
                    tick_sign = self._last_sign

            self._last_price = price
            self._last_sign = tick_sign
            self._last_open_interest = open_interest if np.isfinite(open_interest) else self._last_open_interest

            book_sum = 0.0
            if np.isfinite(bid_volume):
                book_sum += bid_volume
            if np.isfinite(ask_volume):
                book_sum += ask_volume
            imbalance = np.nan
            if book_sum > 0:
                imbalance = ((bid_volume if np.isfinite(bid_volume) else 0.0) - (ask_volume if np.isfinite(ask_volume) else 0.0)) / book_sum

            microprice_bias = np.nan
            if book_sum > 0 and np.isfinite(bid_price) and np.isfinite(ask_price):
                mid_price = (bid_price + ask_price) / 2.0
                microprice = (
                    ask_price * (bid_volume if np.isfinite(bid_volume) else 0.0)
                    + bid_price * (ask_volume if np.isfinite(ask_volume) else 0.0)
                ) / book_sum
                if mid_price:
                    microprice_bias = (microprice - mid_price) / mid_price

            bucket_dt = timestamp.floor("5min")
            bucket = self._buckets.get(bucket_dt)
            if bucket is None:
                bucket = BucketState(
                    datetime=bucket_dt,
                    tick_first_time=timestamp,
                    tick_last_time=timestamp,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume_5m=0.0,
                    buy_est_5m=0.0,
                    sell_est_5m=0.0,
                    tick_count_5m=0,
                    imbalance_sum=0.0,
                    imbalance_sq_sum=0.0,
                    imbalance_count=0,
                    imbalance_close_5m=np.nan,
                    microprice_bias_sum=0.0,
                    microprice_bias_count=0,
                    oi_open_5m=open_interest if np.isfinite(open_interest) else np.nan,
                    oi_close_5m=open_interest if np.isfinite(open_interest) else np.nan,
                    bid_volume1_close_5m=bid_volume if np.isfinite(bid_volume) else np.nan,
                    ask_volume1_close_5m=ask_volume if np.isfinite(ask_volume) else np.nan,
                )
                self._buckets[bucket_dt] = bucket

            bucket.update(
                timestamp=timestamp,
                price=price,
                dvol=dvol,
                tick_sign=tick_sign,
                imbalance=None if not np.isfinite(imbalance) else float(imbalance),
                microprice_bias=None if not np.isfinite(microprice_bias) else float(microprice_bias),
                open_interest=None if not np.isfinite(open_interest) else float(open_interest),
                bid_volume1=None if not np.isfinite(bid_volume) else float(bid_volume),
                ask_volume1=None if not np.isfinite(ask_volume) else float(ask_volume),
            )

        while len(self._buckets) > self.max_bars:
            self._buckets.popitem(last=False)

    def to_frame(self) -> pd.DataFrame:
        if not self._buckets:
            return pd.DataFrame()

        bars = pd.DataFrame([bucket.to_record() for bucket in self._buckets.values()])
        bars = bars.sort_values("datetime").reset_index(drop=True)

        bars["delta_5m"] = bars["buy_est_5m"] - bars["sell_est_5m"]
        bars["delta_ratio_5m"] = bars["delta_5m"] / bars["volume_5m"].replace(0.0, np.nan)
        bars["cvd_change_5m"] = bars["delta_5m"]
        bars["cvd_5m"] = bars["cvd_change_5m"].cumsum()
        bars["cvd_slope_3"] = bars["cvd_5m"].diff(3)
        bars["dOI_5m"] = bars["oi_close_5m"] - bars["oi_open_5m"]
        bars["ret_5m"] = bars["close"].pct_change().fillna(0.0)
        bars["bar_body"] = (bars["close"] - bars["open"]).abs()
        bars["efficiency"] = bars["bar_body"] / bars["volume_5m"].replace(0.0, np.nan)
        bars["efficiency2"] = bars["bar_body"] / (bars["delta_5m"].abs() + 1e-9)
        bars["return_per_delta"] = bars["ret_5m"] / (bars["delta_5m"].abs() + 1e-9)
        bars["return_per_volume"] = bars["ret_5m"] / (bars["volume_5m"] + 1e-9)

        bars["delta_sign"] = np.sign(bars["delta_5m"]).astype(int)
        bars["doi_sign"] = np.sign(bars["dOI_5m"]).astype(int)
        bars["oi_delta_confirm"] = (bars["delta_sign"] == bars["doi_sign"]).astype(int)

        delta_ratio_mean_20 = bars["delta_ratio_5m"].shift(1).rolling(20, min_periods=5).mean()
        efficiency_median_20 = bars["efficiency"].shift(1).rolling(20, min_periods=5).median()

        bars["delta_positive_flag_5m"] = (bars["delta_5m"] > 0).astype(int)
        bars["delta_ratio_above_mean20_flag_5m"] = (bars["delta_ratio_5m"] > delta_ratio_mean_20).astype(int)
        bars["doi_positive_flag_5m"] = (bars["dOI_5m"] > 0).astype(int)
        bars["imbalance_positive_flag_5m"] = (bars["imbalance_close_5m"] > 0).astype(int)
        bars["efficiency_above_median20_flag_5m"] = (bars["efficiency"] > efficiency_median_20).astype(int)
        bars["orderflow_strength_score_5m"] = (
            bars["delta_positive_flag_5m"]
            + bars["delta_ratio_above_mean20_flag_5m"]
            + bars["doi_positive_flag_5m"]
            + bars["imbalance_positive_flag_5m"]
            + bars["efficiency_above_median20_flag_5m"]
        )

        bars["price_change"] = bars["close"] - bars["open"]
        bars["regime"] = np.select(
            [
                (bars["price_change"] > 0) & (bars["volume_5m"] > 0) & (bars["dOI_5m"] > 0),
                (bars["price_change"] > 0) & (bars["volume_5m"] > 0) & (bars["dOI_5m"] < 0),
                (bars["price_change"] < 0) & (bars["volume_5m"] > 0) & (bars["dOI_5m"] > 0),
                (bars["price_change"] < 0) & (bars["volume_5m"] > 0) & (bars["dOI_5m"] < 0),
            ],
            ["价涨_放量_增仓", "价涨_放量_减仓", "价跌_放量_增仓", "价跌_放量_减仓"],
            default="中性",
        )
        return bars.replace([np.inf, -np.inf], np.nan)
