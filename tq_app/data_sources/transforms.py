from __future__ import annotations

import pandas as pd


EMPTY_BAR_COLUMNS = ["datetime", "open", "high", "low", "close", "volume"]


def normalize_bars(klines: pd.DataFrame) -> pd.DataFrame:
    bars = klines.copy()
    bars["datetime"] = pd.to_datetime(bars["datetime"], unit="ns")
    bars = bars.dropna(subset=["open", "high", "low", "close"])
    if bars.empty:
        return pd.DataFrame(columns=EMPTY_BAR_COLUMNS)
    bars = bars.sort_values("datetime")
    return bars.reset_index(drop=True)


def normalize_ticks(ticks: pd.DataFrame) -> pd.DataFrame:
    normalized = ticks.copy()
    if normalized.empty:
        return pd.DataFrame(columns=["datetime", "last_price", "volume"])
    if pd.api.types.is_numeric_dtype(normalized["datetime"]):
        normalized["datetime"] = pd.to_datetime(normalized["datetime"], unit="ns")
    else:
        normalized["datetime"] = pd.to_datetime(normalized["datetime"])
    normalized = normalized.dropna(subset=["last_price"])
    if normalized.empty:
        return pd.DataFrame(columns=["datetime", "last_price", "volume"])
    normalized = normalized.sort_values("datetime")
    return normalized.reset_index(drop=True)


def build_tick_bars(ticks: pd.DataFrame, data_length: int | None = None) -> pd.DataFrame:
    if ticks.empty:
        return pd.DataFrame(columns=EMPTY_BAR_COLUMNS)

    working = ticks.copy()
    working["volume"] = pd.to_numeric(working.get("volume", 0), errors="coerce").fillna(0)
    working["volume_delta"] = working["volume"].diff().clip(lower=0).fillna(0)
    bars = pd.DataFrame(
        {
            "datetime": working["datetime"],
            "open": working["last_price"].astype(float),
            "high": working["last_price"].astype(float),
            "low": working["last_price"].astype(float),
            "close": working["last_price"].astype(float),
            "volume": working["volume_delta"].astype(float),
        }
    )
    if data_length is not None and data_length > 0:
        bars = bars.tail(data_length)
    return bars.reset_index(drop=True)


def build_time_bars_from_ticks(ticks: pd.DataFrame, duration_seconds: int, data_length: int) -> pd.DataFrame:
    if ticks.empty:
        return pd.DataFrame(columns=EMPTY_BAR_COLUMNS)
    if duration_seconds <= 0:
        raise RuntimeError("周期必须大于 0 秒。")

    working = ticks.copy()
    working["last_price"] = pd.to_numeric(working["last_price"], errors="coerce")
    working["volume"] = pd.to_numeric(working.get("volume", 0), errors="coerce").fillna(0)
    working = working.dropna(subset=["last_price"])
    if working.empty:
        return pd.DataFrame(columns=EMPTY_BAR_COLUMNS)

    working["volume_delta"] = working["volume"].diff().clip(lower=0).fillna(0)
    bucket_ns = duration_seconds * 1_000_000_000
    working["bucket_ns"] = (working["datetime"].astype("int64") // bucket_ns) * bucket_ns

    grouped = working.groupby("bucket_ns", sort=True)
    bars = pd.DataFrame(
        {
            "datetime": pd.to_datetime(grouped.size().index, unit="ns"),
            "open": grouped["last_price"].first().astype(float).values,
            "high": grouped["last_price"].max().astype(float).values,
            "low": grouped["last_price"].min().astype(float).values,
            "close": grouped["last_price"].last().astype(float).values,
            "volume": grouped["volume_delta"].sum().astype(float).values,
        }
    )
    if data_length > 0:
        bars = bars.tail(data_length)
    return bars.reset_index(drop=True)


def build_range_bars(ticks: pd.DataFrame, price_tick: float, range_ticks: int, brick_length: int) -> pd.DataFrame:
    if ticks.empty:
        return pd.DataFrame(columns=EMPTY_BAR_COLUMNS)

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
        return pd.DataFrame(columns=EMPTY_BAR_COLUMNS)

    bars = pd.DataFrame(rows)
    if brick_length > 0:
        bars = bars.sort_values("datetime").tail(brick_length)
    return bars.reset_index(drop=True)


def build_renko_bars(ticks: pd.DataFrame, price_tick: float, range_ticks: int, brick_length: int) -> pd.DataFrame:
    if ticks.empty:
        return pd.DataFrame(columns=EMPTY_BAR_COLUMNS)

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
        return pd.DataFrame(columns=EMPTY_BAR_COLUMNS)

    bars = pd.DataFrame(rows)
    if brick_length > 0:
        bars = bars.sort_values("datetime").tail(brick_length)
    return bars.reset_index(drop=True)
