from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


def load_ticks_from_duckdb(
    db_path: Path,
    symbol: str,
    provider: str = "tq",
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    if not db_path.exists():
        raise FileNotFoundError(f"找不到 DuckDB 数据库: {db_path}")

    predicates = ["provider = ?", "symbol = ?"]
    params: list[object] = [provider, symbol]
    if start:
        predicates.append("ts >= ?")
        params.append(pd.Timestamp(start))
    if end:
        predicates.append("ts <= ?")
        params.append(pd.Timestamp(end))

    where_clause = " AND ".join(predicates)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        frame = conn.execute(
            f"""
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
            WHERE {where_clause}
            ORDER BY ts
            """,
            params,
        ).fetchdf()
    finally:
        conn.close()

    if frame.empty:
        raise RuntimeError(f"找不到 {symbol} 的 tick 数据。")

    frame["datetime"] = pd.to_datetime(frame["datetime"])
    return frame.reset_index(drop=True)


def _classify_tick_direction(frame: pd.DataFrame) -> pd.Series:
    price = pd.to_numeric(frame["last_price"], errors="coerce")
    bid = pd.to_numeric(frame.get("bid_price1"), errors="coerce")
    ask = pd.to_numeric(frame.get("ask_price1"), errors="coerce")
    mid = (bid + ask) / 2.0
    prev_price = price.shift(1)

    direction = pd.Series(np.nan, index=frame.index, dtype="float64")
    direction = np.where(price > prev_price, 1.0, direction)
    direction = np.where(price < prev_price, -1.0, direction)
    direction = pd.Series(direction, index=frame.index, dtype="float64")

    direction = direction.where(~direction.isna(), np.where(price >= ask, 1.0, np.nan))
    direction = pd.Series(direction, index=frame.index, dtype="float64")
    direction = direction.where(~direction.isna(), np.where(price <= bid, -1.0, np.nan))
    direction = pd.Series(direction, index=frame.index, dtype="float64")
    direction = direction.where(~direction.isna(), np.where(price >= mid, 1.0, np.nan))
    direction = pd.Series(direction, index=frame.index, dtype="float64")
    direction = direction.where(~direction.isna(), np.where(price < mid, -1.0, np.nan))
    direction = pd.Series(direction, index=frame.index, dtype="float64")
    return direction.ffill().fillna(0.0)


def build_5m_pseudo_orderflow(ticks: pd.DataFrame) -> pd.DataFrame:
    frame = ticks.copy()
    frame["datetime"] = pd.to_datetime(frame["datetime"])
    frame = frame.sort_values("datetime").reset_index(drop=True)

    numeric_columns = [
        "last_price",
        "volume",
        "open_interest",
        "bid_price1",
        "bid_volume1",
        "ask_price1",
        "ask_volume1",
    ]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")

    frame["dvol"] = frame["volume"].diff().clip(lower=0).fillna(0.0)
    frame["doi"] = frame["open_interest"].diff().fillna(0.0)
    frame["tick_sign"] = _classify_tick_direction(frame)
    frame["signed_vol"] = frame["dvol"] * frame["tick_sign"]
    frame["buy_est"] = np.where(frame["tick_sign"] > 0, frame["dvol"], 0.0)
    frame["sell_est"] = np.where(frame["tick_sign"] < 0, frame["dvol"], 0.0)

    book_sum = (frame["bid_volume1"].fillna(0.0) + frame["ask_volume1"].fillna(0.0)).replace(0.0, np.nan)
    frame["imbalance"] = (frame["bid_volume1"].fillna(0.0) - frame["ask_volume1"].fillna(0.0)) / book_sum
    frame["mid_price"] = (frame["bid_price1"] + frame["ask_price1"]) / 2.0
    frame["microprice"] = (
        frame["ask_price1"] * frame["bid_volume1"].fillna(0.0)
        + frame["bid_price1"] * frame["ask_volume1"].fillna(0.0)
    ) / book_sum
    frame["microprice_bias"] = (frame["microprice"] - frame["mid_price"]) / frame["mid_price"].replace(0.0, np.nan)

    frame["bucket"] = frame["datetime"].dt.floor("5min")
    grouped = frame.groupby("bucket", sort=True)

    bars = pd.DataFrame(
        {
            "datetime": grouped["bucket"].first(),
            "tick_first_time": grouped["datetime"].first(),
            "tick_last_time": grouped["datetime"].last(),
            "open": grouped["last_price"].first(),
            "high": grouped["last_price"].max(),
            "low": grouped["last_price"].min(),
            "close": grouped["last_price"].last(),
            "volume_5m": grouped["dvol"].sum(),
            "buy_est_5m": grouped["buy_est"].sum(),
            "sell_est_5m": grouped["sell_est"].sum(),
            "tick_count_5m": grouped["last_price"].size(),
            "imbalance_mean_5m": grouped["imbalance"].mean(),
            "imbalance_std_5m": grouped["imbalance"].std(),
            "imbalance_close_5m": grouped["imbalance"].last(),
            "microprice_bias_5m": grouped["microprice_bias"].mean(),
            "oi_open_5m": grouped["open_interest"].first(),
            "oi_close_5m": grouped["open_interest"].last(),
            "bid_volume1_close_5m": grouped["bid_volume1"].last(),
            "ask_volume1_close_5m": grouped["ask_volume1"].last(),
        }
    ).reset_index(drop=True)

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
    bars["delta_ratio_above_mean20_flag_5m"] = (
        bars["delta_ratio_5m"] > delta_ratio_mean_20
    ).astype(int)
    bars["doi_positive_flag_5m"] = (bars["dOI_5m"] > 0).astype(int)
    bars["imbalance_positive_flag_5m"] = (bars["imbalance_close_5m"] > 0).astype(int)
    bars["efficiency_above_median20_flag_5m"] = (
        bars["efficiency"] > efficiency_median_20
    ).astype(int)
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
        [
            "价涨_放量_增仓",
            "价涨_放量_减仓",
            "价跌_放量_增仓",
            "价跌_放量_减仓",
        ],
        default="中性",
    )

    bars = bars.replace([np.inf, -np.inf], np.nan)
    return bars


def merge_5m_pseudo_orderflow_into_bars(bars: pd.DataFrame, ticks: pd.DataFrame) -> pd.DataFrame:
    if bars.empty or ticks.empty:
        return bars

    features = build_5m_pseudo_orderflow(ticks)
    if features.empty:
        return bars

    merged = bars.copy()
    merged["datetime"] = pd.to_datetime(merged["datetime"])
    feature_columns = [
        "datetime",
        "tick_first_time",
        "tick_last_time",
        "buy_est_5m",
        "sell_est_5m",
        "delta_5m",
        "delta_ratio_5m",
        "cvd_change_5m",
        "cvd_5m",
        "cvd_slope_3",
        "imbalance_mean_5m",
        "imbalance_std_5m",
        "imbalance_close_5m",
        "microprice_bias_5m",
        "dOI_5m",
        "efficiency",
        "efficiency2",
        "return_per_delta",
        "return_per_volume",
        "oi_delta_confirm",
        "delta_positive_flag_5m",
        "delta_ratio_above_mean20_flag_5m",
        "doi_positive_flag_5m",
        "imbalance_positive_flag_5m",
        "efficiency_above_median20_flag_5m",
        "orderflow_strength_score_5m",
        "regime",
    ]
    return merged.merge(features[feature_columns], on="datetime", how="left")
