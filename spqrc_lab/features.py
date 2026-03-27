from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from orderflow import build_5m_pseudo_orderflow, build_spqrc_signal_frame, load_ticks_from_duckdb
from orderflow.spqrc import SPQRC_MODEL_FEATURE_COLUMNS


FEATURE_META_COLUMNS = [
    "datetime",
    "bar_open",
    "bar_high",
    "bar_low",
    "bar_close",
    "bar_volume",
]

LABEL_COLUMNS = [
    "future_return_1",
    "future_return_3",
    "state_label",
]


@dataclass(slots=True)
class SPQRCDataset:
    frame: pd.DataFrame
    feature_columns: list[str]
    label_columns: list[str]


def _safe_div(numerator: pd.Series, denominator: pd.Series | float, eps: float = 1e-9) -> pd.Series:
    return numerator / (denominator.replace(0.0, np.nan) + eps if isinstance(denominator, pd.Series) else denominator + eps)


def _build_500ms_snapshots(ticks: pd.DataFrame) -> pd.DataFrame:
    frame = ticks.copy()
    frame["datetime"] = pd.to_datetime(frame["datetime"])
    frame = frame.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")
    frame = frame.set_index("datetime")

    price_columns = ["last_price", "bid_price1", "ask_price1", "open_interest", "volume", "amount"]
    size_columns = ["bid_volume1", "ask_volume1"]
    available_columns = [column for column in price_columns + size_columns if column in frame.columns]
    frame = frame[available_columns].apply(pd.to_numeric, errors="coerce")

    snapshots = frame.resample("500ms").last().ffill()
    snapshots = snapshots.dropna(subset=["last_price"]).reset_index()
    snapshots["mid_price"] = (snapshots["bid_price1"] + snapshots["ask_price1"]) / 2.0
    snapshots["spread"] = snapshots["ask_price1"] - snapshots["bid_price1"]
    book_sum = (snapshots["bid_volume1"].fillna(0.0) + snapshots["ask_volume1"].fillna(0.0)).replace(0.0, np.nan)
    snapshots["imbalance"] = (snapshots["bid_volume1"].fillna(0.0) - snapshots["ask_volume1"].fillna(0.0)) / book_sum
    snapshots["microprice"] = (
        snapshots["ask_price1"] * snapshots["bid_volume1"].fillna(0.0)
        + snapshots["bid_price1"] * snapshots["ask_volume1"].fillna(0.0)
    ) / book_sum
    snapshots["micro_gap"] = _safe_div(snapshots["microprice"] - snapshots["mid_price"], snapshots["mid_price"])
    snapshots["ret_500ms"] = snapshots["mid_price"].pct_change().fillna(0.0)
    snapshots["dvol"] = snapshots["volume"].diff().clip(lower=0).fillna(0.0)
    snapshots["doi"] = snapshots["open_interest"].diff().fillna(0.0)
    snapshots["amount_delta"] = snapshots.get("amount", pd.Series(0.0, index=snapshots.index)).diff().clip(lower=0).fillna(0.0)
    snapshots["rv_short"] = snapshots["ret_500ms"].rolling(6, min_periods=3).std().fillna(0.0)
    return snapshots


def _build_5m_bars_from_ticks(ticks: pd.DataFrame) -> pd.DataFrame:
    frame = ticks.copy()
    frame["datetime"] = pd.to_datetime(frame["datetime"])
    frame = frame.sort_values("datetime").reset_index(drop=True)
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce").fillna(0.0)
    frame["volume_delta"] = frame["volume"].diff().clip(lower=0.0).fillna(0.0)
    frame["bucket"] = frame["datetime"].dt.floor("5min")
    grouped = frame.groupby("bucket", sort=True)
    bars = pd.DataFrame(
        {
            "datetime": grouped["bucket"].first(),
            "bar_open": grouped["last_price"].first().astype(float).values,
            "bar_high": grouped["last_price"].max().astype(float).values,
            "bar_low": grouped["last_price"].min().astype(float).values,
            "bar_close": grouped["last_price"].last().astype(float).values,
            "bar_volume": grouped["volume_delta"].sum().astype(float).values,
        }
    )
    bars["ret_1"] = bars["bar_close"].pct_change().fillna(0.0)
    return bars.reset_index(drop=True)


def _signature_level12(increments: np.ndarray, prefix: str) -> dict[str, float]:
    features: dict[str, float] = {}
    if increments.size == 0:
        return features

    level1 = increments.sum(axis=0)
    for idx, value in enumerate(level1):
        features[f"{prefix}_sig1_{idx}"] = float(value)

    cumulative = np.cumsum(increments, axis=0)
    prior = np.vstack([np.zeros((1, increments.shape[1])), cumulative[:-1]])
    level2 = prior.T @ increments + 0.5 * (increments.T @ increments)
    for i in range(level2.shape[0]):
        for j in range(level2.shape[1]):
            features[f"{prefix}_sig2_{i}_{j}"] = float(level2[i, j])
    return features


def _window_features(window: pd.DataFrame) -> dict[str, float]:
    features: dict[str, float] = {}
    if window.empty:
        return features

    ret = window["ret_500ms"].fillna(0.0)
    imbalance = window["imbalance"].fillna(0.0)
    micro_gap = window["micro_gap"].fillna(0.0)
    doi = window["doi"].fillna(0.0)
    dvol = window["dvol"].fillna(0.0)
    spread = window["spread"].fillna(0.0)
    rv_short = window["rv_short"].fillna(0.0)

    signed_ret = ret.sum()
    abs_ret = ret.abs().sum()
    path_efficiency = abs(float(window["mid_price"].iloc[-1] - window["mid_price"].iloc[0])) / (window["mid_price"].diff().abs().sum() + 1e-9)
    flip_rate = (np.sign(ret).diff().abs() > 0).astype(float).mean()
    roughness_proxy = float(
        np.clip(
            0.45 * flip_rate
            + 0.30 * (1.0 - np.clip(path_efficiency, 0.0, 1.0))
            + 0.25 * np.clip(rv_short.std() / (ret.std() + 1e-9), 0.0, 2.0) / 2.0,
            0.0,
            1.0,
        )
    )

    features.update(
        {
            "path_signed_return": float(signed_ret),
            "path_abs_return": float(abs_ret),
            "path_efficiency": float(path_efficiency),
            "flip_rate": float(flip_rate),
            "roughness_proxy": roughness_proxy,
            "spread_mean": float(spread.mean()),
            "spread_std": float(spread.std(ddof=0)),
            "imbalance_mean": float(imbalance.mean()),
            "imbalance_last": float(imbalance.iloc[-1]),
            "micro_gap_mean": float(micro_gap.mean()),
            "micro_gap_last": float(micro_gap.iloc[-1]),
            "dvol_sum": float(dvol.sum()),
            "doi_sum": float(doi.sum()),
            "rv_mean": float(rv_short.mean()),
            "rv_last": float(rv_short.iloc[-1]),
        }
    )

    signature_matrix = np.column_stack(
        [
            ret.to_numpy(),
            imbalance.to_numpy(),
            micro_gap.to_numpy(),
            np.tanh(doi.to_numpy() / (np.abs(doi.to_numpy()).mean() + 1e-9)),
        ]
    )
    features.update(_signature_level12(signature_matrix, "path"))
    return features


def _label_state(bars: pd.DataFrame, idx: int, breakout_window: int) -> str | None:
    if idx >= len(bars) - 3:
        return None
    close_now = bars.iloc[idx]["bar_close"]
    if not np.isfinite(close_now) or abs(float(close_now)) < 1e-9:
        return None
    future_1 = bars.iloc[idx + 1]["bar_close"] / close_now - 1.0
    future_3 = bars.iloc[idx + 3]["bar_close"] / close_now - 1.0
    vol_ref = bars["ret_1"].iloc[max(0, idx - 30):idx].std()
    threshold = max(float(vol_ref or 0.0) * 0.8, 0.001)

    rolling_high = bars["bar_close"].iloc[max(0, idx - breakout_window):idx].max()
    rolling_low = bars["bar_close"].iloc[max(0, idx - breakout_window):idx].min()
    breakout_up = bool(idx > breakout_window and close_now > rolling_high)
    breakout_down = bool(idx > breakout_window and close_now < rolling_low)

    if breakout_up and future_1 < -threshold:
        return "fade_up"
    if breakout_down and future_1 > threshold:
        return "fade_down"
    if future_3 > threshold:
        return "push_up"
    if future_3 < -threshold:
        return "push_down"
    return "noise"


def build_spqrc_feature_dataset(
    ticks: pd.DataFrame,
    *,
    lookback_minutes: int = 5,
    breakout_window: int = 6,
    horizons: tuple[int, int] = (1, 3),
) -> SPQRCDataset:
    snapshots = _build_500ms_snapshots(ticks)
    bars = _build_5m_bars_from_ticks(ticks)

    lookback = pd.Timedelta(minutes=lookback_minutes)
    rows: list[dict[str, object]] = []
    for idx, bar in bars.iterrows():
        decision_time = pd.Timestamp(bar["datetime"])
        window = snapshots.loc[
            (snapshots["datetime"] >= decision_time - lookback) & (snapshots["datetime"] < decision_time)
        ]
        if len(window) < 20:
            continue

        features = _window_features(window)
        row: dict[str, object] = {
            "datetime": decision_time,
            "bar_open": float(bar["bar_open"]),
            "bar_high": float(bar["bar_high"]),
            "bar_low": float(bar["bar_low"]),
            "bar_close": float(bar["bar_close"]),
            "bar_volume": float(bar["bar_volume"]),
        }
        row.update(features)

        for horizon in horizons:
            if idx + horizon < len(bars):
                row[f"future_return_{horizon}"] = float(bars.iloc[idx + horizon]["bar_close"] / bar["bar_close"] - 1.0)
            else:
                row[f"future_return_{horizon}"] = np.nan
        row["state_label"] = _label_state(bars, idx, breakout_window)
        rows.append(row)

    frame = pd.DataFrame(rows).replace([np.inf, -np.inf], np.nan)
    frame = frame.dropna(subset=["future_return_1", "future_return_3", "state_label"]).reset_index(drop=True)
    feature_columns = [column for column in frame.columns if column not in FEATURE_META_COLUMNS + LABEL_COLUMNS]
    return SPQRCDataset(frame=frame, feature_columns=feature_columns, label_columns=LABEL_COLUMNS)


def load_spqrc_dataset_from_duckdb(
    db_path,
    symbol: str,
    provider: str = "tq",
    start: str | None = None,
    end: str | None = None,
) -> SPQRCDataset:
    ticks = load_ticks_from_duckdb(db_path=db_path, symbol=symbol, provider=provider, start=start, end=end)
    return build_spqrc_feature_dataset(ticks)


def build_runtime_spqrc_dataset(
    ticks: pd.DataFrame,
    *,
    breakout_window: int = 6,
) -> SPQRCDataset:
    bars = build_5m_pseudo_orderflow(ticks).rename(columns={"volume_5m": "volume"})
    runtime = build_spqrc_signal_frame(bars)
    runtime["bar_open"] = runtime["open"]
    runtime["bar_high"] = runtime["high"]
    runtime["bar_low"] = runtime["low"]
    runtime["bar_close"] = runtime["close"]
    runtime["bar_volume"] = runtime["volume"]
    runtime["future_return_1"] = runtime["close"].shift(-1) / runtime["close"] - 1.0
    runtime["future_return_3"] = runtime["close"].shift(-3) / runtime["close"] - 1.0

    label_frame = runtime[["bar_close", "ret_1"]].copy()
    labels: list[str | None] = []
    for idx in range(len(runtime)):
        labels.append(_label_state(label_frame, idx, breakout_window))
    runtime["state_label"] = labels

    columns = FEATURE_META_COLUMNS + LABEL_COLUMNS + SPQRC_MODEL_FEATURE_COLUMNS
    frame = runtime[[column for column in columns if column in runtime.columns]].replace([np.inf, -np.inf], np.nan)
    frame = frame.dropna(subset=["future_return_1", "future_return_3", "state_label"]).reset_index(drop=True)
    return SPQRCDataset(
        frame=frame,
        feature_columns=[column for column in SPQRC_MODEL_FEATURE_COLUMNS if column in frame.columns],
        label_columns=LABEL_COLUMNS,
    )


def load_runtime_spqrc_dataset_from_duckdb(
    db_path,
    symbol: str,
    provider: str = "tq",
    start: str | None = None,
    end: str | None = None,
) -> SPQRCDataset:
    ticks = load_ticks_from_duckdb(db_path=db_path, symbol=symbol, provider=provider, start=start, end=end)
    return build_runtime_spqrc_dataset(ticks)
