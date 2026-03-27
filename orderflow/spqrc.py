from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_SPQRC_BUNDLE_PATH = Path(__file__).resolve().parents[1] / "spqrc_outputs" / "latest" / "spqrc_runtime_bundle.pkl"
_SPQRC_BUNDLE_CACHE: dict[str, object] | None = None
_SPQRC_BUNDLE_MTIME: int | None = None

SPQRC_MODEL_FEATURE_COLUMNS = [
    "ret_1",
    "body_ratio",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "path_trend",
    "path_efficiency",
    "roughness_score",
    "queue_pressure",
    "signature_proxy",
    "breakout_up",
    "breakout_down",
    "delta_ratio_5m",
    "imbalance_close_5m",
    "microprice_bias_5m",
    "dOI_5m",
]


def _to_numeric(series: pd.Series | None, default: float = 0.0) -> pd.Series:
    if series is None:
        return pd.Series(dtype="float64")
    return pd.to_numeric(series, errors="coerce").fillna(default)


def _rolling_zscore(series: pd.Series, window: int, floor: float = 1e-9) -> pd.Series:
    mean = series.shift(1).rolling(window, min_periods=max(5, window // 4)).mean()
    std = series.shift(1).rolling(window, min_periods=max(5, window // 4)).std().replace(0.0, np.nan)
    return ((series - mean) / std.clip(lower=floor)).replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _clip01(series: pd.Series) -> pd.Series:
    return series.clip(0.0, 1.0)


def _softmax_frame(frame: pd.DataFrame) -> pd.DataFrame:
    centered = frame.sub(frame.max(axis=1), axis=0)
    exp = np.exp(centered)
    denom = exp.sum(axis=1).replace(0.0, np.nan)
    return exp.div(denom, axis=0).fillna(0.0)


def load_spqrc_runtime_bundle(bundle_path: Path | None = None) -> dict[str, object] | None:
    global _SPQRC_BUNDLE_CACHE, _SPQRC_BUNDLE_MTIME

    path = Path(bundle_path or DEFAULT_SPQRC_BUNDLE_PATH)
    if not path.exists():
        _SPQRC_BUNDLE_CACHE = None
        _SPQRC_BUNDLE_MTIME = None
        return None

    stat = path.stat()
    if _SPQRC_BUNDLE_CACHE is not None and _SPQRC_BUNDLE_MTIME == stat.st_mtime_ns:
        return _SPQRC_BUNDLE_CACHE

    with path.open("rb") as file:
        bundle = pickle.load(file)
    _SPQRC_BUNDLE_CACHE = bundle
    _SPQRC_BUNDLE_MTIME = stat.st_mtime_ns
    return bundle


def build_spqrc_signal_frame(bars: pd.DataFrame, params: dict[str, float | int | bool] | None = None) -> pd.DataFrame:
    resolved = {
        "path_window": 6,
        "breakout_window": 6,
        "rough_window": 12,
        "conformal_window": 120,
        "horizon_bars": 1,
        "entry_threshold": 0.55,
        "fade_threshold": 0.6,
        "roughness_max": 0.6,
        "noise_max": 0.35,
        "cost_bps": 3.0,
    }
    if params:
        resolved.update(params)

    df = bars.copy()
    for column in ["open", "high", "low", "close", "volume"]:
        df[column] = _to_numeric(df[column])

    delta_ratio = _to_numeric(df.get("delta_ratio_5m"))
    imbalance_close = _to_numeric(df.get("imbalance_close_5m"))
    microprice_bias = _to_numeric(df.get("microprice_bias_5m"))
    doi = _to_numeric(df.get("dOI_5m"))

    eps = 1e-9
    path_window = int(resolved["path_window"])
    breakout_window = int(resolved["breakout_window"])
    rough_window = int(resolved["rough_window"])
    conformal_window = int(resolved["conformal_window"])
    horizon_bars = int(resolved["horizon_bars"])
    cost_rate = float(resolved["cost_bps"]) / 10000.0

    df["ret_1"] = df["close"].pct_change().fillna(0.0)
    bar_range = (df["high"] - df["low"]).abs()
    df["body_ratio"] = (df["close"] - df["open"]).abs() / (bar_range + eps)
    df["upper_wick_ratio"] = (df["high"] - df[["open", "close"]].max(axis=1)).clip(lower=0.0) / (bar_range + eps)
    df["lower_wick_ratio"] = (df[["open", "close"]].min(axis=1) - df["low"]).clip(lower=0.0) / (bar_range + eps)

    signed_ret_sum = df["ret_1"].rolling(path_window, min_periods=max(3, path_window // 2)).sum()
    abs_ret_sum = df["ret_1"].abs().rolling(path_window, min_periods=max(3, path_window // 2)).sum()
    df["path_trend"] = (signed_ret_sum / (abs_ret_sum + eps)).clip(-1.0, 1.0).fillna(0.0)

    df["path_efficiency"] = _clip01(df["body_ratio"])
    flip_rate = (np.sign(df["ret_1"]).diff().abs() > 0).astype(float).rolling(
        rough_window, min_periods=max(4, rough_window // 2)
    ).mean()
    rv_short = df["ret_1"].rolling(6, min_periods=3).std()
    rv_long = df["ret_1"].rolling(rough_window, min_periods=max(4, rough_window // 2)).std()
    vol_ratio = ((rv_short / (rv_long + eps)) - 1.0).clip(-1.0, 2.0)
    df["roughness_score"] = _clip01(
        0.45 * flip_rate.fillna(0.0)
        + 0.30 * (1.0 - df["path_efficiency"])
        + 0.25 * ((vol_ratio.fillna(0.0) + 1.0) / 2.0)
    )

    delta_ratio_z = _rolling_zscore(delta_ratio, conformal_window)
    imbalance_z = _rolling_zscore(imbalance_close, conformal_window)
    microprice_z = _rolling_zscore(microprice_bias, conformal_window)
    doi_z = _rolling_zscore(doi, conformal_window)
    df["queue_pressure"] = np.tanh(
        0.40 * delta_ratio_z + 0.25 * imbalance_z + 0.20 * doi_z + 0.15 * microprice_z
    )

    df["rolling_high"] = df["close"].rolling(breakout_window, min_periods=breakout_window).max().shift(1)
    df["rolling_low"] = df["close"].rolling(breakout_window, min_periods=breakout_window).min().shift(1)
    df["breakout_up"] = df["close"] > df["rolling_high"]
    df["breakout_down"] = df["close"] < df["rolling_low"]

    df["signature_proxy"] = np.tanh(
        0.55 * df["path_trend"]
        + 0.25 * np.sign(df["ret_1"]) * df["path_efficiency"]
        + 0.20 * df["queue_pressure"]
    )
    bundle = load_spqrc_runtime_bundle()
    df["model_mode"] = 0.0
    if bundle is not None:
        df["model_mode"] = 1.0
        feature_columns = bundle.get("feature_columns", SPQRC_MODEL_FEATURE_COLUMNS)
        x = df[feature_columns].fillna(0.0)
        state_model = bundle["state_model"]
        quantile_models = bundle["quantile_models_h1"]
        conformal_q = float(bundle.get("conformal_q_h1", 0.0))
        probs = pd.DataFrame(
            state_model.predict_proba(x),
            columns=state_model.classes_,
            index=df.index,
        )
        for state_name in ["push_up", "push_down", "fade_up", "fade_down", "noise"]:
            df[f"{state_name}_prob"] = probs[state_name] if state_name in probs.columns else 0.0
        df["q50_pred_1"] = quantile_models["0.5"].predict(x)
        df["q10_pred_1"] = quantile_models["0.1"].predict(x) - conformal_q
        df["q90_pred_1"] = quantile_models["0.9"].predict(x) + conformal_q
    else:
        raw = pd.DataFrame(index=df.index)
        raw["push_up"] = (
            1.15 * df["signature_proxy"].clip(lower=0.0)
            + 0.85 * df["queue_pressure"].clip(lower=0.0)
            + 0.45 * df["path_efficiency"]
            - 1.10 * df["roughness_score"]
        )
        raw["push_down"] = (
            1.15 * (-df["signature_proxy"]).clip(lower=0.0)
            + 0.85 * (-df["queue_pressure"]).clip(lower=0.0)
            + 0.45 * df["path_efficiency"]
            - 1.10 * df["roughness_score"]
        )
        raw["fade_up"] = (
            df["breakout_up"].astype(float)
            * (
                0.70 * df["upper_wick_ratio"]
                + 0.65 * df["roughness_score"]
                + 0.55 * (-df["queue_pressure"]).clip(lower=0.0)
                + 0.35 * (-df["path_trend"]).clip(lower=0.0)
            )
        )
        raw["fade_down"] = (
            df["breakout_down"].astype(float)
            * (
                0.70 * df["lower_wick_ratio"]
                + 0.65 * df["roughness_score"]
                + 0.55 * df["queue_pressure"].clip(lower=0.0)
                + 0.35 * df["path_trend"].clip(lower=0.0)
            )
        )
        raw["noise"] = 0.85 * df["roughness_score"] + 0.35 * (1.0 - df["path_efficiency"]) + 0.20 * (
            1.0 - df["queue_pressure"].abs()
        )

        probs = _softmax_frame(raw)
        for column in probs.columns:
            df[f"{column}_prob"] = probs[column]

        df["pred_center_1"] = (
            0.55 * (df["push_up_prob"] - df["push_down_prob"])
            + 0.30 * (df["fade_down_prob"] - df["fade_up_prob"])
            + 0.15 * df["signature_proxy"]
        ) * rv_long.fillna(0.0)

        future_ret_1 = df["close"].shift(-horizon_bars) / df["close"] - 1.0
        residual = (future_ret_1 - df["pred_center_1"]).abs()
        resid_q = residual.shift(1).rolling(conformal_window, min_periods=max(20, conformal_window // 4)).quantile(0.8)
        df["q50_pred_1"] = df["pred_center_1"]
        df["q10_pred_1"] = df["pred_center_1"] - resid_q.fillna(0.0)
        df["q90_pred_1"] = df["pred_center_1"] + resid_q.fillna(0.0)

    entry_threshold = float(resolved["entry_threshold"])
    fade_threshold = float(resolved["fade_threshold"])
    roughness_max = float(resolved["roughness_max"])
    noise_max = float(resolved["noise_max"])

    df["long_signal"] = (
        (df["push_up_prob"] > entry_threshold)
        & (df["q10_pred_1"] > cost_rate)
        & (df["roughness_score"] < roughness_max)
        & (df["noise_prob"] < noise_max)
    )
    df["short_signal"] = (
        (df["push_down_prob"] > entry_threshold)
        & (df["q90_pred_1"] < -cost_rate)
        & (df["roughness_score"] < roughness_max)
        & (df["noise_prob"] < noise_max)
    )
    df["fade_short_signal"] = (
        df["breakout_up"]
        & (df["fade_up_prob"] > fade_threshold)
        & (df["noise_prob"] < 0.7)
    )
    df["fade_long_signal"] = (
        df["breakout_down"]
        & (df["fade_down_prob"] > fade_threshold)
        & (df["noise_prob"] < 0.7)
    )

    df["structure_score"] = (df["push_up_prob"] - df["push_down_prob"]).clip(-1.0, 1.0)
    df["fade_score"] = (df["fade_down_prob"] - df["fade_up_prob"]).clip(-1.0, 1.0)
    rv_scale = rv_long.abs().replace(0.0, np.nan)
    df["edge_score"] = (df["q50_pred_1"] / (rv_scale + eps)).clip(-1.0, 1.0).fillna(0.0)
    df["state_signal"] = np.select(
        [
            df["long_signal"],
            df["short_signal"],
            df["fade_long_signal"],
            df["fade_short_signal"],
        ],
        [1.0, -1.0, 0.5, -0.5],
        default=0.0,
    )
    return df
