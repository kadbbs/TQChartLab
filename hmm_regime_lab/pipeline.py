from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
from hmmlearn.hmm import GaussianHMM


STATE_RANGE = 0
STATE_SLOW = 1
STATE_STRONG = 2
STATE_NAMES = {
    STATE_RANGE: "Range",
    STATE_SLOW: "Slow Trend",
    STATE_STRONG: "Strong Trend",
}
DEFAULT_HMM_RUNTIME_BUNDLE_PATH = Path(__file__).resolve().parents[1] / "hmm_regime_outputs" / "latest" / "hull_atr_hmm_bundle.json"


def _wma(series: pd.Series, period: int) -> pd.Series:
    safe_period = max(int(period), 1)
    weights = np.arange(1, safe_period + 1, dtype="float64")
    return series.rolling(safe_period).apply(lambda values: float(np.dot(values, weights) / weights.sum()), raw=True)


def _hma(series: pd.Series, period: int) -> pd.Series:
    safe_period = max(int(period), 1)
    half_length = max(safe_period // 2, 1)
    sqrt_length = max(int(safe_period**0.5), 1)
    base = 2 * _wma(series, half_length) - _wma(series, safe_period)
    return _wma(base, sqrt_length)


def _true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - prev_close).abs()
    tr3 = (df["low"] - prev_close).abs()
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)


def _adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    up_move = df["high"].diff()
    down_move = -df["low"].diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    tr = _true_range(df)
    atr = tr.rolling(period).mean()
    plus_di = 100 * pd.Series(plus_dm, index=df.index).rolling(period).mean() / atr.replace(0.0, np.nan)
    minus_di = 100 * pd.Series(minus_dm, index=df.index).rolling(period).mean() / atr.replace(0.0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
    return dx.rolling(period).mean().fillna(0.0)


def load_5m_bars_from_duckdb(
    db_path: Path,
    symbol: str,
    start: str | None = None,
    end: str | None = None,
    provider: str = "tq",
) -> pd.DataFrame:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        predicates = ["provider = ?", "symbol = ?"]
        params: list[object] = [provider, symbol]
        if start:
            predicates.append("bar_start >= ?")
            params.append(pd.Timestamp(start))
        if end:
            predicates.append("bar_start <= ?")
            params.append(pd.Timestamp(end))
        where_clause = " AND ".join(predicates)
        frame = conn.execute(
            f"""
            SELECT
                bar_start AS datetime,
                open,
                high,
                low,
                close,
                volume
            FROM market_bars_5m
            WHERE {where_clause}
            ORDER BY bar_start
            """,
            params,
        ).fetchdf()
    finally:
        conn.close()

    frame["datetime"] = pd.to_datetime(frame["datetime"])
    for column in ["open", "high", "low", "close", "volume"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)


def compute_hull_segments(df: pd.DataFrame, hull_length: int = 55) -> pd.DataFrame:
    frame = df.copy()
    frame["atr"] = _true_range(frame).rolling(14).mean()
    frame["hull"] = _hma(frame["close"], hull_length)
    frame["hull_up"] = frame["hull"] >= frame["hull"].shift(1)
    frame["segment_id"] = frame["hull_up"].ne(frame["hull_up"].shift(1)).cumsum()
    frame["label"] = np.nan
    frame["segment_efficiency"] = np.nan
    frame["segment_drawdown"] = np.nan
    frame["segment_direction_ratio"] = np.nan

    for _, group in frame.groupby("segment_id", sort=True):
        if len(group) < 2:
            continue
        trend_up = bool(group["hull_up"].iloc[0])
        second_open = float(group["open"].iloc[min(1, len(group) - 1)])
        atr_ref = float(group["atr"].iloc[0]) if pd.notna(group["atr"].iloc[0]) else np.nan
        if not np.isfinite(atr_ref) or atr_ref <= 0:
            continue

        if trend_up:
            displacement = float(group["high"].max() - second_open)
            adverse = float(max(second_open - group["low"].min(), 0.0))
            direction_ratio = float((group["close"] >= group["open"]).mean())
        else:
            displacement = float(second_open - group["low"].min())
            adverse = float(max(group["high"].max() - second_open, 0.0))
            direction_ratio = float((group["close"] <= group["open"]).mean())

        efficiency = displacement / max(len(group), 1)
        if displacement < 2.0 * atr_ref:
            label = STATE_RANGE
        elif efficiency < 0.08 * atr_ref or direction_ratio < 0.55:
            label = STATE_SLOW
        elif adverse <= 1.0 * atr_ref:
            label = STATE_STRONG
        else:
            label = STATE_SLOW

        frame.loc[group.index, "label"] = label
        frame.loc[group.index, "segment_efficiency"] = efficiency / atr_ref
        frame.loc[group.index, "segment_drawdown"] = adverse / atr_ref
        frame.loc[group.index, "segment_direction_ratio"] = direction_ratio

    frame["label"] = frame["label"].ffill().fillna(STATE_RANGE).astype(int)
    return frame


def compute_hmm_features(df: pd.DataFrame, slow_ma: int = 89, direction_window: int = 8) -> pd.DataFrame:
    frame = df.copy()
    frame["adx"] = _adx(frame, 14)
    frame["slow_ma"] = frame["close"].rolling(slow_ma).mean()
    frame["ma_spread"] = ((frame["hull"] - frame["slow_ma"]) / frame["close"]).replace([np.inf, -np.inf], np.nan)
    frame["hull_slope"] = (frame["hull"] - frame["hull"].shift(1)) / frame["close"].replace(0.0, np.nan)
    frame["volatility"] = frame["atr"] / frame["close"].replace(0.0, np.nan)
    direction = np.sign(frame["close"].diff()).replace(0.0, np.nan).ffill().fillna(0.0)
    frame["direction_consistency"] = direction.rolling(direction_window).apply(lambda x: abs(np.mean(x)), raw=True)
    feature_cols = ["adx", "ma_spread", "hull_slope", "volatility", "direction_consistency"]
    frame[feature_cols] = frame[feature_cols].replace([np.inf, -np.inf], np.nan)
    return frame


def _fit_robust_scaler(train_df: pd.DataFrame, feature_cols: list[str]) -> dict[str, np.ndarray]:
    x = train_df[feature_cols].fillna(0.0)
    center = x.median().to_numpy(dtype="float64")
    scale = (x.quantile(0.75) - x.quantile(0.25)).replace(0.0, np.nan).fillna(1.0).to_numpy(dtype="float64")
    return {"center": center, "scale": scale}


def _apply_scaler(frame: pd.DataFrame, feature_cols: list[str], scaler: dict[str, np.ndarray]) -> np.ndarray:
    x = frame[feature_cols].fillna(0.0).to_numpy(dtype="float64")
    return (x - scaler["center"]) / scaler["scale"]


def _label_initialized_params(train_df: pd.DataFrame, x_scaled: np.ndarray) -> dict[str, Any]:
    labels = train_df["label"].astype(int).to_numpy()
    n_features = x_scaled.shape[1]
    means = []
    covs = []
    priors = np.zeros(3, dtype="float64")
    transitions = np.ones((3, 3), dtype="float64") * 1e-3

    for state in [STATE_RANGE, STATE_SLOW, STATE_STRONG]:
        x_state = x_scaled[labels == state]
        if len(x_state) == 0:
            means.append(np.zeros(n_features, dtype="float64"))
            covs.append(np.eye(n_features, dtype="float64"))
            priors[state] = 1e-3
            continue
        means.append(np.nanmean(x_state, axis=0))
        cov = np.cov(x_state.T) if len(x_state) > 1 else np.eye(n_features, dtype="float64")
        cov = np.atleast_2d(cov) + np.eye(n_features) * 1e-4
        covs.append(cov)
        priors[state] = max(float((labels == state).mean()), 1e-3)

    for prev_state, next_state in zip(labels[:-1], labels[1:]):
        transitions[int(prev_state), int(next_state)] += 1.0
    transitions = transitions / transitions.sum(axis=1, keepdims=True)
    priors = priors / priors.sum()

    return {
        "priors": priors,
        "transitions": transitions,
        "means": np.asarray(means, dtype="float64"),
        "covs": np.asarray(covs, dtype="float64"),
    }


def _fit_gaussian_hmm(train_df: pd.DataFrame, feature_cols: list[str]) -> dict[str, Any]:
    scaler = _fit_robust_scaler(train_df, feature_cols)
    x = _apply_scaler(train_df, feature_cols, scaler)
    init = _label_initialized_params(train_df, x)
    model = GaussianHMM(
        n_components=3,
        covariance_type="full",
        n_iter=200,
        tol=1e-4,
        random_state=42,
        init_params="",
        params="stmc",
        min_covar=1e-4,
    )
    model.startprob_ = init["priors"]
    model.transmat_ = init["transitions"]
    model.means_ = init["means"]
    model.covars_ = init["covs"]
    model.fit(x)
    return {
        "feature_columns": feature_cols,
        "scaler_center": scaler["center"],
        "scaler_scale": scaler["scale"],
        "priors": np.asarray(model.startprob_, dtype="float64"),
        "transitions": np.asarray(model.transmat_, dtype="float64"),
        "means": {idx: np.asarray(model.means_[idx], dtype="float64") for idx in range(model.n_components)},
        "covs": {idx: np.asarray(model.covars_[idx], dtype="float64") for idx in range(model.n_components)},
    }


def _forward_filter(model: dict[str, Any], frame: pd.DataFrame) -> pd.DataFrame:
    feature_cols = model["feature_columns"]
    scaler = {
        "center": np.asarray(model["scaler_center"], dtype="float64"),
        "scale": np.asarray(model["scaler_scale"], dtype="float64"),
    }
    x = _apply_scaler(frame, feature_cols, scaler)
    hmm = GaussianHMM(n_components=3, covariance_type="full", init_params="")
    hmm.startprob_ = np.asarray(model["priors"], dtype="float64")
    hmm.transmat_ = np.asarray(model["transitions"], dtype="float64")
    hmm.means_ = np.asarray([model["means"][idx] for idx in sorted(model["means"])], dtype="float64")
    hmm.covars_ = np.asarray([model["covs"][idx] for idx in sorted(model["covs"])], dtype="float64")
    hmm.n_features = x.shape[1]
    _, probs = hmm.score_samples(x)

    result = frame.copy()
    result["prob_range"] = probs[:, 0]
    result["prob_slow_trend"] = probs[:, 1]
    result["prob_strong_trend"] = probs[:, 2]
    result["pred_state"] = probs.argmax(axis=1)
    return result


def load_hmm_runtime_bundle(bundle_path: Path | None = None) -> dict[str, Any] | None:
    path = Path(bundle_path or DEFAULT_HMM_RUNTIME_BUNDLE_PATH)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        "feature_columns": payload["feature_columns"],
        "priors": np.asarray(payload["priors"], dtype="float64"),
        "transitions": np.asarray(payload["transitions"], dtype="float64"),
        "means": {int(key): np.asarray(value, dtype="float64") for key, value in payload["means"].items()},
        "covs": {int(key): np.asarray(value, dtype="float64") for key, value in payload["covs"].items()},
        "state_mapping": {int(key): value for key, value in payload.get("state_mapping", {}).items()},
    }


def _map_state_names(frame: pd.DataFrame) -> dict[int, str]:
    state_stats: list[tuple[int, float, float, float]] = []
    for state in sorted(frame["pred_state"].dropna().unique()):
        state_frame = frame.loc[frame["pred_state"] == state]
        label_mean = float(state_frame["label"].mean()) if len(state_frame) else 0.0
        adx_mean = float(state_frame["adx"].mean()) if "adx" in state_frame else 0.0
        eff_mean = float(state_frame["segment_efficiency"].mean()) if "segment_efficiency" in state_frame else 0.0
        state_stats.append((int(state), label_mean, adx_mean, eff_mean))
    state_stats.sort(key=lambda item: (item[1], item[2], item[3]))
    mapping: dict[int, str] = {}
    if len(state_stats) >= 1:
        mapping[state_stats[0][0]] = STATE_NAMES[STATE_RANGE]
    if len(state_stats) >= 2:
        mapping[state_stats[1][0]] = STATE_NAMES[STATE_SLOW]
    if len(state_stats) >= 3:
        mapping[state_stats[2][0]] = STATE_NAMES[STATE_STRONG]
    return mapping


@dataclass(slots=True)
class HullAtrHMMResult:
    train_frame: pd.DataFrame
    test_frame: pd.DataFrame
    feature_columns: list[str]
    state_mapping: dict[int, str]
    summary: dict[str, Any]


def run_hull_atr_hmm_pipeline(
    db_path: Path,
    train_symbol: str,
    train_start: str,
    train_end: str,
    test_symbol: str,
    test_start: str | None = None,
    test_end: str | None = None,
    provider: str = "tq",
) -> HullAtrHMMResult:
    train_bars = load_5m_bars_from_duckdb(db_path, train_symbol, train_start, train_end, provider)
    test_bars = load_5m_bars_from_duckdb(db_path, test_symbol, test_start, test_end, provider)

    train_labeled = compute_hull_segments(train_bars)
    test_labeled = compute_hull_segments(test_bars)
    train_features = compute_hmm_features(train_labeled)
    test_features = compute_hmm_features(test_labeled)

    feature_cols = ["adx", "ma_spread", "hull_slope", "volatility", "direction_consistency"]
    train_ready = train_features.dropna(subset=feature_cols + ["label"]).reset_index(drop=True)
    test_ready = test_features.dropna(subset=feature_cols + ["label"]).reset_index(drop=True)

    model = _fit_gaussian_hmm(train_ready, feature_cols)
    train_out = _forward_filter(model, train_ready)
    test_out = _forward_filter(model, test_ready)
    state_mapping = _map_state_names(train_out)

    summary = {
        "train_rows": int(len(train_out)),
        "test_rows": int(len(test_out)),
        "train_symbol": train_symbol,
        "test_symbol": test_symbol,
        "train_period": [str(train_start), str(train_end)],
        "test_period": [str(test_start), str(test_end)],
        "feature_columns": feature_cols,
        "state_mapping": {str(key): value for key, value in state_mapping.items()},
        "train_label_distribution": train_out["label"].value_counts().sort_index().to_dict(),
        "test_label_distribution": test_out["label"].value_counts().sort_index().to_dict(),
        "test_prob_strong_mean": float(test_out["prob_strong_trend"].mean()) if len(test_out) else 0.0,
        "test_prob_range_mean": float(test_out["prob_range"].mean()) if len(test_out) else 0.0,
        "scaler_center": model["scaler_center"].tolist(),
        "scaler_scale": model["scaler_scale"].tolist(),
        "priors": model["priors"].tolist(),
        "transitions": model["transitions"].tolist(),
        "means": {str(key): value.tolist() for key, value in model["means"].items()},
        "covs": {str(key): value.tolist() for key, value in model["covs"].items()},
    }
    return HullAtrHMMResult(
        train_frame=train_out,
        test_frame=test_out,
        feature_columns=feature_cols,
        state_mapping=state_mapping,
        summary=summary,
    )


def apply_hull_atr_hmm_runtime(bars: pd.DataFrame, bundle: dict[str, Any] | None = None) -> pd.DataFrame:
    runtime_bundle = bundle or load_hmm_runtime_bundle()
    frame = compute_hull_segments(bars)
    frame = compute_hmm_features(frame)
    feature_cols = ["adx", "ma_spread", "hull_slope", "volatility", "direction_consistency"]
    ready = frame.dropna(subset=feature_cols).reset_index(drop=True)
    if runtime_bundle is None or ready.empty:
        frame["prob_range"] = np.nan
        frame["prob_slow_trend"] = np.nan
        frame["prob_strong_trend"] = np.nan
        frame["regime_allow_trend"] = False
        frame["regime_state_name"] = None
        frame["model_mode"] = 0.0
        return frame

    out = _forward_filter(runtime_bundle, ready)
    state_mapping = runtime_bundle.get("state_mapping", {})
    out["regime_state_name"] = out["pred_state"].map(state_mapping)
    out["regime_allow_trend"] = out["prob_strong_trend"] > 0.6
    out["model_mode"] = 1.0
    merged = frame.merge(
        out[["datetime", "prob_range", "prob_slow_trend", "prob_strong_trend", "pred_state", "regime_state_name", "regime_allow_trend", "model_mode"]],
        on="datetime",
        how="left",
    )
    return merged


def save_hull_atr_hmm_result(result: HullAtrHMMResult, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    result.train_frame.to_csv(output_dir / "train_states.csv", index=False)
    result.test_frame.to_csv(output_dir / "test_states.csv", index=False)
    (output_dir / "summary.json").write_text(json.dumps(result.summary, ensure_ascii=False, indent=2), encoding="utf-8")
    runtime_bundle = {
        "feature_columns": result.feature_columns,
        "scaler_center": result.summary["scaler_center"],
        "scaler_scale": result.summary["scaler_scale"],
        "priors": result.summary["priors"],
        "transitions": result.summary["transitions"],
        "means": result.summary["means"],
        "covs": result.summary["covs"],
        "state_mapping": result.summary["state_mapping"],
    }
    (output_dir / "hull_atr_hmm_bundle.json").write_text(
        json.dumps(runtime_bundle, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
