from __future__ import annotations

import json
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, GradientBoostingRegressor
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, mean_absolute_error

from .features import FEATURE_META_COLUMNS, LABEL_COLUMNS, SPQRCDataset


@dataclass(slots=True)
class SPQRCTrainResult:
    summary: dict[str, Any]
    predictions: pd.DataFrame


def _time_split(frame: pd.DataFrame, train_ratio: float = 0.6, calib_ratio: float = 0.2) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    n = len(frame)
    train_end = max(int(n * train_ratio), 1)
    calib_end = max(int(n * (train_ratio + calib_ratio)), train_end + 1)
    train = frame.iloc[:train_end].reset_index(drop=True)
    calib = frame.iloc[train_end:calib_end].reset_index(drop=True)
    test = frame.iloc[calib_end:].reset_index(drop=True)
    return train, calib, test


def train_spqrc_models(
    dataset: SPQRCDataset,
    output_dir: Path,
    random_state: int = 42,
) -> SPQRCTrainResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    frame = dataset.frame.copy()
    feature_columns = dataset.feature_columns

    train, calib, test = _time_split(frame)
    x_train = train[feature_columns].fillna(0.0)
    x_calib = calib[feature_columns].fillna(0.0)
    x_test = test[feature_columns].fillna(0.0)

    state_model = GradientBoostingClassifier(random_state=random_state)
    state_model.fit(x_train, train["state_label"])

    q_models_h1 = {
        q: GradientBoostingRegressor(loss="quantile", alpha=q, random_state=random_state)
        for q in (0.1, 0.5, 0.9)
    }
    for model in q_models_h1.values():
        model.fit(x_train, train["future_return_1"])

    calib_pred_q10 = q_models_h1[0.1].predict(x_calib)
    calib_pred_q50 = q_models_h1[0.5].predict(x_calib)
    calib_pred_q90 = q_models_h1[0.9].predict(x_calib)
    calib_residual = (calib["future_return_1"] - calib_pred_q50).abs()
    conformal_q = float(calib_residual.quantile(0.8)) if len(calib_residual) else 0.0

    test_pred_state = state_model.predict(x_test)
    test_state_proba = pd.DataFrame(
        state_model.predict_proba(x_test),
        columns=[f"prob_{name}" for name in state_model.classes_],
        index=test.index,
    )
    test_pred_q10 = q_models_h1[0.1].predict(x_test) - conformal_q
    test_pred_q50 = q_models_h1[0.5].predict(x_test)
    test_pred_q90 = q_models_h1[0.9].predict(x_test) + conformal_q

    predictions = pd.concat(
        [
            test[FEATURE_META_COLUMNS + ["future_return_1", "future_return_3", "state_label"]].reset_index(drop=True),
            pd.DataFrame(
                {
                    "pred_state": test_pred_state,
                    "pred_q10_h1": test_pred_q10,
                    "pred_q50_h1": test_pred_q50,
                    "pred_q90_h1": test_pred_q90,
                }
            ),
            test_state_proba.reset_index(drop=True),
        ],
        axis=1,
    )

    state_accuracy = float(accuracy_score(test["state_label"], test_pred_state)) if len(test) else 0.0
    mae_h1 = float(mean_absolute_error(test["future_return_1"], test_pred_q50)) if len(test) else 0.0
    interval_coverage = float(((test["future_return_1"] >= test_pred_q10) & (test["future_return_1"] <= test_pred_q90)).mean()) if len(test) else 0.0

    summary = {
        "rows_total": int(len(frame)),
        "rows_train": int(len(train)),
        "rows_calibration": int(len(calib)),
        "rows_test": int(len(test)),
        "feature_count": int(len(feature_columns)),
        "state_accuracy": state_accuracy,
        "future_return_h1_mae": mae_h1,
        "interval_coverage_h1": interval_coverage,
        "conformal_q_h1": conformal_q,
        "state_classes": list(state_model.classes_),
        "state_confusion_matrix": confusion_matrix(test["state_label"], test_pred_state, labels=state_model.classes_).tolist()
        if len(test)
        else [],
        "state_classification_report": classification_report(test["state_label"], test_pred_state, labels=state_model.classes_, output_dict=True)
        if len(test)
        else {},
        "feature_columns": feature_columns,
    }

    (output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    predictions.to_csv(output_dir / "predictions.csv", index=False)
    bundle = {
        "feature_columns": feature_columns,
        "state_model": state_model,
        "quantile_models_h1": {
            "0.1": q_models_h1[0.1],
            "0.5": q_models_h1[0.5],
            "0.9": q_models_h1[0.9],
        },
        "conformal_q_h1": conformal_q,
        "summary": summary,
    }
    with (output_dir / "spqrc_runtime_bundle.pkl").open("wb") as file:
        pickle.dump(bundle, file)
    return SPQRCTrainResult(summary=summary, predictions=predictions)
