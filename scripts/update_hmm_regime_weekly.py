from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from hmm_regime_lab.pipeline import run_hull_atr_hmm_pipeline, save_hull_atr_hmm_result


@dataclass(slots=True)
class SymbolWindow:
    symbol: str
    start: str
    end: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="每周滚动更新 Hull+ATR+HMM Regime 模型")
    parser.add_argument("--db-path", default="data/duckdb/ticks.duckdb", help="DuckDB 路径")
    parser.add_argument("--provider", default="tq", help="数据 provider")
    parser.add_argument("--train-symbol", default="DCE.v2409", help="训练合约")
    parser.add_argument("--test-symbol", default="DCE.v2509", help="测试合约")
    parser.add_argument("--train-lookback-days", type=int, default=180, help="训练窗口天数")
    parser.add_argument("--test-lookback-days", type=int, default=7, help="测试窗口天数")
    parser.add_argument("--train-end", default=None, help="手工指定训练截止日期，默认取训练合约最新 bar")
    parser.add_argument("--test-end", default=None, help="手工指定测试截止日期，默认取测试合约最新 bar")
    parser.add_argument("--output-dir", default=None, help="输出目录")
    parser.add_argument("--publish-latest", action="store_true", help="发布 bundle 到 hmm_regime_outputs/latest")
    return parser.parse_args()


def latest_bar_time(db_path: Path, symbol: str, provider: str) -> pd.Timestamp:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        row = conn.execute(
            """
            SELECT max(bar_start)
            FROM market_bars_5m
            WHERE provider = ? AND symbol = ?
            """,
            [provider, symbol],
        ).fetchone()
    finally:
        conn.close()
    if row is None or row[0] is None:
        raise RuntimeError(f"找不到 {symbol} 的 5 分钟 K 线数据。")
    return pd.Timestamp(row[0])


def resolve_window(
    db_path: Path,
    symbol: str,
    provider: str,
    lookback_days: int,
    end: str | None,
) -> SymbolWindow:
    end_ts = pd.Timestamp(end) if end else latest_bar_time(db_path, symbol, provider)
    start_ts = end_ts - pd.Timedelta(days=lookback_days)
    return SymbolWindow(
        symbol=symbol,
        start=start_ts.strftime("%Y-%m-%d"),
        end=end_ts.strftime("%Y-%m-%d"),
    )


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    train_window = resolve_window(
        db_path=db_path,
        symbol=args.train_symbol,
        provider=args.provider,
        lookback_days=args.train_lookback_days,
        end=args.train_end,
    )
    test_window = resolve_window(
        db_path=db_path,
        symbol=args.test_symbol,
        provider=args.provider,
        lookback_days=args.test_lookback_days,
        end=args.test_end,
    )

    output_dir = Path(
        args.output_dir
        or f"hmm_regime_outputs/{train_window.symbol.replace('.', '_')}_{train_window.end}_to_{test_window.symbol.replace('.', '_')}_{test_window.end}"
    )
    result = run_hull_atr_hmm_pipeline(
        db_path=db_path,
        train_symbol=train_window.symbol,
        train_start=train_window.start,
        train_end=train_window.end,
        test_symbol=test_window.symbol,
        test_start=test_window.start,
        test_end=test_window.end,
        provider=args.provider,
    )
    save_hull_atr_hmm_result(result, output_dir)

    meta = {
        "provider": args.provider,
        "train": asdict(train_window),
        "test": asdict(test_window),
        "train_lookback_days": args.train_lookback_days,
        "test_lookback_days": args.test_lookback_days,
        "summary": result.summary,
    }
    (output_dir / "run_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print("每周滚动更新完成")
    print(f"训练窗口: {train_window.symbol} {train_window.start} -> {train_window.end}")
    print(f"测试窗口: {test_window.symbol} {test_window.start} -> {test_window.end}")
    print(f"输出目录: {output_dir.resolve()}")
    print(f"状态映射: {result.summary['state_mapping']}")
    print(f"平均 P(Strong Trend): {result.summary['test_prob_strong_mean']:.4f}")
    print(f"平均 P(Range): {result.summary['test_prob_range_mean']:.4f}")

    if args.publish_latest:
        latest_dir = Path("hmm_regime_outputs/latest")
        latest_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(output_dir / "hull_atr_hmm_bundle.json", latest_dir / "hull_atr_hmm_bundle.json")
        shutil.copy2(output_dir / "summary.json", latest_dir / "summary.json")
        shutil.copy2(output_dir / "run_meta.json", latest_dir / "run_meta.json")
        print(f"已发布运行时 bundle: {latest_dir.resolve()}")


if __name__ == "__main__":
    main()
