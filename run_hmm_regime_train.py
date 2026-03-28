from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from hmm_regime_lab.pipeline import run_hull_atr_hmm_pipeline, save_hull_atr_hmm_result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hull + ATR + HMM Regime 训练入口")
    parser.add_argument("--db-path", default="data/duckdb/ticks.duckdb", help="DuckDB 路径")
    parser.add_argument("--provider", default="tq", help="数据来源 provider")
    parser.add_argument("--train-symbol", default="DCE.v2409", help="训练合约")
    parser.add_argument("--train-start", default="2024-03-20", help="训练开始日期")
    parser.add_argument("--train-end", default="2024-09-13", help="训练结束日期")
    parser.add_argument("--test-symbol", default="DCE.v2509", help="测试合约")
    parser.add_argument("--test-start", default="2025-09-09", help="测试开始日期")
    parser.add_argument("--test-end", default="2025-09-12", help="测试结束日期")
    parser.add_argument("--output-dir", default=None, help="输出目录")
    parser.add_argument("--publish-latest", action="store_true", help="发布 runtime bundle 到 hmm_regime_outputs/latest")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir or f"hmm_regime_outputs/{args.train_symbol.replace('.', '_')}_to_{args.test_symbol.replace('.', '_')}")
    result = run_hull_atr_hmm_pipeline(
        db_path=Path(args.db_path),
        train_symbol=args.train_symbol,
        train_start=args.train_start,
        train_end=args.train_end,
        test_symbol=args.test_symbol,
        test_start=args.test_start,
        test_end=args.test_end,
        provider=args.provider,
    )
    save_hull_atr_hmm_result(result, output_dir)
    print("Hull + ATR + HMM 训练完成")
    print(f"输出目录: {output_dir.resolve()}")
    print(f"训练样本: {result.summary['train_rows']}")
    print(f"测试样本: {result.summary['test_rows']}")
    print(f"平均 P(Strong Trend): {result.summary['test_prob_strong_mean']:.4f}")
    print(f"平均 P(Range): {result.summary['test_prob_range_mean']:.4f}")
    if args.publish_latest:
        latest_dir = Path("hmm_regime_outputs/latest")
        latest_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(output_dir / "hull_atr_hmm_bundle.json", latest_dir / "hull_atr_hmm_bundle.json")
        shutil.copy2(output_dir / "summary.json", latest_dir / "summary.json")
        print(f"已发布运行时 bundle: {latest_dir.resolve()}")


if __name__ == "__main__":
    main()
