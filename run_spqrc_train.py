from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from spqrc_lab.features import load_runtime_spqrc_dataset_from_duckdb
from spqrc_lab.train import train_spqrc_models


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SPQRC 训练入口（独立实验模块）")
    parser.add_argument("--db-path", default="data/duckdb/ticks.duckdb", help="DuckDB 路径")
    parser.add_argument("--provider", default="tq", help="数据来源 provider")
    parser.add_argument("--symbol", required=True, help="合约代码，例如 DCE.v2609")
    parser.add_argument("--start", default=None, help="起始时间，例如 2025-09-01")
    parser.add_argument("--end", default=None, help="结束时间，例如 2026-03-20")
    parser.add_argument("--output-dir", default=None, help="输出目录")
    parser.add_argument("--publish-latest", action="store_true", help="将训练好的运行时 bundle 发布到 spqrc_outputs/latest")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir or f"spqrc_outputs/{args.symbol.replace('.', '_')}")
    dataset = load_runtime_spqrc_dataset_from_duckdb(
        db_path=Path(args.db_path),
        symbol=args.symbol,
        provider=args.provider,
        start=args.start,
        end=args.end,
    )
    result = train_spqrc_models(dataset, output_dir)
    print("SPQRC 训练完成")
    print(f"输出目录: {output_dir.resolve()}")
    print(f"总样本: {result.summary['rows_total']}")
    print(f"测试状态准确率: {result.summary['state_accuracy']:.4f}")
    print(f"H1 中位预测 MAE: {result.summary['future_return_h1_mae']:.6f}")
    print(f"H1 区间覆盖率: {result.summary['interval_coverage_h1']:.4f}")
    if args.publish_latest:
        latest_dir = Path("spqrc_outputs/latest")
        latest_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(output_dir / "spqrc_runtime_bundle.pkl", latest_dir / "spqrc_runtime_bundle.pkl")
        shutil.copy2(output_dir / "summary.json", latest_dir / "summary.json")
        print(f"已发布运行时 bundle: {latest_dir.resolve()}")


if __name__ == "__main__":
    main()
