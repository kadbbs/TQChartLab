from __future__ import annotations

import argparse
from pathlib import Path

from orderflow import build_5m_pseudo_orderflow, load_ticks_from_duckdb


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DB = PROJECT_ROOT / "data" / "duckdb" / "ticks.duckdb"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "orderflow_outputs"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="从 tick 构建 5 分钟级仿订单流特征表。")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--provider", default="tq")
    parser.add_argument("--symbol", default="DCE.v2609")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--output-name", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    output_name = args.output_name.strip() or f"{args.symbol}_pseudo_orderflow_5m"

    ticks = load_ticks_from_duckdb(
        db_path=args.db_path,
        provider=args.provider,
        symbol=args.symbol,
        start=args.start,
        end=args.end,
    )
    features = build_5m_pseudo_orderflow(ticks)

    csv_path = output_dir / f"{output_name}.csv"
    parquet_path = output_dir / f"{output_name}.parquet"
    features.to_csv(csv_path, index=False)
    features.to_parquet(parquet_path, index=False)

    print("5分钟仿订单流特征已生成")
    print(f"标的: {args.symbol}")
    print(f"tick条数: {len(ticks)}")
    print(f"5分钟bar数: {len(features)}")
    print(f"时间范围: {features.iloc[0]['datetime']} -> {features.iloc[-1]['datetime']}")
    print(f"CSV: {csv_path}")
    print(f"Parquet: {parquet_path}")


if __name__ == "__main__":
    main()
