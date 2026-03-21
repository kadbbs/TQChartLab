from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tick_archive.duckdb_store import DuckDBTickStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="从 DuckDB 的 tick 主表聚合生成 5 分钟 K 线表。")
    parser.add_argument("--db-path", type=Path, default=PROJECT_ROOT / "data" / "duckdb" / "ticks.duckdb")
    parser.add_argument("--provider", type=str, default="tq", help="存储里的 provider，默认 tq。")
    parser.add_argument("--symbols", nargs="*", default=None, help="只聚合指定合约，不传则聚合该 provider 下全部合约。")
    return parser.parse_args()


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def main() -> int:
    args = parse_args()
    store = DuckDBTickStore(args.db_path.resolve())
    try:
        log(
            f"start build 5m bars provider={args.provider} "
            f"symbols={','.join(args.symbols or []) or 'ALL'} db={args.db_path.resolve()}"
        )
        inserted = store.refresh_5m_bars(provider=args.provider, symbols=args.symbols)
        if args.symbols:
            details = [
                f"{symbol}:{store.get_5m_bar_count(args.provider, symbol)}"
                for symbol in args.symbols
            ]
            log(f"done inserted={inserted} counts={' '.join(details)}")
        else:
            log(f"done inserted={inserted}")
        return 0
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
