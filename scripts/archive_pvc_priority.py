from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
from dotenv import load_dotenv
from tqsdk import TqApi, TqAuth

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tq_app.contracts import load_tq_contract_catalog

PERIODS: dict[str, int] = {
    "tick": 0,
    "5m": 300,
    "10m": 600,
    "15m": 900,
}


@dataclass
class Config:
    output_dir: Path
    periods: list[str]
    tick_length: int
    kline_length: int
    poll_seconds: int
    wait_timeout_seconds: float
    target_additional_bytes: int
    include_avg_contracts: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="持续归档 PVC 优先行情数据。")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "pvc_priority",
        help="PVC 数据归档目录。",
    )
    parser.add_argument(
        "--periods",
        nargs="+",
        default=["tick", "5m", "10m", "15m"],
        help="要归档的周期，默认 tick 5m 10m 15m",
    )
    parser.add_argument("--tick-length", type=int, default=500000, help="每次抓取的 tick 序列长度。")
    parser.add_argument("--kline-length", type=int, default=20000, help="每次抓取的 K 线序列长度。")
    parser.add_argument("--poll-seconds", type=int, default=90, help="每轮抓取后的休眠秒数。")
    parser.add_argument(
        "--target-size-gb",
        type=float,
        default=20.0,
        help="相对当前目录，额外归档的目标大小（GB）。",
    )
    parser.add_argument(
        "--include-avg-contracts",
        action="store_true",
        help="包含类似 DCE.v2609F 的月均合约。",
    )
    parser.add_argument("--wait-timeout-seconds", type=float, default=10.0, help="单次序列等待秒数。")
    return parser.parse_args()


def ensure_auth() -> tuple[str, str]:
    load_dotenv(PROJECT_ROOT / ".env")
    user = os.getenv("TQ_USER")
    password = os.getenv("TQ_PASSWORD")
    if not user or not password:
        raise RuntimeError("请先在 .env 中设置 TQ_USER 和 TQ_PASSWORD。")
    return user, password


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def directory_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for file_path in path.rglob("*"):
        if file_path.is_file():
            total += file_path.stat().st_size
    return total


def normalize_periods(periods: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    for period in periods:
        key = period.strip().lower()
        if key not in PERIODS:
            raise ValueError(f"不支持的周期: {period}")
        if key not in normalized:
            normalized.append(key)
    return normalized


def select_pvc_contracts(include_avg_contracts: bool) -> list[dict[str, str]]:
    contracts = load_tq_contract_catalog(PROJECT_ROOT)
    selected: list[dict[str, str]] = []
    for contract in contracts:
        if contract.get("exchange_id") != "DCE" or contract.get("product_id") != "v":
            continue
        symbol = str(contract["symbol"])
        if not include_avg_contracts and symbol.endswith("F"):
            continue
        selected.append(contract)
    selected.sort(key=lambda item: item["symbol"])
    return selected


def output_path(output_dir: Path, contract: dict[str, str], period: str) -> Path:
    symbol = str(contract["symbol"])
    exchange_id = str(contract["exchange_id"])
    return output_dir / period / exchange_id / f"{symbol}.csv"


def read_last_timestamp(path: Path) -> pd.Timestamp | None:
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        with path.open("rb") as file_obj:
            file_obj.seek(0, os.SEEK_END)
            end = file_obj.tell()
            if end == 0:
                return None
            pointer = max(end - 4096, 0)
            file_obj.seek(pointer)
            tail = file_obj.read().decode("utf-8", errors="ignore")
        lines = [line.strip() for line in tail.splitlines() if line.strip()]
        if len(lines) <= 1:
            return None
        last_line = lines[-1]
        row = next(csv.reader([last_line]))
        if not row:
            return None
        return pd.to_datetime(row[0])
    except Exception:
        return None


def fetch_frame(api_auth: tuple[str, str], symbol: str, period: str, config: Config) -> pd.DataFrame:
    api = TqApi(auth=TqAuth(*api_auth))
    try:
        if period == "tick":
            serial = api.get_tick_serial(symbol, data_length=config.tick_length)
            required_columns = ["datetime", "last_price"]
        else:
            serial = api.get_kline_serial(
                symbol,
                duration_seconds=PERIODS[period],
                data_length=config.kline_length,
            )
            required_columns = ["datetime", "open", "high", "low", "close"]

        deadline = time.time() + config.wait_timeout_seconds
        frame = pd.DataFrame()
        while time.time() < deadline:
            api.wait_update(deadline=time.time() + 0.5)
            candidate = serial.copy()
            if candidate.empty:
                continue
            candidate = candidate.dropna(subset=required_columns)
            if candidate.empty:
                continue
            frame = candidate
            break

        if frame.empty:
            return frame

        frame = frame.copy()
        frame["datetime"] = pd.to_datetime(frame["datetime"], unit="ns")
        return frame.sort_values("datetime").reset_index(drop=True)
    finally:
        api.close()


def append_new_rows(path: Path, frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0

    path.parent.mkdir(parents=True, exist_ok=True)
    last_timestamp = read_last_timestamp(path)
    if last_timestamp is not None:
        frame = frame.loc[frame["datetime"] > last_timestamp]
    if frame.empty:
        return 0

    write_header = not path.exists() or path.stat().st_size == 0
    frame.to_csv(path, mode="a", header=write_header, index=False)
    return len(frame)


def main() -> int:
    args = parse_args()
    config = Config(
        output_dir=args.output_dir.resolve(),
        periods=normalize_periods(args.periods),
        tick_length=max(args.tick_length, 1),
        kline_length=max(args.kline_length, 1),
        poll_seconds=max(args.poll_seconds, 10),
        wait_timeout_seconds=max(args.wait_timeout_seconds, 1.0),
        target_additional_bytes=max(int(args.target_size_gb * 1024**3), 1),
        include_avg_contracts=bool(args.include_avg_contracts),
    )
    api_auth = ensure_auth()
    config.output_dir.mkdir(parents=True, exist_ok=True)

    contracts = select_pvc_contracts(config.include_avg_contracts)
    if not contracts:
        raise RuntimeError("没有找到可归档的 PVC 合约。")

    baseline_size = directory_size_bytes(config.output_dir)
    target_size = baseline_size + config.target_additional_bytes

    log(
        "start pvc archive "
        f"contracts={len(contracts)} periods={','.join(config.periods)} "
        f"baseline={baseline_size} target={target_size}"
    )

    cycle = 0
    while True:
        cycle += 1
        current_size = directory_size_bytes(config.output_dir)
        if current_size >= target_size:
            log(f"target reached size={current_size}")
            break

        log(f"cycle={cycle} current_size={current_size} target={target_size}")
        cycle_added_rows = 0
        for contract in contracts:
            symbol = str(contract["symbol"])
            for period in config.periods:
                try:
                    frame = fetch_frame(api_auth, symbol, period, config)
                    path = output_path(config.output_dir, contract, period)
                    added_rows = append_new_rows(path, frame)
                    cycle_added_rows += added_rows
                    if added_rows > 0:
                        log(f"appended {symbol} {period} rows={added_rows} -> {path}")
                    else:
                        log(f"no-new-data {symbol} {period}")
                except Exception as exc:
                    log(f"failed {symbol} {period}: {exc}")

        current_size = directory_size_bytes(config.output_dir)
        log(f"cycle={cycle} done rows={cycle_added_rows} size={current_size}")
        if current_size >= target_size:
            log(f"target reached size={current_size}")
            break
        time.sleep(config.poll_seconds)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
