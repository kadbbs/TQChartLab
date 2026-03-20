from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from tqsdk import TqApi, TqAuth

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tq_app.contracts import load_tq_contract_catalog

PERIODS: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "1d": 86400,
}

DEFAULT_PERIODS = ["tick", "1m", "5m", "15m", "30m", "1h", "1d"]


@dataclass
class DownloadConfig:
    output_dir: Path
    periods: list[str]
    tick_length: int
    intraday_length: int
    daily_length: int
    limit: int | None
    resume: bool
    wait_timeout_seconds: float
    wait_step_seconds: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="批量保存当前账号可访问的天勤行情序列。")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "tq_downloads",
        help="下载文件输出目录。",
    )
    parser.add_argument(
        "--periods",
        nargs="+",
        default=DEFAULT_PERIODS,
        help="要下载的周期，可选：tick 1m 5m 15m 30m 1h 1d",
    )
    parser.add_argument("--tick-length", type=int, default=500000, help="Tick 序列长度。")
    parser.add_argument("--intraday-length", type=int, default=8000, help="分钟/小时 K 线长度。")
    parser.add_argument("--daily-length", type=int, default=5000, help="日线长度。")
    parser.add_argument("--limit", type=int, default=None, help="仅下载前 N 个合约，用于试跑。")
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="不跳过已存在文件，强制重下。",
    )
    parser.add_argument("--wait-timeout-seconds", type=float, default=12.0, help="单次序列等待超时。")
    parser.add_argument("--wait-step-seconds", type=float, default=0.5, help="等待序列时的轮询步长。")
    return parser.parse_args()


def ensure_auth() -> tuple[str, str]:
    load_dotenv(PROJECT_ROOT / ".env")
    user = os.getenv("TQ_USER")
    password = os.getenv("TQ_PASSWORD")
    if not user or not password:
        raise RuntimeError("请先在 .env 中设置 TQ_USER 和 TQ_PASSWORD。")
    return user, password


def validate_periods(periods: list[str]) -> list[str]:
    normalized: list[str] = []
    allowed = {"tick", *PERIODS.keys()}
    for period in periods:
        key = period.strip().lower()
        if key not in allowed:
            raise ValueError(f"不支持的周期: {period}")
        if key not in normalized:
            normalized.append(key)
    return normalized


def prepare_directories(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "catalog").mkdir(parents=True, exist_ok=True)
    (output_dir / "recent").mkdir(parents=True, exist_ok=True)


def export_contract_catalog(contracts: list[dict[str, Any]], output_dir: Path) -> None:
    catalog_dir = output_dir / "catalog"
    json_path = catalog_dir / "contracts.json"
    csv_path = catalog_dir / "contracts.csv"

    json_path.write_text(json.dumps(contracts, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(contracts).to_csv(csv_path, index=False)


def write_manifest(config: DownloadConfig, contracts: list[dict[str, Any]]) -> None:
    manifest = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "periods": config.periods,
        "tick_length": config.tick_length,
        "intraday_length": config.intraday_length,
        "daily_length": config.daily_length,
        "contract_count": len(contracts),
        "resume": config.resume,
    }
    path = config.output_dir / "manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def symbol_output_path(output_dir: Path, period: str, contract: dict[str, Any]) -> Path:
    exchange_id = str(contract.get("exchange_id") or "UNKNOWN")
    product_id = str(contract.get("product_id") or "UNKNOWN")
    symbol = str(contract["symbol"])
    period_dir = output_dir / "recent" / period / exchange_id / product_id
    period_dir.mkdir(parents=True, exist_ok=True)
    return period_dir / f"{symbol}.csv"


def fetch_serial_frame(api: TqApi, symbol: str, period: str, config: DownloadConfig) -> pd.DataFrame:
    if period == "tick":
        serial = api.get_tick_serial(symbol, data_length=config.tick_length)
        required_columns = ["datetime", "last_price"]
    else:
        duration_seconds = PERIODS[period]
        data_length = config.daily_length if period == "1d" else config.intraday_length
        serial = api.get_kline_serial(symbol, duration_seconds=duration_seconds, data_length=data_length)
        required_columns = ["datetime", "open", "high", "low", "close"]

    deadline = time.time() + config.wait_timeout_seconds
    last_frame = pd.DataFrame()
    while time.time() < deadline:
        api.wait_update(deadline=time.time() + config.wait_step_seconds)
        frame = serial.copy()
        if frame.empty:
            continue
        frame = frame.dropna(subset=required_columns)
        if not frame.empty:
            last_frame = frame
            break
        last_frame = frame

    if last_frame.empty:
        return pd.DataFrame()

    normalized = last_frame.copy()
    normalized["datetime"] = pd.to_datetime(normalized["datetime"], unit="ns")
    normalized = normalized.sort_values("datetime").reset_index(drop=True)
    return normalized


def save_frame(frame: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def download_contract(api_auth: tuple[str, str], contract: dict[str, Any], config: DownloadConfig) -> dict[str, Any]:
    symbol = str(contract["symbol"])
    summary = {"symbol": symbol, "saved": 0, "skipped": 0, "failed": 0}

    api = TqApi(auth=TqAuth(*api_auth))
    try:
        for period in config.periods:
            output_path = symbol_output_path(config.output_dir, period, contract)
            if config.resume and output_path.exists() and output_path.stat().st_size > 0:
                summary["skipped"] += 1
                log(f"skip {symbol} {period} -> {output_path}")
                continue

            try:
                frame = fetch_serial_frame(api, symbol, period, config)
                if frame.empty:
                    summary["failed"] += 1
                    log(f"empty {symbol} {period}")
                    continue
                save_frame(frame, output_path)
                summary["saved"] += 1
                log(f"saved {symbol} {period} rows={len(frame)} -> {output_path}")
            except Exception as exc:
                summary["failed"] += 1
                log(f"failed {symbol} {period}: {exc}")
    finally:
        api.close()

    return summary


def main() -> int:
    args = parse_args()
    config = DownloadConfig(
        output_dir=args.output_dir.resolve(),
        periods=validate_periods(args.periods),
        tick_length=max(args.tick_length, 1),
        intraday_length=max(args.intraday_length, 1),
        daily_length=max(args.daily_length, 1),
        limit=args.limit if args.limit and args.limit > 0 else None,
        resume=not args.no_resume,
        wait_timeout_seconds=max(args.wait_timeout_seconds, 1.0),
        wait_step_seconds=max(min(args.wait_step_seconds, 5.0), 0.1),
    )

    api_auth = ensure_auth()
    prepare_directories(config.output_dir)

    contracts = load_tq_contract_catalog(PROJECT_ROOT)
    if config.limit is not None:
        contracts = contracts[: config.limit]
    if not contracts:
        raise RuntimeError("没有获取到可下载的合约列表。")

    export_contract_catalog(contracts, config.output_dir)
    write_manifest(config, contracts)

    log(f"start download contracts={len(contracts)} periods={','.join(config.periods)}")

    totals = {"saved": 0, "skipped": 0, "failed": 0}
    for index, contract in enumerate(contracts, start=1):
        symbol = str(contract["symbol"])
        label = str(contract.get("label") or symbol)
        log(f"[{index}/{len(contracts)}] {label}")
        summary = download_contract(api_auth, contract, config)
        totals["saved"] += int(summary["saved"])
        totals["skipped"] += int(summary["skipped"])
        totals["failed"] += int(summary["failed"])

    log(
        "done "
        f"saved={totals['saved']} skipped={totals['skipped']} failed={totals['failed']} "
        f"output={config.output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
