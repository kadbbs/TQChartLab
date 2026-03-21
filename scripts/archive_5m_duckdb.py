from __future__ import annotations

import argparse
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tick_archive.duckdb_store import ContractMetadata, DuckDBTickStore, TickContract
from tick_archive.tq_history import TqTickHistoryDownloader, load_tq_auth

SUPPORTED_DURATIONS = {60, 300, 600, 900}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="直接从天勤下载分钟 K 线，并归档到 DuckDB 的独立表。")
    parser.add_argument("--db-path", type=Path, default=PROJECT_ROOT / "data" / "duckdb" / "ticks.duckdb")
    parser.add_argument(
        "--temp-dir",
        type=Path,
        default=None,
        help="分钟 K 线下载的临时 CSV 目录，默认会按周期写到 data/duckdb/tmp_bars_<Xm>。",
    )
    parser.add_argument("--duration-seconds", type=int, default=300, help="K 线周期秒数，支持 60/300/600/900。")
    parser.add_argument("--start-date", type=str, default="2024-01-01", help="回补开始日期，格式 YYYY-MM-DD。")
    parser.add_argument("--end-date", type=str, default=date.today().isoformat(), help="回补结束日期。")
    parser.add_argument("--chunk-days", type=int, default=45, help="每次下载的日期块大小。")
    parser.add_argument("--provider", type=str, default="tq", help="数据源标识。")
    parser.add_argument("--exchange-id", type=str, default="DCE", help="交易所过滤。")
    parser.add_argument("--product-id", type=str, default="v", help="品种过滤。")
    parser.add_argument("--include-avg-contracts", action="store_true", help="包含类似 v2609F 的月均合约。")
    parser.add_argument("--symbols", nargs="*", default=None, help="只回补指定合约。")
    parser.add_argument("--limit", type=int, default=None, help="仅处理前 N 个合约，便于试跑。")
    parser.add_argument(
        "--ignore-latest-ts",
        action="store_true",
        help="忽略库内最新 K 线时间，从 start-date 强制回填；重复 K 线会被主键覆盖。",
    )
    parser.add_argument("--keep-temp-files", action="store_true", help="保留下载得到的临时 CSV。")
    return parser.parse_args()


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def daterange_chunks(start_dt: date, end_dt: date, chunk_days: int) -> list[tuple[date, date]]:
    windows: list[tuple[date, date]] = []
    current = start_dt
    while current <= end_dt:
        window_end = min(current + timedelta(days=chunk_days - 1), end_dt)
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)
    return windows


def contract_may_overlap_start(symbol: str, start_date: date) -> bool:
    matched = re.search(r"\.(?:[A-Za-z]+)(\d{4})F?$", symbol)
    if not matched:
        return True
    yymm = matched.group(1)
    contract_year = 2000 + int(yymm[:2])
    contract_month = int(yymm[2:])
    contract_code = contract_year * 100 + contract_month
    start_code = start_date.year * 100 + start_date.month
    return contract_code >= start_code


def select_contracts(
    downloader: TqTickHistoryDownloader,
    exchange_id: str,
    product_id: str,
    include_avg_contracts: bool,
    symbols: list[str] | None,
    limit: int | None,
    start_date: date,
) -> list[dict[str, str]]:
    symbol_filter = {item.strip() for item in symbols or [] if item.strip()}
    if symbol_filter:
        selected = downloader.query_contracts_by_symbols(sorted(symbol_filter))
        if limit is not None and limit > 0:
            selected = selected[:limit]
        return selected

    catalog = downloader.query_futures_contracts(
        exchange_id=exchange_id,
        product_id=product_id,
        include_expired=True,
    )
    selected: list[dict[str, str]] = []
    for contract in catalog:
        symbol = str(contract["symbol"])
        if str(contract.get("exchange_id") or "") != exchange_id:
            continue
        if str(contract.get("product_id") or "") != product_id:
            continue
        if not include_avg_contracts and symbol.endswith("F"):
            continue
        if not contract_may_overlap_start(symbol, start_date):
            continue
        selected.append(contract)
    selected.sort(key=lambda item: item["symbol"])
    if limit is not None and limit > 0:
        selected = selected[:limit]
    return selected


def make_contract(provider: str, contract: dict[str, str]) -> TickContract:
    return TickContract(
        provider=provider,
        symbol=str(contract["symbol"]),
        exchange_id=str(contract.get("exchange_id") or ""),
        product_id=str(contract.get("product_id") or ""),
        instrument_name=str(contract.get("name") or contract["symbol"]),
    )


def make_contract_metadata(provider: str, contract: dict[str, object]) -> ContractMetadata:
    symbol = str(contract["symbol"])
    return ContractMetadata(
        provider=provider,
        symbol=symbol,
        exchange_id=str(contract.get("exchange_id") or ""),
        product_id=str(contract.get("product_id") or ""),
        instrument_name=str(contract.get("name") or symbol),
        underlying_symbol=str(contract.get("underlying_symbol") or ""),
        ins_class=str(contract.get("ins_class") or ""),
        price_tick=float(contract["price_tick"]) if contract.get("price_tick") is not None else None,
        volume_multiple=float(contract["volume_multiple"]) if contract.get("volume_multiple") is not None else None,
        delivery_year=int(contract["delivery_year"]) if contract.get("delivery_year") is not None else None,
        delivery_month=int(contract["delivery_month"]) if contract.get("delivery_month") is not None else None,
        expire_datetime=contract.get("expire_datetime"),
        is_expired=bool(contract.get("expired", False)),
        is_average_contract=symbol.endswith("F"),
    )


def temp_csv_path(temp_dir: Path, symbol: str, window_start: date, window_end: date) -> Path:
    safe_symbol = symbol.replace("/", "_")
    return temp_dir / safe_symbol / f"{window_start.isoformat()}_{window_end.isoformat()}.csv"


def main() -> int:
    args = parse_args()
    if args.duration_seconds not in SUPPORTED_DURATIONS:
        supported = ", ".join(str(item) for item in sorted(SUPPORTED_DURATIONS))
        raise RuntimeError(f"仅支持以下分钟 K 周期秒数: {supported}")
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    if end_date < start_date:
        raise RuntimeError("end-date 不能早于 start-date。")

    duration_minutes = args.duration_seconds // 60
    temp_dir = (
        args.temp_dir.resolve()
        if args.temp_dir is not None
        else (PROJECT_ROOT / "data" / "duckdb" / f"tmp_bars_{duration_minutes}m").resolve()
    )
    temp_dir.mkdir(parents=True, exist_ok=True)

    auth = load_tq_auth(PROJECT_ROOT)
    store = DuckDBTickStore(args.db_path.resolve())
    downloader = TqTickHistoryDownloader(auth)
    total_inserted = 0
    try:
        contracts = select_contracts(
            downloader=downloader,
            exchange_id=args.exchange_id,
            product_id=args.product_id,
            include_avg_contracts=bool(args.include_avg_contracts),
            symbols=args.symbols,
            limit=args.limit,
            start_date=start_date,
        )
        if not contracts:
            raise RuntimeError("没有找到匹配的合约。")
        store.upsert_contracts([make_contract_metadata(args.provider, item) for item in contracts])

        log(
            f"start duckdb {duration_minutes}m archive contracts={len(contracts)} "
            f"date_range={start_date}..{end_date} db={args.db_path.resolve()}"
        )
        for index, contract_dict in enumerate(contracts, start=1):
            contract = make_contract(args.provider, contract_dict)
            symbol_start = start_date
            latest_ts = (
                None
                if args.ignore_latest_ts
                else store.get_latest_bar_timestamp(contract.provider, contract.symbol, args.duration_seconds)
            )
            if latest_ts is not None:
                symbol_start = max(start_date, latest_ts.date())
            if symbol_start > end_date:
                log(f"[{index}/{len(contracts)}] skip {contract.symbol} already up to {latest_ts}")
                continue

            log(f"[{index}/{len(contracts)}] {contract.symbol} from {symbol_start} to {end_date}")
            for window_start, window_end in daterange_chunks(symbol_start, end_date, max(args.chunk_days, 1)):
                csv_path = temp_csv_path(temp_dir, contract.symbol, window_start, window_end)
                batch_id = f"{contract.symbol}:{window_start.isoformat()}:{window_end.isoformat()}"
                log(f"download {batch_id}")
                downloader.download_kline_csv(
                    symbol=contract.symbol,
                    duration_seconds=args.duration_seconds,
                    start_dt=window_start,
                    end_dt=window_end,
                    output_path=csv_path,
                )
                inserted_rows = store.import_kline_csv(
                    csv_path=csv_path,
                    contract=contract,
                    duration_seconds=args.duration_seconds,
                    source_start=window_start,
                    source_end=window_end,
                    ingest_batch_id=batch_id,
                )
                total_inserted += inserted_rows
                log(f"imported {batch_id} rows={inserted_rows}")
                if not args.keep_temp_files and csv_path.exists():
                    csv_path.unlink()
                    parent = csv_path.parent
                    if parent.exists() and not any(parent.iterdir()):
                        parent.rmdir()

        log(f"done total_inserted={total_inserted} db={args.db_path.resolve()}")
        return 0
    finally:
        downloader.close()
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
