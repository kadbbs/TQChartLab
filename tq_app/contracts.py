from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb
from dotenv import load_dotenv
from tqsdk import TqApi, TqAuth

DEFAULT_EXCHANGES = ["CFFEX", "DCE", "CZCE", "GFEX", "INE", "SHFE"]


def format_contract_label(symbol: str, instrument_name: str | None = None) -> str:
    readable_name = (instrument_name or "").strip()
    if not readable_name:
        return symbol
    return f"{readable_name} · {symbol}"


def load_tq_contract_catalog(project_root: Path) -> list[dict[str, Any]]:
    load_dotenv(project_root / ".env")
    user = os.getenv("TQ_USER")
    password = os.getenv("TQ_PASSWORD")
    if not user or not password:
        return []

    api = TqApi(auth=TqAuth(user, password))
    try:
        contracts: list[dict[str, Any]] = []
        for exchange_id in DEFAULT_EXCHANGES:
            symbols = list(api.query_quotes(ins_class="FUTURE", exchange_id=exchange_id, expired=False))
            if not symbols:
                continue
            info_df = api.query_symbol_info(symbols)
            for row in info_df.itertuples(index=False):
                symbol = str(row.instrument_id)
                instrument_name = str(row.instrument_name or "").strip()
                contracts.append(
                    {
                        "symbol": symbol,
                        "name": instrument_name or symbol,
                        "label": format_contract_label(symbol, instrument_name),
                        "exchange_id": str(row.exchange_id),
                        "product_id": str(row.product_id),
                    }
                )
        contracts.sort(key=lambda item: (item["exchange_id"], item["product_id"], item["symbol"]))
        return contracts
    finally:
        api.close()


def load_duckdb_contract_catalog(project_root: Path) -> list[dict[str, Any]]:
    db_path_raw = os.getenv("DUCKDB_TICK_DB_PATH", "").strip()
    db_path = Path(db_path_raw).expanduser() if db_path_raw else project_root / "data" / "duckdb" / "ticks.duckdb"
    if not db_path.exists():
        return []

    storage_provider = os.getenv("DUCKDB_SOURCE_PROVIDER", "tq").strip() or "tq"
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT
                meta.symbol,
                meta.instrument_name AS name,
                meta.label,
                meta.exchange_id,
                meta.product_id,
                meta.price_tick,
                meta.volume_multiple,
                meta.delivery_year,
                meta.delivery_month,
                meta.contract_month,
                max(ticks.ts) AS last_tick_at,
                min(ticks.ts) AS first_tick_at,
                count(ticks.ts) AS tick_count
            FROM contract_metadata AS meta
            LEFT JOIN market_ticks AS ticks
                ON ticks.provider = meta.provider
               AND ticks.symbol = meta.symbol
            WHERE meta.provider = ?
            GROUP BY
                meta.symbol,
                meta.instrument_name,
                meta.label,
                meta.exchange_id,
                meta.product_id,
                meta.price_tick,
                meta.volume_multiple,
                meta.delivery_year,
                meta.delivery_month,
                meta.contract_month
            ORDER BY meta.exchange_id, meta.product_id, meta.delivery_year, meta.delivery_month, meta.symbol
            """,
            [storage_provider],
        ).fetchall()
        return [
            {
                "symbol": str(row[0]),
                "name": str(row[1]),
                "label": str(row[2]),
                "exchange_id": str(row[3]),
                "product_id": str(row[4]),
                "price_tick": float(row[5]) if row[5] is not None else None,
                "volume_multiple": float(row[6]) if row[6] is not None else None,
                "delivery_year": int(row[7]) if row[7] is not None else None,
                "delivery_month": int(row[8]) if row[8] is not None else None,
                "contract_month": str(row[9]) if row[9] is not None else "",
                "last_tick_at": str(row[10]) if row[10] is not None else "",
                "first_tick_at": str(row[11]) if row[11] is not None else "",
                "tick_count": int(row[12] or 0),
            }
            for row in rows
        ]
    finally:
        conn.close()
