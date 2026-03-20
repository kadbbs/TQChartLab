from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd


@dataclass(frozen=True)
class TickContract:
    provider: str
    symbol: str
    exchange_id: str
    product_id: str
    instrument_name: str


@dataclass(frozen=True)
class ContractMetadata:
    provider: str
    symbol: str
    exchange_id: str
    product_id: str
    instrument_name: str
    underlying_symbol: str
    ins_class: str
    price_tick: float | None
    volume_multiple: float | None
    delivery_year: int | None
    delivery_month: int | None
    expire_datetime: pd.Timestamp | None
    is_expired: bool
    is_average_contract: bool


def _quote_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


class DuckDBTickStore:
    table_name = "market_ticks"
    contract_table_name = "contract_metadata"

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))
        self._configure()
        self.ensure_schema()

    def close(self) -> None:
        self.conn.close()

    def _configure(self) -> None:
        self.conn.execute("PRAGMA threads=4")

    def ensure_schema(self) -> None:
        self.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                provider VARCHAR NOT NULL,
                symbol VARCHAR NOT NULL,
                exchange_id VARCHAR NOT NULL,
                product_id VARCHAR NOT NULL,
                instrument_name VARCHAR,
                trading_day DATE NOT NULL,
                ts TIMESTAMP NOT NULL,
                ts_nano BIGINT NOT NULL,
                last_price DOUBLE,
                highest DOUBLE,
                lowest DOUBLE,
                average DOUBLE,
                volume BIGINT,
                amount DOUBLE,
                open_interest BIGINT,
                bid_price1 DOUBLE,
                bid_volume1 BIGINT,
                ask_price1 DOUBLE,
                ask_volume1 BIGINT,
                source_start DATE NOT NULL,
                source_end DATE NOT NULL,
                ingest_batch_id VARCHAR NOT NULL,
                inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS market_ticks_unique_idx
            ON {self.table_name} (provider, symbol, ts_nano)
            """
        )
        self.conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS market_ticks_symbol_ts_idx
            ON {self.table_name} (symbol, ts)
            """
        )
        self.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.contract_table_name} (
                provider VARCHAR NOT NULL,
                symbol VARCHAR NOT NULL,
                exchange_id VARCHAR NOT NULL,
                product_id VARCHAR NOT NULL,
                instrument_name VARCHAR NOT NULL,
                label VARCHAR NOT NULL,
                underlying_symbol VARCHAR,
                ins_class VARCHAR,
                price_tick DOUBLE,
                volume_multiple DOUBLE,
                delivery_year INTEGER,
                delivery_month INTEGER,
                contract_month VARCHAR,
                is_average_contract BOOLEAN NOT NULL DEFAULT FALSE,
                is_expired BOOLEAN NOT NULL DEFAULT FALSE,
                expire_datetime TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (provider, symbol)
            )
            """
        )
        self.conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS contract_metadata_lookup_idx
            ON {self.contract_table_name} (provider, exchange_id, product_id, symbol)
            """
        )

    def get_latest_timestamp(self, provider: str, symbol: str) -> pd.Timestamp | None:
        row = self.conn.execute(
            f"""
            SELECT max(ts) AS latest_ts
            FROM {self.table_name}
            WHERE provider = ? AND symbol = ?
            """,
            [provider, symbol],
        ).fetchone()
        if not row or row[0] is None:
            return None
        return pd.Timestamp(row[0])

    def get_row_count(self, provider: str, symbol: str) -> int:
        row = self.conn.execute(
            f"""
            SELECT count(*)
            FROM {self.table_name}
            WHERE provider = ? AND symbol = ?
            """,
            [provider, symbol],
        ).fetchone()
        return int(row[0] or 0)

    def upsert_contracts(self, contracts: list[ContractMetadata]) -> int:
        if not contracts:
            return 0

        rows = [
            {
                "provider": item.provider,
                "symbol": item.symbol,
                "exchange_id": item.exchange_id,
                "product_id": item.product_id,
                "instrument_name": item.instrument_name,
                "label": format_contract_label(item.symbol, item.instrument_name),
                "underlying_symbol": item.underlying_symbol,
                "ins_class": item.ins_class,
                "price_tick": item.price_tick,
                "volume_multiple": item.volume_multiple,
                "delivery_year": item.delivery_year,
                "delivery_month": item.delivery_month,
                "contract_month": format_contract_month(item.delivery_year, item.delivery_month),
                "is_average_contract": item.is_average_contract,
                "is_expired": item.is_expired,
                "expire_datetime": item.expire_datetime,
            }
            for item in contracts
        ]
        frame = pd.DataFrame(rows)
        self.conn.register("contract_metadata_frame", frame)
        try:
            self.conn.execute(
                f"""
                INSERT OR REPLACE INTO {self.contract_table_name} (
                    provider,
                    symbol,
                    exchange_id,
                    product_id,
                    instrument_name,
                    label,
                    underlying_symbol,
                    ins_class,
                    price_tick,
                    volume_multiple,
                    delivery_year,
                    delivery_month,
                    contract_month,
                    is_average_contract,
                    is_expired,
                    expire_datetime,
                    updated_at
                )
                SELECT
                    provider,
                    symbol,
                    exchange_id,
                    product_id,
                    instrument_name,
                    label,
                    underlying_symbol,
                    ins_class,
                    price_tick,
                    volume_multiple,
                    delivery_year,
                    delivery_month,
                    contract_month,
                    is_average_contract,
                    is_expired,
                    expire_datetime,
                    CURRENT_TIMESTAMP
                FROM contract_metadata_frame
                """
            )
        finally:
            self.conn.unregister("contract_metadata_frame")
        return len(rows)

    def list_contracts(self, storage_provider: str) -> list[dict[str, object]]:
        rows = self.conn.execute(
            f"""
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
            FROM {self.contract_table_name} AS meta
            INNER JOIN {self.table_name} AS ticks
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

        contracts: list[dict[str, object]] = []
        for row in rows:
            contracts.append(
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
                    "last_tick_at": str(pd.Timestamp(row[10])) if row[10] is not None else "",
                    "first_tick_at": str(pd.Timestamp(row[11])) if row[11] is not None else "",
                    "tick_count": int(row[12] or 0),
                }
            )
        return contracts

    def get_contract(self, storage_provider: str, symbol: str) -> dict[str, object] | None:
        row = self.conn.execute(
            f"""
            SELECT
                symbol,
                instrument_name,
                label,
                exchange_id,
                product_id,
                price_tick,
                volume_multiple,
                delivery_year,
                delivery_month,
                contract_month,
                is_average_contract,
                is_expired,
                expire_datetime
            FROM {self.contract_table_name}
            WHERE provider = ? AND symbol = ?
            """,
            [storage_provider, symbol],
        ).fetchone()
        if row is None:
            return None
        return {
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
            "is_average_contract": bool(row[10]),
            "is_expired": bool(row[11]),
            "expire_datetime": str(pd.Timestamp(row[12])) if row[12] is not None else "",
        }

    def import_tick_csv(
        self,
        csv_path: Path,
        contract: TickContract,
        source_start: date,
        source_end: date,
        ingest_batch_id: str,
    ) -> int:
        if not csv_path.exists() or csv_path.stat().st_size == 0:
            return 0

        last_price_col = _quote_identifier(f"{contract.symbol}.last_price")
        highest_col = _quote_identifier(f"{contract.symbol}.highest")
        lowest_col = _quote_identifier(f"{contract.symbol}.lowest")
        average_col = _quote_identifier(f"{contract.symbol}.average")
        volume_col = _quote_identifier(f"{contract.symbol}.volume")
        amount_col = _quote_identifier(f"{contract.symbol}.amount")
        open_interest_col = _quote_identifier(f"{contract.symbol}.open_interest")
        bid_price_col = _quote_identifier(f"{contract.symbol}.bid_price1")
        bid_volume_col = _quote_identifier(f"{contract.symbol}.bid_volume1")
        ask_price_col = _quote_identifier(f"{contract.symbol}.ask_price1")
        ask_volume_col = _quote_identifier(f"{contract.symbol}.ask_volume1")

        before_count = self.get_row_count(contract.provider, contract.symbol)
        self.conn.execute(
            f"""
            INSERT OR IGNORE INTO {self.table_name} (
                provider,
                symbol,
                exchange_id,
                product_id,
                instrument_name,
                trading_day,
                ts,
                ts_nano,
                last_price,
                highest,
                lowest,
                average,
                volume,
                amount,
                open_interest,
                bid_price1,
                bid_volume1,
                ask_price1,
                ask_volume1,
                source_start,
                source_end,
                ingest_batch_id
            )
            SELECT
                ?,
                ?,
                ?,
                ?,
                ?,
                CAST(datetime AS DATE),
                CAST(datetime AS TIMESTAMP),
                CAST(datetime_nano AS BIGINT),
                CAST({last_price_col} AS DOUBLE),
                CAST({highest_col} AS DOUBLE),
                CAST({lowest_col} AS DOUBLE),
                CAST({average_col} AS DOUBLE),
                CAST({volume_col} AS BIGINT),
                CAST({amount_col} AS DOUBLE),
                CAST({open_interest_col} AS BIGINT),
                CAST({bid_price_col} AS DOUBLE),
                CAST({bid_volume_col} AS BIGINT),
                CAST({ask_price_col} AS DOUBLE),
                CAST({ask_volume_col} AS BIGINT),
                ?,
                ?,
                ?
            FROM read_csv_auto(?, header = true)
            WHERE datetime IS NOT NULL AND datetime_nano IS NOT NULL
            """,
            [
                contract.provider,
                contract.symbol,
                contract.exchange_id,
                contract.product_id,
                contract.instrument_name,
                source_start,
                source_end,
                ingest_batch_id,
                str(csv_path),
            ],
        )
        after_count = self.get_row_count(contract.provider, contract.symbol)
        return max(after_count - before_count, 0)


def format_contract_label(symbol: str, instrument_name: str | None = None) -> str:
    readable_name = (instrument_name or "").strip()
    if not readable_name:
        return symbol
    return f"{readable_name} · {symbol}"


def format_contract_month(delivery_year: int | None, delivery_month: int | None) -> str:
    if delivery_year is None or delivery_month is None:
        return ""
    return f"{delivery_year:04d}-{delivery_month:02d}"
