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
    bar_table_names = {
        60: "market_bars_1m",
        300: "market_bars_5m",
        600: "market_bars_10m",
        900: "market_bars_15m",
    }
    bar_5m_table_name = bar_table_names[300]

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
        self._ensure_bar_table_schemas()

    @classmethod
    def get_bar_table_name(cls, duration_seconds: int) -> str:
        table_name = cls.bar_table_names.get(duration_seconds)
        if table_name is None:
            raise ValueError(f"暂不支持 duration_seconds={duration_seconds} 的原生 K 线表。")
        return table_name

    def _ensure_bar_table_schemas(self) -> None:
        for duration_seconds in self.bar_table_names:
            self._ensure_bar_table_schema(duration_seconds)

    def _create_bar_table(self, duration_seconds: int) -> None:
        table_name = self.get_bar_table_name(duration_seconds)
        self.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                provider VARCHAR NOT NULL,
                symbol VARCHAR NOT NULL,
                exchange_id VARCHAR NOT NULL,
                product_id VARCHAR NOT NULL,
                instrument_name VARCHAR,
                trading_day DATE NOT NULL,
                bar_start TIMESTAMP NOT NULL,
                bar_start_epoch BIGINT NOT NULL,
                bar_id BIGINT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                open_oi BIGINT,
                close_oi BIGINT,
                duration_seconds INTEGER NOT NULL DEFAULT {duration_seconds},
                source_start DATE NOT NULL DEFAULT CURRENT_DATE,
                source_end DATE NOT NULL DEFAULT CURRENT_DATE,
                ingest_batch_id VARCHAR NOT NULL DEFAULT '',
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (provider, symbol, bar_start)
            )
            """
        )
        self.conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {table_name}_symbol_bar_start_idx
            ON {table_name} (symbol, bar_start)
            """
        )

    def _ensure_bar_table_schema(self, duration_seconds: int) -> None:
        table_name = self.get_bar_table_name(duration_seconds)
        required_columns = {
            "bar_id",
            "open_oi",
            "close_oi",
            "duration_seconds",
            "source_start",
            "source_end",
            "ingest_batch_id",
        }
        existing_tables = {row[0] for row in self.conn.execute("SHOW TABLES").fetchall()}
        if table_name not in existing_tables:
            self._create_bar_table(duration_seconds)
            return

        columns = {
            str(row[0])
            for row in self.conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        }
        if required_columns.issubset(columns):
            self.conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {table_name}_symbol_bar_start_idx
                ON {table_name} (symbol, bar_start)
                """
            )
            return

        row_count = int(self.conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0] or 0)
        if row_count > 0:
            missing = ", ".join(sorted(required_columns - columns))
            raise RuntimeError(
                f"{table_name} 仍是旧结构且已有 {row_count} 行数据，缺少列: {missing}。"
            )

        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        self._create_bar_table(duration_seconds)

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

    def get_bar_count(self, provider: str, symbol: str, duration_seconds: int) -> int:
        table_name = self.get_bar_table_name(duration_seconds)
        row = self.conn.execute(
            f"""
            SELECT count(*)
            FROM {table_name}
            WHERE provider = ? AND symbol = ? AND duration_seconds = ?
            """,
            [provider, symbol, duration_seconds],
        ).fetchone()
        return int(row[0] or 0)

    def get_5m_bar_count(self, provider: str, symbol: str) -> int:
        return self.get_bar_count(provider, symbol, 300)

    def get_latest_bar_timestamp(self, provider: str, symbol: str, duration_seconds: int) -> pd.Timestamp | None:
        table_name = self.get_bar_table_name(duration_seconds)
        row = self.conn.execute(
            f"""
            SELECT max(bar_start) AS latest_ts
            FROM {table_name}
            WHERE provider = ? AND symbol = ? AND duration_seconds = ?
            """,
            [provider, symbol, duration_seconds],
        ).fetchone()
        if not row or row[0] is None:
            return None
        return pd.Timestamp(row[0])

    def get_latest_5m_timestamp(self, provider: str, symbol: str) -> pd.Timestamp | None:
        return self.get_latest_bar_timestamp(provider, symbol, 300)

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

    def import_kline_csv(
        self,
        csv_path: Path,
        contract: TickContract,
        duration_seconds: int,
        source_start: date,
        source_end: date,
        ingest_batch_id: str,
    ) -> int:
        if not csv_path.exists() or csv_path.stat().st_size == 0:
            return 0
        table_name = self.get_bar_table_name(duration_seconds)

        open_col = _quote_identifier(f"{contract.symbol}.open")
        high_col = _quote_identifier(f"{contract.symbol}.high")
        low_col = _quote_identifier(f"{contract.symbol}.low")
        close_col = _quote_identifier(f"{contract.symbol}.close")
        volume_col = _quote_identifier(f"{contract.symbol}.volume")
        open_oi_col = _quote_identifier(f"{contract.symbol}.open_oi")
        close_oi_col = _quote_identifier(f"{contract.symbol}.close_oi")

        before_count = self.get_bar_count(contract.provider, contract.symbol, duration_seconds)
        self.conn.execute(
            f"""
            INSERT OR REPLACE INTO {table_name} (
                provider,
                symbol,
                exchange_id,
                product_id,
                instrument_name,
                trading_day,
                bar_start,
                bar_start_epoch,
                bar_id,
                open,
                high,
                low,
                close,
                volume,
                open_oi,
                close_oi,
                duration_seconds,
                source_start,
                source_end,
                ingest_batch_id,
                updated_at
            )
            SELECT
                ?,
                ?,
                ?,
                ?,
                ?,
                CAST(datetime AS DATE),
                CAST(datetime AS TIMESTAMP),
                CAST(CAST(datetime_nano AS BIGINT) / 1000000000 AS BIGINT),
                NULL,
                CAST({open_col} AS DOUBLE),
                CAST({high_col} AS DOUBLE),
                CAST({low_col} AS DOUBLE),
                CAST({close_col} AS DOUBLE),
                CAST({volume_col} AS BIGINT),
                CAST({open_oi_col} AS BIGINT),
                CAST({close_oi_col} AS BIGINT),
                ?,
                ?,
                ?,
                ?,
                CURRENT_TIMESTAMP
            FROM read_csv_auto(?, header = true)
            WHERE datetime IS NOT NULL AND datetime_nano IS NOT NULL
            """,
            [
                contract.provider,
                contract.symbol,
                contract.exchange_id,
                contract.product_id,
                contract.instrument_name,
                duration_seconds,
                source_start,
                source_end,
                ingest_batch_id,
                str(csv_path),
            ],
        )
        after_count = self.get_bar_count(contract.provider, contract.symbol, duration_seconds)
        return max(after_count - before_count, 0)

    def import_5m_kline_csv(
        self,
        csv_path: Path,
        contract: TickContract,
        source_start: date,
        source_end: date,
        ingest_batch_id: str,
    ) -> int:
        return self.import_kline_csv(
            csv_path=csv_path,
            contract=contract,
            duration_seconds=300,
            source_start=source_start,
            source_end=source_end,
            ingest_batch_id=ingest_batch_id,
        )

    def refresh_bar_table(
        self,
        provider: str,
        duration_seconds: int,
        symbols: list[str] | None = None,
    ) -> int:
        table_name = self.get_bar_table_name(duration_seconds)
        where_clauses = ["ticks.provider = ?"]
        parameters: list[object] = [provider]
        if symbols:
            placeholders = ", ".join(["?"] * len(symbols))
            where_clauses.append(f"ticks.symbol IN ({placeholders})")
            parameters.extend(symbols)

        where_sql = " AND ".join(where_clauses)
        before_count = self._get_bar_total_count(provider, duration_seconds, symbols)
        self.conn.execute(
            f"""
            INSERT OR REPLACE INTO {table_name} (
                provider,
                symbol,
                exchange_id,
                product_id,
                instrument_name,
                trading_day,
                bar_start,
                bar_start_epoch,
                bar_id,
                open,
                high,
                low,
                close,
                volume,
                open_oi,
                close_oi,
                duration_seconds,
                source_start,
                source_end,
                ingest_batch_id,
                updated_at
            )
            WITH ordered AS (
                SELECT
                    ticks.provider,
                    ticks.symbol,
                    ticks.exchange_id,
                    ticks.product_id,
                    ticks.instrument_name,
                    ticks.ts,
                    ticks.last_price,
                    CASE
                        WHEN ticks.volume IS NULL THEN 0
                        ELSE GREATEST(
                            ticks.volume - COALESCE(
                                LAG(ticks.volume) OVER (
                                    PARTITION BY ticks.provider, ticks.symbol
                                    ORDER BY ticks.ts
                                ),
                                ticks.volume
                            ),
                            0
                        )
                    END AS volume_delta,
                    FLOOR(epoch(ticks.ts) / ?) * ? AS bucket_epoch
                FROM {self.table_name} AS ticks
                WHERE {where_sql}
            ),
            bucketed AS (
                SELECT
                    provider,
                    symbol,
                    arg_min(exchange_id, ts) AS exchange_id,
                    arg_min(product_id, ts) AS product_id,
                    arg_min(instrument_name, ts) AS instrument_name,
                    CAST(to_timestamp(bucket_epoch) AS DATE) AS trading_day,
                    CAST(to_timestamp(bucket_epoch) AS TIMESTAMP) AS bar_start,
                    CAST(bucket_epoch AS BIGINT) AS bar_start_epoch,
                    arg_min(last_price, ts) AS open,
                    max(last_price) AS high,
                    min(last_price) AS low,
                    arg_max(last_price, ts) AS close,
                    CAST(sum(volume_delta) AS BIGINT) AS volume,
                    CAST(NULL AS BIGINT) AS open_oi,
                    CAST(NULL AS BIGINT) AS close_oi,
                    min(CAST(ts AS DATE)) AS source_start,
                    max(CAST(ts AS DATE)) AS source_end,
                    'derived_from_ticks' AS ingest_batch_id
                FROM ordered
                GROUP BY provider, symbol, bucket_epoch
            )
            SELECT
                provider,
                symbol,
                exchange_id,
                product_id,
                instrument_name,
                trading_day,
                bar_start,
                bar_start_epoch,
                NULL,
                open,
                high,
                low,
                close,
                volume,
                open_oi,
                close_oi,
                ?,
                source_start,
                source_end,
                ingest_batch_id,
                CURRENT_TIMESTAMP
            FROM bucketed
            """,
            [duration_seconds, duration_seconds, *parameters, duration_seconds],
        )
        after_count = self._get_bar_total_count(provider, duration_seconds, symbols)
        return max(after_count - before_count, 0)

    def refresh_5m_bars(self, provider: str, symbols: list[str] | None = None) -> int:
        return self.refresh_bar_table(provider=provider, duration_seconds=300, symbols=symbols)

    def _get_bar_total_count(self, provider: str, duration_seconds: int, symbols: list[str] | None = None) -> int:
        table_name = self.get_bar_table_name(duration_seconds)
        where_clauses = ["provider = ?"]
        parameters: list[object] = [provider]
        if symbols:
            placeholders = ", ".join(["?"] * len(symbols))
            where_clauses.append(f"symbol IN ({placeholders})")
            parameters.extend(symbols)
        where_clauses.append("duration_seconds = ?")
        parameters.append(duration_seconds)
        where_sql = " AND ".join(where_clauses)
        row = self.conn.execute(
            f"""
            SELECT count(*)
            FROM {table_name}
            WHERE {where_sql}
            """,
            parameters,
        ).fetchone()
        return int(row[0] or 0)

    def _get_5m_total_count(self, provider: str, symbols: list[str] | None = None) -> int:
        return self._get_bar_total_count(provider, 300, symbols)


def format_contract_label(symbol: str, instrument_name: str | None = None) -> str:
    readable_name = (instrument_name or "").strip()
    if not readable_name:
        return symbol
    return f"{readable_name} · {symbol}"


def format_contract_month(delivery_year: int | None, delivery_month: int | None) -> str:
    if delivery_year is None or delivery_month is None:
        return ""
    return f"{delivery_year:04d}-{delivery_month:02d}"
