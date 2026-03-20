from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tqsdk import TqApi, TqAuth
from tqsdk.tools import DataDownloader


@dataclass(frozen=True)
class TqAuthConfig:
    user: str
    password: str


def load_tq_auth(project_root: Path) -> TqAuthConfig:
    load_dotenv(project_root / ".env")
    user = os.getenv("TQ_USER")
    password = os.getenv("TQ_PASSWORD")
    if not user or not password:
        raise RuntimeError("请先在 .env 中设置 TQ_USER 和 TQ_PASSWORD。")
    return TqAuthConfig(user=user, password=password)


class TqTickHistoryDownloader:
    def __init__(self, auth: TqAuthConfig) -> None:
        self.api = TqApi(auth=TqAuth(auth.user, auth.password))

    def close(self) -> None:
        self.api.close()

    def download_tick_csv(self, symbol: str, start_dt: date, end_dt: date, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        downloader = DataDownloader(
            api=self.api,
            symbol_list=symbol,
            dur_sec=0,
            start_dt=start_dt,
            end_dt=end_dt,
            csv_file_name=str(output_path),
        )
        while not downloader.is_finished():
            self.api.wait_update()

    def query_futures_contracts(
        self,
        exchange_id: str,
        product_id: str,
        include_expired: bool = True,
    ) -> list[dict[str, str]]:
        symbols = list(
            self.api.query_quotes(
                ins_class="FUTURE",
                exchange_id=exchange_id,
                product_id=product_id,
                expired=include_expired,
            )
        )
        if not symbols:
            return []

        info_df = self.api.query_symbol_info(symbols)
        contracts: list[dict[str, str]] = []
        for row in info_df.itertuples(index=False):
            expire_timestamp = None
            raw_expire = getattr(row, "expire_datetime", None)
            if raw_expire:
                expire_timestamp = pd.to_datetime(raw_expire, unit="s", errors="coerce")
            contracts.append(
                {
                    "symbol": str(row.instrument_id),
                    "name": str(row.instrument_name or "").strip() or str(row.instrument_id),
                    "exchange_id": str(row.exchange_id),
                    "product_id": str(row.product_id),
                    "ins_class": str(getattr(row, "ins_class", "") or ""),
                    "underlying_symbol": str(getattr(row, "underlying_symbol", "") or ""),
                    "price_tick": float(getattr(row, "price_tick", 0) or 0) or None,
                    "volume_multiple": float(getattr(row, "volume_multiple", 0) or 0) or None,
                    "delivery_year": int(getattr(row, "delivery_year", 0) or 0) or None,
                    "delivery_month": int(getattr(row, "delivery_month", 0) or 0) or None,
                    "expire_datetime": expire_timestamp,
                    "expired": bool(getattr(row, "expired", False)),
                }
            )
        contracts.sort(key=lambda item: item["symbol"])
        return contracts

    def query_contracts_by_symbols(self, symbols: list[str]) -> list[dict[str, str]]:
        normalized = [symbol.strip() for symbol in symbols if symbol and symbol.strip()]
        if not normalized:
            return []

        info_df = self.api.query_symbol_info(normalized)
        contracts: list[dict[str, str]] = []
        for row in info_df.itertuples(index=False):
            expire_timestamp = None
            raw_expire = getattr(row, "expire_datetime", None)
            if raw_expire:
                expire_timestamp = pd.to_datetime(raw_expire, unit="s", errors="coerce")
            contracts.append(
                {
                    "symbol": str(row.instrument_id),
                    "name": str(row.instrument_name or "").strip() or str(row.instrument_id),
                    "exchange_id": str(row.exchange_id),
                    "product_id": str(row.product_id),
                    "ins_class": str(getattr(row, "ins_class", "") or ""),
                    "underlying_symbol": str(getattr(row, "underlying_symbol", "") or ""),
                    "price_tick": float(getattr(row, "price_tick", 0) or 0) or None,
                    "volume_multiple": float(getattr(row, "volume_multiple", 0) or 0) or None,
                    "delivery_year": int(getattr(row, "delivery_year", 0) or 0) or None,
                    "delivery_month": int(getattr(row, "delivery_month", 0) or 0) or None,
                    "expire_datetime": expire_timestamp,
                    "expired": bool(getattr(row, "expired", False)),
                }
            )
        contracts.sort(key=lambda item: item["symbol"])
        return contracts
