from __future__ import annotations

import os
from pathlib import Path
from typing import Any

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
