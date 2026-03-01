"""Watchlist loader.

Expected CSV columns:
- symbol
- market
- currency
- tags
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

REQUIRED_WATCHLIST_COLUMNS = ["symbol", "market", "currency", "tags"]
DEFAULT_CURRENCY_BY_MARKET = {
    "US": "USD",
    "HK": "HKD",
    "EU": "EUR",
    "CN": "CNY",
    "JP": "JPY",
}


def load_watchlist(path: str | Path) -> pd.DataFrame:
    """Load watchlist CSV and validate required columns."""

    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Watchlist file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    missing = [c for c in REQUIRED_WATCHLIST_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Watchlist missing required columns: {missing}")

    out = df[REQUIRED_WATCHLIST_COLUMNS].copy()
    out["symbol"] = out["symbol"].astype(str).str.strip()
    out["market"] = out["market"].astype(str).str.upper().str.strip()
    out["currency"] = out["currency"].astype(str).str.upper().str.strip()
    out["tags"] = out["tags"].astype(str).fillna("")
    return out


def load_symbols(symbol_specs: list[str]) -> pd.DataFrame:
    """Build a watchlist-like dataframe from explicit symbol specs.

    Supported formats:
    - ``US.AAPL``
    - ``US.AAPL:USD``
    - ``HK.00700``

    The first ``.`` splits market from the remaining broker-facing symbol text.
    """

    rows: list[dict[str, str]] = []

    for raw in symbol_specs:
        item = str(raw).strip()
        if not item:
            continue

        currency = ""
        if ":" in item:
            item, currency = item.rsplit(":", 1)
            currency = currency.strip().upper()

        if "." not in item:
            raise ValueError(
                f"Invalid symbol spec: {raw}. Expected formats like US.AAPL or HK.00700:HKD."
            )

        market, symbol = item.split(".", 1)
        market = market.strip().upper()
        symbol = symbol.strip()
        if not market or not symbol:
            raise ValueError(
                f"Invalid symbol spec: {raw}. Expected formats like US.AAPL or HK.00700:HKD."
            )

        rows.append(
            {
                "symbol": symbol,
                "market": market,
                "currency": currency or DEFAULT_CURRENCY_BY_MARKET.get(market, "UNKNOWN"),
                "tags": "cli_input",
            }
        )

    return pd.DataFrame(rows, columns=REQUIRED_WATCHLIST_COLUMNS)
