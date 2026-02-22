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
