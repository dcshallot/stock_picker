"""Universe rule loader and screener stub.

This module provides a placeholder for rule-driven universe generation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yaml

_DEMO_SYMBOLS_BY_MARKET: dict[str, list[tuple[str, str]]] = {
    "US": [("AAPL", "USD"), ("MSFT", "USD")],
    "HK": [("0700.HK", "HKD")],
    "EU": [("SAP.DE", "EUR")],
    "CN": [("600519.SS", "CNY")],
}


def load_rules(path: str | Path) -> dict[str, Any]:
    """Load universe YAML rules."""

    rules_path = Path(path)
    if not rules_path.exists():
        raise FileNotFoundError(f"Rules file not found: {rules_path}")

    return yaml.safe_load(rules_path.read_text(encoding="utf-8")) or {}


def screen_universe_from_rules(path: str | Path) -> pd.DataFrame:
    """Return a demo universe based on market whitelist in rules.

    Current behavior is intentionally simple and deterministic for scaffold
    validation. Future implementation should apply real rule screening logic.
    """

    rules = load_rules(path)
    markets = rules.get("market_whitelist") or ["US"]

    rows: list[dict[str, str]] = []
    for market in markets:
        for symbol, currency in _DEMO_SYMBOLS_BY_MARKET.get(str(market).upper(), []):
            rows.append(
                {
                    "symbol": symbol,
                    "market": str(market).upper(),
                    "currency": currency,
                    "tags": "from_rules",
                }
            )

    return pd.DataFrame(rows, columns=["symbol", "market", "currency", "tags"])
