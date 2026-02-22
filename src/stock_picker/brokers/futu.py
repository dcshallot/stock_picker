"""Futu connector stub implementation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from stock_picker.brokers.base import BrokerCapabilities, BrokerConnector


class FutuConnector(BrokerConnector):
    """Stub connector for Futu OpenD.

    Future implementation point:
    - replace generated demo data with real OpenD calls
    - apply permission checks by market
    - respect timeframe/adjustment options
    """

    name = "futu"

    def capabilities_check(self) -> BrokerCapabilities:
        return BrokerCapabilities(
            broker_name=self.name,
            supports_bars=True,
            supports_quotes=True,
            delayed_data=False,
            permission_ok=True,
            notes=["stub_connector"],
        )

    def resolve_instruments(self, universe_df: pd.DataFrame) -> dict[str, Any]:
        mapping: dict[str, Any] = {}
        for _, row in universe_df.iterrows():
            symbol = str(row["symbol"])
            mapping[symbol] = {"broker_symbol": symbol, "market": row.get("market", "UNKNOWN")}
        return mapping

    def fetch_bars(self, request: dict[str, Any]) -> pd.DataFrame:
        universe_df: pd.DataFrame = request.get("universe", pd.DataFrame())
        rows: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

        for i, row in universe_df.reset_index(drop=True).iterrows():
            base = 100 + i * 5
            for d in range(5):
                ts = now - timedelta(days=5 - d)
                rows.append(
                    {
                        "timestamp": ts.isoformat(),
                        "symbol": row["symbol"],
                        "market": row.get("market", "UNKNOWN"),
                        "currency": row.get("currency", "USD"),
                        "timeframe": "1D",
                        "open": base + d,
                        "high": base + d + 1.5,
                        "low": base + d - 1.5,
                        "close": base + d + 0.7,
                        "volume": 100000 + (d * 5000),
                        "source": self.name,
                    }
                )

        return pd.DataFrame(rows)

    def fetch_quotes(self, request: dict[str, Any]) -> pd.DataFrame:
        universe_df: pd.DataFrame = request.get("universe", pd.DataFrame())
        rows: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc).isoformat()

        for i, row in universe_df.reset_index(drop=True).iterrows():
            px = 100 + i * 5
            rows.append(
                {
                    "timestamp": now,
                    "symbol": row["symbol"],
                    "market": row.get("market", "UNKNOWN"),
                    "currency": row.get("currency", "USD"),
                    "bid": px - 0.1,
                    "ask": px + 0.1,
                    "last": px,
                    "source": self.name,
                }
            )

        return pd.DataFrame(rows)
