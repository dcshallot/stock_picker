"""IBKR TWS/Gateway connector stub implementation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from stock_picker.brokers.base import BrokerCapabilities, BrokerConnector


class IbkrTwsConnector(BrokerConnector):
    """Stub connector for IBKR TWS/Gateway.

    Future implementation point:
    - map generic symbol to IBKR contract objects
    - support historical data endpoint and pacing constraints
    """

    name = "ibkr_tws"

    def capabilities_check(self) -> BrokerCapabilities:
        return BrokerCapabilities(
            broker_name=self.name,
            supports_bars=True,
            supports_quotes=True,
            delayed_data=True,
            permission_ok=True,
            notes=["stub_connector", "assume_delayed_if_no_subscription"],
        )

    def resolve_instruments(self, universe_df: pd.DataFrame) -> dict[str, Any]:
        mapping: dict[str, Any] = {}
        for _, row in universe_df.iterrows():
            symbol = str(row["symbol"])
            mapping[symbol] = {
                "contract_hint": {
                    "symbol": symbol,
                    "exchange": str(row.get("market", "SMART")),
                }
            }
        return mapping

    def fetch_bars(self, request: dict[str, Any]) -> pd.DataFrame:
        universe_df: pd.DataFrame = request.get("universe", pd.DataFrame())
        rows: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

        for i, row in universe_df.reset_index(drop=True).iterrows():
            base = 90 + i * 4
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
                        "high": base + d + 1,
                        "low": base + d - 1,
                        "close": base + d + 0.4,
                        "volume": 80000 + (d * 3000),
                        "source": self.name,
                    }
                )

        return pd.DataFrame(rows)

    def fetch_quotes(self, request: dict[str, Any]) -> pd.DataFrame:
        universe_df: pd.DataFrame = request.get("universe", pd.DataFrame())
        rows: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc).isoformat()

        for i, row in universe_df.reset_index(drop=True).iterrows():
            px = 90 + i * 4
            rows.append(
                {
                    "timestamp": now,
                    "symbol": row["symbol"],
                    "market": row.get("market", "UNKNOWN"),
                    "currency": row.get("currency", "USD"),
                    "bid": px - 0.2,
                    "ask": px + 0.2,
                    "last": px,
                    "source": self.name,
                    "delayed": True,
                }
            )

        return pd.DataFrame(rows)
