"""IBKR Client Portal connector stub implementation."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from stock_picker.brokers.base import (
    BrokerCapabilities,
    BrokerConnector,
    BrokerDataRequest,
    NotSupported,
    PermissionDenied,
)


class IbkrCpConnector(BrokerConnector):
    """Stub connector for IBKR Client Portal REST API.

    Future implementation point:
    - implement session auth and account context
    - map symbol -> conid and request snapshots/history
    """

    name = "ibkr_cp"

    def capabilities_check(self) -> BrokerCapabilities:
        api_key = str(self.config.get("api_key", "")).strip()
        permission_ok = bool(api_key) and not api_key.startswith("<")
        notes = ["stub_connector", "requires_api_key"]
        if not permission_ok:
            notes.append("api_key_missing_or_placeholder")

        return BrokerCapabilities(
            broker_name=self.name,
            supports_bars=False,
            supports_quotes=True,
            delayed_data=True,
            permission_ok=permission_ok,
            notes=notes,
        )

    def resolve_instruments(self, universe_df: pd.DataFrame) -> dict[str, Any]:
        return {str(sym): {"conid": None} for sym in universe_df["symbol"].tolist()}

    def _assert_api_key(self) -> None:
        api_key = str(self.config.get("api_key", "")).strip()
        if not api_key or api_key.startswith("<"):
            raise PermissionDenied("IBKR CP api_key is missing. Set a real key in config.")

    def fetch_bars(self, request: BrokerDataRequest) -> pd.DataFrame:
        raise NotSupported("IBKR CP bar endpoint is not implemented in this scaffold.")

    def fetch_quotes(self, request: BrokerDataRequest) -> pd.DataFrame:
        self._assert_api_key()

        universe_df = pd.DataFrame(request.universe)
        now = datetime.now(timezone.utc).isoformat()
        rows: list[dict[str, Any]] = []
        for i, row in universe_df.reset_index(drop=True).iterrows():
            px = 95 + i * 3
            rows.append(
                {
                    "timestamp": now,
                    "symbol": row["symbol"],
                    "market": row.get("market", "UNKNOWN"),
                    "currency": row.get("currency", "USD"),
                    "bid": px - 0.15,
                    "ask": px + 0.15,
                    "last": px,
                    "source": self.name,
                    "delayed": True,
                }
            )

        return pd.DataFrame(rows)
