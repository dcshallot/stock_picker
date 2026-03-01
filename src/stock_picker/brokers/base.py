"""Broker connector abstraction and shared error types.

Data adapters implement BrokerConnector. Strategy modules should not depend on
broker-specific details directly.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Mapping

import pandas as pd
from pydantic import BaseModel, Field


class BrokerError(Exception):
    """Base class for broker adapter errors."""


class PermissionDenied(BrokerError):
    """Raised when broker account lacks required market permissions."""


class RateLimited(BrokerError):
    """Raised when broker request rate limit is exceeded."""


class NotSupported(BrokerError):
    """Raised when a broker does not support a requested capability."""


class BrokerCapabilities(BaseModel):
    """Capability probe result for a broker adapter."""

    broker_name: str
    supports_bars: bool = True
    supports_quotes: bool = True
    delayed_data: bool = False
    permission_ok: bool = True
    notes: list[str] = Field(default_factory=list)


class BrokerDataRequest(BaseModel):
    """Broker fetch request contract.

    This object carries the minimum shared context needed by adapters and cache
    key generation. Future implementations can extend it with paging windows,
    entitlement scope, or exchange-specific knobs.
    """

    universe: list[dict[str, Any]] = Field(default_factory=list)
    mapping: dict[str, Any] = Field(default_factory=dict)
    timeframe: str = "1D"
    adjustment: str = "forward"
    start_date: date | None = None
    end_date: date | None = None

    def symbol_list(self) -> list[str]:
        """Return stable ordered symbols for cache signatures and logging."""

        symbols = [str(row.get("symbol", "")).strip() for row in self.universe]
        return [symbol for symbol in symbols if symbol]


class BrokerConnector(ABC):
    """Abstract broker connector interface.

    Methods return broker-native or semi-normalized dataframes depending on
    implementation. Normalization is handled by data.normalize.
    """

    name: str = "base"

    def __init__(self, config: Mapping[str, Any] | None = None) -> None:
        self.config = dict(config or {})
        self.last_fetch_notes: dict[str, Any] = {}

    def build_cache_key(self, dataset: str, request: BrokerDataRequest) -> str:
        """Build a deterministic cache key for a dataset request.

        Adapters can override this when request parameters must be translated to
        broker-specific cache segmentation.
        """

        start = request.start_date.isoformat() if request.start_date else "na"
        end = request.end_date.isoformat() if request.end_date else "na"
        symbols = request.symbol_list()
        first = symbols[0] if symbols else "empty"
        last = symbols[-1] if symbols else "empty"
        return (
            f"{dataset}__{request.timeframe}__{request.adjustment}"
            f"__{start}__{end}__n{len(symbols)}__{first}__{last}"
        )

    @abstractmethod
    def capabilities_check(self) -> BrokerCapabilities:
        """Probe broker capabilities for diagnostics and graceful degradation."""

    @abstractmethod
    def resolve_instruments(self, universe_df: pd.DataFrame) -> dict[str, Any]:
        """Map generic symbols to broker-specific instrument identifiers."""

    @abstractmethod
    def fetch_bars(self, request: BrokerDataRequest) -> pd.DataFrame:
        """Fetch OHLCV bars (stub for this scaffold)."""

    @abstractmethod
    def fetch_quotes(self, request: BrokerDataRequest) -> pd.DataFrame:
        """Fetch quote snapshots (stub for this scaffold)."""
