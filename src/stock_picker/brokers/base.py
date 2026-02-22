"""Broker connector abstraction and shared error types.

Data adapters implement BrokerConnector. Strategy modules should not depend on
broker-specific details directly.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
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


class BrokerConnector(ABC):
    """Abstract broker connector interface.

    Methods return broker-native or semi-normalized dataframes depending on
    implementation. Normalization is handled by data.normalize.
    """

    name: str = "base"

    def __init__(self, config: Mapping[str, Any] | None = None) -> None:
        self.config = dict(config or {})

    @abstractmethod
    def capabilities_check(self) -> BrokerCapabilities:
        """Probe broker capabilities for diagnostics and graceful degradation."""

    @abstractmethod
    def resolve_instruments(self, universe_df: pd.DataFrame) -> dict[str, Any]:
        """Map generic symbols to broker-specific instrument identifiers."""

    @abstractmethod
    def fetch_bars(self, request: dict[str, Any]) -> pd.DataFrame:
        """Fetch OHLCV bars (stub for this scaffold)."""

    @abstractmethod
    def fetch_quotes(self, request: dict[str, Any]) -> pd.DataFrame:
        """Fetch quote snapshots (stub for this scaffold)."""
