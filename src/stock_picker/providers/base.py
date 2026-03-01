"""Generic historical-bars provider abstractions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field


class ProviderCapabilities(BaseModel):
    """Capability probe result for a market-data provider."""

    provider_name: str
    supports_history_bars: bool = True
    available: bool = True
    requires_network: bool = False
    notes: list[str] = Field(default_factory=list)


class HistoricalBarsRequest(BaseModel):
    """Canonical request contract for one symbol's daily bars."""

    symbol: str
    market: str
    currency: str
    start_date: date
    end_date: date
    timeframe: str = "1D"
    adjustment: str = "forward"


class HistoricalBarsResult(BaseModel):
    """Provider response contract for one symbol's daily bars."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    rows: pd.DataFrame
    provider: str
    partial: bool = False
    coverage_start: date | None = None
    coverage_end: date | None = None
    notes: dict[str, Any] = Field(default_factory=dict)


class HistoricalBarsProvider(ABC):
    """Abstract interface for historical daily-bar providers."""

    name: str = "base"

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = dict(config or {})
        self.last_fetch_notes: dict[str, Any] = {}

    @abstractmethod
    def capabilities_check(self) -> ProviderCapabilities:
        """Probe provider capabilities for diagnostics and graceful degradation."""

    @abstractmethod
    def resolve_symbol(self, symbol: str, market: str, currency: str) -> str:
        """Map a generic symbol into the provider-specific symbol syntax."""

    @abstractmethod
    def fetch_daily_bars(self, request: HistoricalBarsRequest) -> HistoricalBarsResult:
        """Fetch one symbol's daily OHLCV history for the requested date range."""


class ProviderRegistry:
    """Small provider registry for sync orchestration."""

    def __init__(self, providers: list[HistoricalBarsProvider] | None = None) -> None:
        self._providers: dict[str, HistoricalBarsProvider] = {}
        for provider in providers or []:
            self.register(provider)

    def register(self, provider: HistoricalBarsProvider) -> None:
        self._providers[provider.name] = provider

    def get(self, name: str) -> HistoricalBarsProvider | None:
        return self._providers.get(name)

    def names(self) -> list[str]:
        return list(self._providers.keys())

    def values(self) -> list[HistoricalBarsProvider]:
        return list(self._providers.values())
