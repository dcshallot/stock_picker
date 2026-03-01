"""Futu adapter for the generic historical-bars provider interface."""

from __future__ import annotations

import pandas as pd

from stock_picker.brokers.base import BrokerDataRequest
from stock_picker.brokers.futu import FutuConnector
from stock_picker.providers.base import (
    HistoricalBarsProvider,
    HistoricalBarsRequest,
    HistoricalBarsResult,
    ProviderCapabilities,
)


class FutuHistoricalBarsProvider(HistoricalBarsProvider):
    """Bridge the existing Futu connector into the historical-bars interface."""

    name = "futu"

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        self.connector = FutuConnector(config or {})

    def capabilities_check(self) -> ProviderCapabilities:
        cap = self.connector.capabilities_check()
        return ProviderCapabilities(
            provider_name=self.name,
            supports_history_bars=cap.supports_bars,
            available=cap.permission_ok,
            requires_network=True,
            notes=list(cap.notes),
        )

    def resolve_symbol(self, symbol: str, market: str, currency: str) -> str:
        return self.connector._to_futu_code(symbol, market)  # noqa: SLF001

    def fetch_daily_bars(self, request: HistoricalBarsRequest) -> HistoricalBarsResult:
        mapping = {
            request.symbol: {
                "broker_symbol": self.resolve_symbol(
                    request.symbol,
                    request.market,
                    request.currency,
                ),
                "market": request.market,
            }
        }
        broker_request = BrokerDataRequest(
            universe=[
                {
                    "symbol": request.symbol,
                    "market": request.market,
                    "currency": request.currency,
                }
            ],
            mapping=mapping,
            timeframe=request.timeframe,
            adjustment=request.adjustment,
            start_date=request.start_date,
            end_date=request.end_date,
        )
        rows = self.connector.fetch_bars(broker_request)
        if rows is None:
            rows = pd.DataFrame()
        self.last_fetch_notes = dict(self.connector.last_fetch_notes)
        return HistoricalBarsResult(
            rows=rows,
            provider=self.name,
            partial=False,
            coverage_start=request.start_date,
            coverage_end=request.end_date,
            notes=self.last_fetch_notes.get("bars", {}),
        )
