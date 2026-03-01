"""Tests for incremental history sync and the Parquet history store."""

from __future__ import annotations

from datetime import date, timezone

import pandas as pd

from stock_picker.data.historical_store import HistoricalStore
from stock_picker.data.history_sync import SyncPlan, SyncWindow, build_sync_plans, sync_historical_bars
from stock_picker.providers.base import (
    HistoricalBarsProvider,
    HistoricalBarsRequest,
    HistoricalBarsResult,
    ProviderCapabilities,
    ProviderRegistry,
)


def _bars_frame(
    symbol: str,
    market: str,
    currency: str,
    start_date: date,
    end_date: date,
    *,
    source: str = "yahoo",
    close_bias: float = 0.0,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for offset, ts in enumerate(pd.bdate_range(start_date, end_date, inclusive="both")):
        stamp = pd.Timestamp(ts).tz_localize(timezone.utc)
        close = 100.0 + close_bias + offset
        rows.append(
            {
                "ts_utc": stamp,
                "symbol": symbol,
                "market": market,
                "currency": currency,
                "timeframe": "1D",
                "adjustment": "forward",
                "open": close - 0.5,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 100000 + offset,
                "turnover": None,
                "source": source,
                "quality_flags": "",
            }
        )
    return pd.DataFrame(rows)


class _StaticProvider(HistoricalBarsProvider):
    name = "static"

    def __init__(self, provider_name: str, frame: pd.DataFrame) -> None:
        super().__init__({})
        self.name = provider_name
        self._frame = frame

    def capabilities_check(self) -> ProviderCapabilities:
        return ProviderCapabilities(provider_name=self.name)

    def resolve_symbol(self, symbol: str, market: str, currency: str) -> str:
        return symbol

    def fetch_daily_bars(self, request: HistoricalBarsRequest) -> HistoricalBarsResult:
        frame = self._frame.copy()
        if not frame.empty:
            frame = frame[
                (pd.to_datetime(frame["ts_utc"], utc=True).dt.date >= request.start_date)
                & (pd.to_datetime(frame["ts_utc"], utc=True).dt.date <= request.end_date)
            ].copy()
            frame["timestamp"] = pd.to_datetime(frame["ts_utc"], utc=True).astype(str)
        return HistoricalBarsResult(rows=frame, provider=self.name)


class _TimeoutProvider(HistoricalBarsProvider):
    name = "timeout"

    def __init__(self, provider_name: str = "timeout") -> None:
        super().__init__({})
        self.name = provider_name

    def capabilities_check(self) -> ProviderCapabilities:
        return ProviderCapabilities(provider_name=self.name)

    def resolve_symbol(self, symbol: str, market: str, currency: str) -> str:
        return symbol

    def fetch_daily_bars(self, request: HistoricalBarsRequest) -> HistoricalBarsResult:
        raise TimeoutError("temporary timeout")


def test_build_sync_plans_uses_bootstrap_floor(tmp_path):
    store = HistoricalStore(tmp_path / "history_store")
    universe_df = pd.DataFrame(
        [{"symbol": "AAPL", "market": "US", "currency": "USD", "tags": "x"}]
    )
    routing_diag = {
        "assigned_symbols": [
            {
                "symbol": "AAPL",
                "market": "US",
                "provider": "yahoo",
                "fallback_providers": [],
            }
        ]
    }

    plans = build_sync_plans(
        universe_df,
        routing_diag,
        store=store,
        timeframe="1D",
        adjustment="forward",
        start_date=date(2016, 1, 1),
        end_date=date(2026, 3, 1),
        bootstrap_start_date=date(2018, 1, 1),
        repair_window_days=30,
        max_gap_days_before_full_resync=365,
    )

    assert len(plans) == 1
    assert plans[0].requested_start == date(2018, 1, 1)
    assert plans[0].windows[0].start_date == date(2018, 1, 1)
    assert plans[0].windows[0].reason == "bootstrap"


def test_build_sync_plans_uses_repair_and_append_window(tmp_path):
    store = HistoricalStore(tmp_path / "history_store")
    store.upsert_bars(
        _bars_frame(
            "AAPL",
            "US",
            "USD",
            date(2026, 2, 20),
            date(2026, 2, 24),
        )
    )
    universe_df = pd.DataFrame(
        [{"symbol": "AAPL", "market": "US", "currency": "USD", "tags": "x"}]
    )
    routing_diag = {
        "assigned_symbols": [
            {
                "symbol": "AAPL",
                "market": "US",
                "provider": "yahoo",
                "fallback_providers": [],
            }
        ]
    }

    plans = build_sync_plans(
        universe_df,
        routing_diag,
        store=store,
        timeframe="1D",
        adjustment="forward",
        start_date=date(2026, 2, 22),
        end_date=date(2026, 2, 28),
        bootstrap_start_date=date(2018, 1, 1),
        repair_window_days=3,
        max_gap_days_before_full_resync=365,
    )

    assert plans[0].windows[0].start_date == date(2026, 2, 22)
    assert plans[0].windows[0].end_date == date(2026, 2, 28)
    assert plans[0].windows[0].reason == "repair_and_append"


def test_history_store_deduplicates_overlapping_rows(tmp_path):
    store = HistoricalStore(tmp_path / "history_store")
    store.upsert_bars(
        _bars_frame(
            "AAPL",
            "US",
            "USD",
            date(2026, 2, 24),
            date(2026, 2, 24),
            source="yahoo",
            close_bias=0.0,
        )
    )
    store.upsert_bars(
        _bars_frame(
            "AAPL",
            "US",
            "USD",
            date(2026, 2, 24),
            date(2026, 2, 24),
            source="futu",
            close_bias=5.0,
        )
    )

    loaded = store.load_symbol_bars(
        symbol="AAPL",
        market="US",
        timeframe="1D",
        adjustment="forward",
    )

    assert len(loaded) == 1
    assert float(loaded.iloc[0]["close"]) == 105.0
    assert loaded.iloc[0]["source"] == "futu"


def test_sync_historical_bars_falls_back_to_second_provider(tmp_path):
    store = HistoricalStore(tmp_path / "history_store")
    backup_rows = _bars_frame(
        "AAPL",
        "US",
        "USD",
        date(2026, 2, 24),
        date(2026, 2, 25),
        source="backup",
    )
    registry = ProviderRegistry(
        [
            _TimeoutProvider("primary"),
            _StaticProvider("backup", backup_rows),
        ]
    )
    plans = [
        SyncPlan(
            symbol="AAPL",
            market="US",
            currency="USD",
            timeframe="1D",
            adjustment="forward",
            requested_start=date(2026, 2, 24),
            requested_end=date(2026, 2, 25),
            providers=["primary", "backup"],
            windows=[
                SyncWindow(
                    start_date=date(2026, 2, 24),
                    end_date=date(2026, 2, 25),
                    reason="bootstrap",
                )
            ],
        )
    ]

    bars_df, diagnostics = sync_historical_bars(
        plans,
        registry=registry,
        store=store,
        run_id="run_test",
    )

    assert len(bars_df) == 2
    assert diagnostics["brokers"]["primary"]["bars_details"]["errors"][0]["symbol"] == "AAPL"
    assert diagnostics["brokers"]["backup"]["bars_details"]["ok"][0]["symbol"] == "AAPL"

    sync_runs = pd.read_parquet(store.sync_runs_path())
    assert sync_runs["provider"].tolist() == ["primary", "backup"]
    assert sync_runs["status"].tolist() == ["failed", "success"]
