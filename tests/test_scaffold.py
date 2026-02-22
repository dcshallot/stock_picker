"""Minimal scaffold tests."""

from __future__ import annotations

import pandas as pd

from stock_picker.data.normalize import BARS_SCHEMA_COLUMNS, normalize_bars
from stock_picker.universe.load_watchlist import load_watchlist


def test_load_watchlist_reads_required_columns(tmp_path):
    csv_path = tmp_path / "watchlist.csv"
    csv_path.write_text(
        "symbol,market,currency,tags\nAAPL,US,USD,tech\nMSFT,US,USD,cloud\n",
        encoding="utf-8",
    )

    df = load_watchlist(csv_path)

    assert list(df.columns) == ["symbol", "market", "currency", "tags"]
    assert len(df) == 2
    assert df.iloc[0]["symbol"] == "AAPL"


def test_normalize_bars_has_required_columns():
    raw = pd.DataFrame(
        {
            "timestamp": ["2025-01-01T00:00:00Z"],
            "symbol": ["AAPL"],
            "market": ["US"],
            "currency": ["USD"],
            "timeframe": ["1D"],
            "open": [100],
            "high": [101],
            "low": [99],
            "close": [100.5],
            "volume": [100000],
        }
    )

    out = normalize_bars(raw, source="unit")

    assert set(BARS_SCHEMA_COLUMNS).issubset(set(out.columns))
    assert out.iloc[0]["symbol"] == "AAPL"
    assert str(out.iloc[0]["ts_utc"].tzinfo) in {"UTC", "UTC+00:00"}
