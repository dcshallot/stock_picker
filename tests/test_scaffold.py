"""Minimal scaffold tests."""

from __future__ import annotations

from datetime import date

import pandas as pd

from stock_picker.brokers.base import BrokerDataRequest
from stock_picker.brokers.futu import FutuConnector
from stock_picker.cli.migrate_legacy_cache import migrate_legacy_bars_cache
from stock_picker.config.loader import load_config
from stock_picker.config.schema import AppConfig
from stock_picker.data.historical_store import HistoricalStore
from stock_picker.data.normalize import BARS_SCHEMA_COLUMNS, normalize_bars
from stock_picker.data.router import resolve_provider_assignments
from stock_picker.report.render_md import render_portfolio_markdown
from stock_picker.universe.load_watchlist import load_symbols, load_watchlist


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
            "turnover": [10050000],
        }
    )

    out = normalize_bars(raw, source="unit")

    assert set(BARS_SCHEMA_COLUMNS).issubset(set(out.columns))
    assert out.iloc[0]["symbol"] == "AAPL"
    assert str(out.iloc[0]["ts_utc"].tzinfo) in {"UTC", "UTC+00:00"}
    assert out.iloc[0]["ts_utc"].hour == 0
    assert out.iloc[0]["adjustment"] == "forward"
    assert out.iloc[0]["turnover"] == 10050000


def test_normalize_bars_drops_weekend_daily_rows():
    raw = pd.DataFrame(
        {
            "timestamp": ["2025-01-03T00:00:00Z", "2025-01-04T00:00:00Z"],
            "symbol": ["AAPL", "AAPL"],
            "market": ["US", "US"],
            "currency": ["USD", "USD"],
            "timeframe": ["1D", "1D"],
            "open": [100, 101],
            "high": [101, 102],
            "low": [99, 100],
            "close": [100.5, 101.5],
            "volume": [100000, 100000],
            "turnover": [10050000, 10150000],
        }
    )

    out = normalize_bars(raw, source="unit")

    assert len(out) == 1
    assert out.iloc[0]["ts_utc"].dayofweek == 4


def test_migrate_legacy_bars_cache_imports_json_into_history_store(tmp_path):
    legacy_dir = tmp_path / "data_cache"
    raw_dir = legacy_dir / "raw" / "futu"
    raw_dir.mkdir(parents=True)
    (raw_dir / "bars.json").write_text(
        '[{"timestamp":"2025-01-01T00:00:00Z","symbol":"AAPL","market":"US","currency":"USD","timeframe":"1D","open":100,"high":101,"low":99,"close":100.5,"volume":100000,"source":"futu"}]',
        encoding="utf-8",
    )
    store_dir = tmp_path / "history_store"

    summary = migrate_legacy_bars_cache(legacy_dir, store_dir)

    assert summary["migrated_rows"] == 1
    store = HistoricalStore(store_dir)
    bars = store.load_symbol_bars(
        symbol="AAPL",
        market="US",
        timeframe="1D",
        adjustment="forward",
    )
    assert len(bars) == 1
    assert bars.iloc[0]["symbol"] == "AAPL"
    assert bars.iloc[0]["source"] == "legacy_futu"


def test_futu_cache_key_uses_request_context():
    connector = FutuConnector({"host": "127.0.0.1", "port": 11111})
    request = BrokerDataRequest(
        universe=[{"symbol": "AAPL", "market": "US", "currency": "USD"}],
        timeframe="1D",
        adjustment="forward",
    )

    key = connector.build_cache_key("bars", request)

    assert "bars__futu__US__1D__forward" in key


def test_futu_history_quota_payload_normalization():
    payload = (
        2,
        998,
        {"code": "HK.00700", "name": "Tencent", "request_time": "2026-03-01 10:00:00"},
        {"code": "HK.09988", "name": "Alibaba", "request_time": "2026-03-01 10:01:00"},
    )

    out = FutuConnector._normalize_history_kline_quota_data(payload)

    assert out["used_quota"] == 2
    assert out["remain_quota"] == 998
    assert out["detail_count"] == 2
    assert out["detail_list"][0]["code"] == "HK.00700"


def test_load_symbols_builds_watchlist_like_rows():
    df = load_symbols(["US.AAPL", "HK.00700:HKD"])

    assert list(df.columns) == ["symbol", "market", "currency", "tags"]
    assert df.iloc[0].to_dict() == {
        "symbol": "AAPL",
        "market": "US",
        "currency": "USD",
        "tags": "cli_input",
    }
    assert df.iloc[1]["symbol"] == "00700"
    assert df.iloc[1]["currency"] == "HKD"


def test_routing_prefers_yahoo_for_us_and_keeps_futu_primary_for_hk():
    config = AppConfig()
    universe_df = pd.DataFrame(
        [
            {"symbol": "AAPL", "market": "US", "currency": "USD", "tags": "x"},
            {"symbol": "0700.HK", "market": "HK", "currency": "HKD", "tags": "x"},
        ]
    )

    assignments, routing_diag = resolve_provider_assignments(
        universe_df,
        config,
        dataset="history_bars",
        implemented_providers={"futu", "yahoo", "ibkr_tws", "ibkr_cp"},
    )

    assert list(assignments.keys()) == ["yahoo", "futu"]
    assert assignments["yahoo"]["symbol"].tolist() == ["AAPL"]
    assert assignments["futu"]["symbol"].tolist() == ["0700.HK"]
    assert routing_diag["assigned_symbols"][1]["fallback_providers"] == ["yahoo"]
    assert routing_diag["skipped_symbols"] == []


def test_load_config_upgrades_legacy_brokers_and_flat_data(tmp_path):
    path = tmp_path / "legacy.yaml"
    path.write_text(
        "\n".join(
            [
                "brokers:",
                "  futu:",
                "    host: 127.0.0.1",
                "    port: 11112",
                "data:",
                "  timeframe: 1D",
                "  adjustment: forward",
                "  max_missing_ratio: 0.25",
            ]
        ),
        encoding="utf-8",
    )

    config = load_config(path)

    assert config.providers.futu.port == 11112
    assert config.data.history_bars.timeframe == "1D"
    assert config.data.history_bars.bootstrap_start_date == date(2018, 1, 1)
    assert config.data.quality.max_missing_ratio == 0.25


def test_render_report_includes_bar_date_range_fields():
    run_summary = {
        "run_id": "run_test",
        "output_timezone": "UTC",
        "brokers": ["yahoo"],
        "universe_source": "watchlist",
        "universe_size": 1,
        "bars_rows": 5,
        "min_date": "2026-02-24",
        "max_date": "2026-03-01",
        "bars_max_date_lag_trading_days": 2,
        "quotes_rows": 0,
        "features_rows": 1,
        "candidates_rows": 1,
    }
    candidates_df = pd.DataFrame(
        [
            {
                "rank": 1,
                "symbol": "AAPL",
                "market": "US",
                "currency": "USD",
                "score": 1.0,
                "forecast_return_5d": 0.0,
                "ret_1d": 0.01,
                "close": 100.0,
                "volume": 1000,
            }
        ]
    )
    diagnostics = {
        "quality": {},
        "fetch": {},
        "provider_limits": {},
        "universe_filter": {
            "status": "ok",
            "source": "futu_filter",
            "name": "hk_ma7_30_50",
            "market": "HK",
            "plate_code": "",
            "results_count": 2,
            "universe_rows": 2,
        },
    }

    md = render_portfolio_markdown(run_summary, candidates_df, diagnostics)

    assert "- min_date: 2026-02-24" in md
    assert "- max_date: 2026-03-01" in md
    assert "- bars_max_date_lag_trading_days: 2" in md
    assert "## Universe Filter Summary" in md
