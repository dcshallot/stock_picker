"""Tests for futu filter universe loading."""

from __future__ import annotations

import json

import pandas as pd

from stock_picker.brokers.futu import FutuConnector
from stock_picker.cli.run import _build_universe
from stock_picker.config.schema import AppConfig
from stock_picker.universe.futu_filter_loader import load_from_futu_filter


class _FakeFilterConnector:
    def fetch_stock_filter(
        self,
        *,
        market: str,
        filter_spec: dict,
        plate_code: str | None = None,
        page_size: int = 200,
        max_pages: int = 200,
    ) -> tuple[pd.DataFrame, dict, dict]:
        _ = (filter_spec, plate_code, page_size, max_pages)
        out = pd.DataFrame(
            [
                {"code": f"{market}.00700", "name": "Tencent"},
                {"code": f"{market}.09988", "name": "Alibaba"},
                {"code": f"{market}.00700", "name": "Tencent"},
            ]
        )
        request_payload = {"market": market, "filters": [{"type": "custom_indicator"}]}
        meta = {"pages_fetched": 2, "all_count": 3, "last_page": True, "truncated": False}
        return out, request_payload, meta


def _write_spec(path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_load_from_futu_filter_builds_watchlist_universe(tmp_path):
    spec_path = tmp_path / "filter.json"
    _write_spec(
        spec_path,
        {
            "name": "hk_ma7_30_50",
            "filters": [
                {
                    "type": "custom_indicator",
                    "stock_field1": "MA",
                    "stock_field1_para": [7],
                    "stock_field2": "VALUE",
                    "relative_position": "MORE",
                    "value": 30,
                    "ktype": "K_DAY",
                }
            ],
        },
    )
    config = AppConfig.model_validate(
        {
            "universe": {
                "mode": "futu_filter",
                "filter_spec_path": str(spec_path),
                "filter_market": "HK",
            }
        }
    )

    universe_df, diag, raw_df, request_payload = load_from_futu_filter(config, _FakeFilterConnector())

    assert set(universe_df.columns) == {"symbol", "market", "currency", "tags"}
    assert universe_df["symbol"].tolist() == ["0700.HK", "9988.HK"]
    assert diag["source"] == "futu_filter"
    assert diag["results_count"] == 3
    assert int(len(raw_df)) == 3
    assert request_payload["market"] == "HK"


def test_load_from_futu_filter_rejects_invalid_spec(tmp_path):
    spec_path = tmp_path / "invalid_filter.json"
    _write_spec(spec_path, {"name": "x", "filters": []})
    config = AppConfig.model_validate(
        {
            "universe": {
                "mode": "futu_filter",
                "filter_spec_path": str(spec_path),
                "filter_market": "HK",
            }
        }
    )

    try:
        load_from_futu_filter(config, _FakeFilterConnector())
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "filters" in str(exc)


def test_build_universe_supports_futu_filter_mode(tmp_path):
    spec_path = tmp_path / "filter.json"
    _write_spec(
        spec_path,
        {"name": "demo", "filters": [{"type": "simple", "stock_field": "CUR_PRICE", "filter_min": 1}]},
    )
    config = AppConfig.model_validate(
        {
            "universe": {
                "mode": "futu_filter",
                "filter_spec_path": str(spec_path),
                "filter_market": "HK",
            }
        }
    )

    universe_df, source, diag, raw_df, request_payload = _build_universe(
        config,
        futu_connector=_FakeFilterConnector(),
    )

    assert source == "futu_filter"
    assert len(universe_df) == 2
    assert diag["market"] == "HK"
    assert len(raw_df) == 3
    assert request_payload["market"] == "HK"


def test_futu_connector_request_payload_keeps_custom_indicator():
    connector = FutuConnector({"host": "127.0.0.1", "port": 11111})
    spec = {
        "name": "hk_ma7_30_50",
        "filters": [
            {
                "type": "custom_indicator",
                "stock_field1": "MA",
                "stock_field1_para": [7],
                "stock_field2": "VALUE",
                "relative_position": "MORE",
                "value": 30,
                "ktype": "K_DAY",
            }
        ],
    }

    payload = connector._normalize_filter_request_payload(
        market="HK",
        filter_spec=spec,
        plate_code="",
        page_size=200,
        max_pages=200,
    )

    assert payload["market"] == "HK"
    assert payload["filters"][0]["stock_field1"] == "MA"
    assert payload["filters"][0]["stock_field1_para"] == [7]
