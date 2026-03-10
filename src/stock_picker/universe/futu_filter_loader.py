"""Load universe via Futu server-side stock filter."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol

import pandas as pd

REQUIRED_COLUMNS = ["symbol", "market", "currency", "tags"]
DEFAULT_CURRENCY_BY_MARKET = {
    "US": "USD",
    "HK": "HKD",
    "EU": "EUR",
    "CN": "CNY",
    "JP": "JPY",
}
ALLOWED_FILTER_TYPES = {"simple", "accumulate", "financial", "custom_indicator", "pattern"}


class StockFilterConnector(Protocol):
    """Interface required from a connector that supports stock-filter query."""

    def fetch_stock_filter(
        self,
        *,
        market: str,
        filter_spec: dict[str, Any],
        plate_code: str | None = None,
        page_size: int = 200,
        max_pages: int = 200,
    ) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
        """Fetch filtered stocks from provider.

        Returns:
            results_df: raw results (must contain `code` column)
            request_payload: normalized request payload
            meta: provider pagination/latency metadata
        """


def _normalize_symbol_from_code(code: str, market: str) -> str:
    text = str(code).strip().upper()
    mk = str(market).strip().upper()
    if "." not in text:
        return text

    left, right = text.split(".", 1)
    if mk == "HK":
        digits = "".join(ch for ch in right if ch.isdigit())
        if len(digits) == 5 and digits.startswith("0"):
            digits = digits[1:]
        return f"{digits}.HK" if digits else text
    if mk == "CN":
        suffix = "SS" if left in {"SH", "SS"} else "SZ" if left == "SZ" else right
        return f"{right}.{suffix}"
    if mk == "US":
        return right
    return right


def _load_filter_spec(path: str | Path) -> dict[str, Any]:
    spec_path = Path(path)
    if not spec_path.exists():
        raise FileNotFoundError(f"Filter spec file not found: {spec_path}")
    payload = json.loads(spec_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Filter spec must be a JSON object.")
    filters = payload.get("filters")
    if not isinstance(filters, list) or not filters:
        raise ValueError("Filter spec must include non-empty `filters` list.")
    for i, item in enumerate(filters):
        if not isinstance(item, dict):
            raise ValueError(f"filters[{i}] must be object.")
        filter_type = str(item.get("type", "")).strip().lower()
        if filter_type not in ALLOWED_FILTER_TYPES:
            raise ValueError(
                f"filters[{i}].type must be one of {sorted(ALLOWED_FILTER_TYPES)}, got: {item.get('type')}"
            )
    return payload


def load_from_futu_filter(
    config: Any,
    connector: StockFilterConnector,
) -> tuple[pd.DataFrame, dict[str, Any], pd.DataFrame, dict[str, Any]]:
    """Build watchlist-like universe from Futu filter results."""

    spec_path = str(config.universe.filter_spec_path)
    spec = _load_filter_spec(spec_path)
    market = str(config.universe.filter_market or spec.get("market", "HK")).upper()
    plate_code = (
        str(config.universe.filter_plate_code).strip()
        if config.universe.filter_plate_code is not None
        else spec.get("plate_code")
    )
    plate_code = str(plate_code).strip() if plate_code else None
    page_size = int(config.universe.filter_page_size or 200)
    max_pages = int(config.universe.max_filter_pages or 200)

    results_df, request_payload, fetch_meta = connector.fetch_stock_filter(
        market=market,
        filter_spec=spec,
        plate_code=plate_code,
        page_size=page_size,
        max_pages=max_pages,
    )

    rows: list[dict[str, str]] = []
    if results_df is not None and not results_df.empty:
        for _, row in results_df.iterrows():
            code = str(row.get("code", "")).strip()
            if not code:
                continue
            rows.append(
                {
                    "symbol": _normalize_symbol_from_code(code, market=market),
                    "market": market,
                    "currency": DEFAULT_CURRENCY_BY_MARKET.get(market, "UNKNOWN"),
                    "tags": "futu_filter",
                }
            )

    universe_df = pd.DataFrame(rows, columns=REQUIRED_COLUMNS)
    if not universe_df.empty:
        universe_df = universe_df.drop_duplicates(subset=["symbol", "market"], keep="first").reset_index(drop=True)

    diagnostics = {
        "status": "ok",
        "source": "futu_filter",
        "spec_path": spec_path,
        "name": spec.get("name", ""),
        "market": market,
        "plate_code": plate_code or "",
        "page_size": page_size,
        "max_filter_pages": max_pages,
        "results_count": int(len(results_df)),
        "universe_rows": int(len(universe_df)),
        "fetch_meta": fetch_meta,
    }

    return universe_df, diagnostics, results_df, request_payload
