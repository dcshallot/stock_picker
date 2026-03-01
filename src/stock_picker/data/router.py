"""Market/data routing helpers.

This module decides which provider should handle each symbol for a given
dataset, based on:
- market -> provider route ordering
- provider enabled flags
- provider allowed_markets / datasets declarations
- currently implemented local connectors
"""

from __future__ import annotations

from typing import Any, Iterable

import pandas as pd


def _normalize_names(values: Iterable[str] | None) -> list[str]:
    items = [str(value).strip() for value in values or []]
    return [item for item in items if item]


def _normalize_markets(values: Iterable[str] | None) -> set[str]:
    return {str(value).strip().upper() for value in values or [] if str(value).strip()}


def resolve_provider_assignments(
    universe_df: pd.DataFrame,
    config: Any,
    *,
    dataset: str,
    implemented_providers: set[str],
    selected_providers: list[str] | None = None,
    allowed_markets: list[str] | None = None,
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    """Assign each symbol to one provider for the requested dataset.

    Current behavior chooses the first eligible implemented provider in the
    route order. Fallback providers are recorded for diagnostics but not yet
    attempted automatically.
    """

    selected = _normalize_names(selected_providers)
    allowed_market_set = _normalize_markets(allowed_markets)
    route_map = dict(getattr(config.routing, dataset, {}) or {})

    assignments: dict[str, list[dict[str, Any]]] = {}
    assigned_symbols: list[dict[str, Any]] = []
    skipped_symbols: list[dict[str, Any]] = []

    for row in universe_df.to_dict(orient="records"):
        market = str(row.get("market", "")).upper()
        symbol = str(row.get("symbol", ""))

        if allowed_market_set and market not in allowed_market_set:
            skipped_symbols.append(
                {
                    "symbol": symbol,
                    "market": market,
                    "reason": "filtered_by_allowed_market",
                }
            )
            continue

        candidate_providers = selected or route_map.get(market, [])
        candidate_providers = _normalize_names(candidate_providers)

        if not candidate_providers:
            skipped_symbols.append(
                {
                    "symbol": symbol,
                    "market": market,
                    "reason": "no_route_for_market",
                }
            )
            continue

        eligible: list[str] = []
        rejection_reasons: list[str] = []

        for provider_name in candidate_providers:
            provider_cfg = config.get_provider_config(provider_name)
            if provider_cfg is None:
                rejection_reasons.append(f"{provider_name}:not_configured")
                continue
            if not getattr(provider_cfg, "enabled", False):
                rejection_reasons.append(f"{provider_name}:disabled")
                continue

            provider_markets = _normalize_markets(getattr(provider_cfg, "allowed_markets", []))
            if provider_markets and market not in provider_markets:
                rejection_reasons.append(f"{provider_name}:market_not_allowed")
                continue

            provider_datasets = _normalize_names(getattr(provider_cfg, "datasets", []))
            if provider_datasets and dataset not in provider_datasets:
                rejection_reasons.append(f"{provider_name}:dataset_not_allowed")
                continue

            if provider_name not in implemented_providers:
                rejection_reasons.append(f"{provider_name}:not_implemented")
                continue

            eligible.append(provider_name)

        if not eligible:
            skipped_symbols.append(
                {
                    "symbol": symbol,
                    "market": market,
                    "reason": "no_eligible_provider",
                    "candidate_providers": candidate_providers,
                    "rejections": rejection_reasons,
                }
            )
            continue

        chosen = eligible[0]
        assignments.setdefault(chosen, []).append(row)
        assigned_symbols.append(
            {
                "symbol": symbol,
                "market": market,
                "provider": chosen,
                "fallback_providers": eligible[1:],
                "route": candidate_providers,
            }
        )

    assignment_frames = {
        provider: pd.DataFrame(rows, columns=list(universe_df.columns))
        for provider, rows in assignments.items()
    }
    diagnostics = {
        "dataset": dataset,
        "selected_providers": selected,
        "allowed_markets": sorted(allowed_market_set),
        "assigned_symbols": assigned_symbols,
        "skipped_symbols": skipped_symbols,
    }
    return assignment_frames, diagnostics


def build_symbol_fetch_results(
    universe_df: pd.DataFrame,
    routing_diagnostics: dict[str, Any],
    fetch_diagnostics: dict[str, Any],
    bars_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build a per-symbol fetch result artifact."""

    rows: dict[str, dict[str, Any]] = {}

    for item in universe_df.to_dict(orient="records"):
        symbol = str(item.get("symbol", ""))
        rows[symbol] = {
            "symbol": symbol,
            "market": str(item.get("market", "")),
            "currency": str(item.get("currency", "")),
            "route_provider": "",
            "final_provider": "",
            "status": "pending",
            "rows": 0,
            "latest_bar_ts_utc": "",
            "open": None,
            "close": None,
            "high": None,
            "low": None,
            "turnover": None,
            "error_type": "",
            "error_message": "",
        }

    for item in routing_diagnostics.get("assigned_symbols", []):
        symbol = str(item.get("symbol", ""))
        if symbol in rows:
            rows[symbol]["route_provider"] = str(item.get("provider", ""))
            rows[symbol]["status"] = "assigned"

    for item in routing_diagnostics.get("skipped_symbols", []):
        symbol = str(item.get("symbol", ""))
        if symbol in rows:
            rows[symbol]["status"] = "skipped"
            rows[symbol]["error_type"] = "routing"
            rows[symbol]["error_message"] = str(item.get("reason", ""))

    broker_diags = fetch_diagnostics.get("brokers", {})
    for provider_name, provider_diag in broker_diags.items():
        bars_details = provider_diag.get("bars_details", {})

        for item in bars_details.get("ok", []):
            symbol = str(item.get("symbol", ""))
            if symbol in rows:
                rows[symbol]["final_provider"] = provider_name
                rows[symbol]["status"] = "success"
                rows[symbol]["rows"] = int(item.get("rows", 0))

        for item in bars_details.get("errors", []):
            symbol = str(item.get("symbol", ""))
            if symbol in rows and rows[symbol]["status"] != "success":
                rows[symbol]["final_provider"] = provider_name
                rows[symbol]["status"] = "failed"
                rows[symbol]["error_type"] = str(item.get("error_type", "provider_error"))
                rows[symbol]["error_message"] = str(item.get("error", ""))

    if not bars_df.empty:
        latest_df = (
            bars_df.sort_values(["symbol", "ts_utc"])
            .groupby("symbol", as_index=False)
            .tail(1)
            .reset_index(drop=True)
        )
        for _, item in latest_df.iterrows():
            symbol = str(item.get("symbol", ""))
            if symbol not in rows:
                continue
            rows[symbol]["latest_bar_ts_utc"] = str(item.get("ts_utc", ""))
            rows[symbol]["open"] = item.get("open")
            rows[symbol]["close"] = item.get("close")
            rows[symbol]["high"] = item.get("high")
            rows[symbol]["low"] = item.get("low")
            rows[symbol]["turnover"] = item.get("turnover")

    return pd.DataFrame(list(rows.values()))
