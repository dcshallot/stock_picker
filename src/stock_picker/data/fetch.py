"""Market data fetching orchestration for non-history paths."""

from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from stock_picker.brokers.base import (
    BrokerDataRequest,
    BrokerConnector,
    NotSupported,
    PermissionDenied,
    RateLimited,
)
from stock_picker.data.history_sync import build_sync_plans, sync_historical_bars

__all__ = ["build_sync_plans", "sync_historical_bars", "fetch_market_data"]


def _safe_concat(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def fetch_market_data(
    connectors: Sequence[BrokerConnector],
    universe_df: pd.DataFrame,
    *,
    universe_by_connector: dict[str, pd.DataFrame] | None = None,
    timeframe: str = "1D",
    adjustment: str = "forward",
    start_date: Any = None,
    end_date: Any = None,
    include_bars: bool = True,
    include_quotes: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Fetch bars/quotes directly from connectors.

    Returns:
        raw_bars_df, raw_quotes_df, fetch_diagnostics
    """

    bars_frames: list[pd.DataFrame] = []
    quotes_frames: list[pd.DataFrame] = []

    diagnostics: dict[str, Any] = {
        "brokers": {},
        "permission_denied": [],
        "rate_limited": [],
        "not_supported": [],
        "other_errors": [],
    }

    for connector in connectors:
        connector_universe_df = (
            universe_by_connector.get(connector.name, universe_df)
            if universe_by_connector is not None
            else universe_df
        )
        broker_diag: dict[str, Any] = {
            "status": "ok",
            "bars_rows": 0,
            "quotes_rows": 0,
            "bars_status": "ok",
            "quotes_status": "ok",
        }

        if connector_universe_df.empty:
            broker_diag["status"] = "skipped"
            broker_diag["bars_status"] = "skipped"
            broker_diag["quotes_status"] = "skipped"
            broker_diag["bars_details"] = {"ok": [], "errors": []}
            broker_diag["quotes_details"] = {"ok": [], "errors": []}
            diagnostics["brokers"][connector.name] = broker_diag
            continue

        try:
            mapping = connector.resolve_instruments(connector_universe_df)
            request = BrokerDataRequest(
                universe=connector_universe_df.to_dict(orient="records"),
                mapping=mapping,
                timeframe=timeframe,
                adjustment=adjustment,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as exc:  # noqa: BLE001
            broker_diag["status"] = "error"
            broker_diag["error"] = f"resolve_instruments_failed: {exc}"
            diagnostics["other_errors"].append({"broker": connector.name, "error": str(exc)})
            diagnostics["brokers"][connector.name] = broker_diag
            continue

        if include_bars:
            try:
                bars_df = connector.fetch_bars(request)
                if not bars_df.empty and "source" not in bars_df.columns:
                    bars_df = bars_df.assign(source=connector.name)
                broker_diag["bars_rows"] = int(len(bars_df))
                broker_diag["bars_details"] = connector.last_fetch_notes.get("bars", {})
                bars_frames.append(bars_df)
            except PermissionDenied as exc:
                broker_diag["bars_status"] = "permission_denied"
                broker_diag["bars_error"] = str(exc)
                diagnostics["permission_denied"].append(f"{connector.name}:bars")
            except RateLimited as exc:
                broker_diag["bars_status"] = "rate_limited"
                broker_diag["bars_error"] = str(exc)
                diagnostics["rate_limited"].append(f"{connector.name}:bars")
            except NotSupported as exc:
                broker_diag["bars_status"] = "not_supported"
                broker_diag["bars_error"] = str(exc)
                diagnostics["not_supported"].append(f"{connector.name}:bars")
            except Exception as exc:  # noqa: BLE001
                broker_diag["bars_status"] = "error"
                broker_diag["bars_error"] = str(exc)
                diagnostics["other_errors"].append(
                    {"broker": connector.name, "endpoint": "bars", "error": str(exc)}
                )
        else:
            broker_diag["bars_status"] = "skipped"
            broker_diag["bars_rows"] = 0
            broker_diag["bars_details"] = {"ok": [], "errors": []}

        if include_quotes:
            try:
                quotes_df = connector.fetch_quotes(request)
                if not quotes_df.empty and "source" not in quotes_df.columns:
                    quotes_df = quotes_df.assign(source=connector.name)
                broker_diag["quotes_rows"] = int(len(quotes_df))
                broker_diag["quotes_details"] = connector.last_fetch_notes.get("quotes", {})
                quotes_frames.append(quotes_df)
            except PermissionDenied as exc:
                broker_diag["quotes_status"] = "permission_denied"
                broker_diag["quotes_error"] = str(exc)
                diagnostics["permission_denied"].append(f"{connector.name}:quotes")
            except RateLimited as exc:
                broker_diag["quotes_status"] = "rate_limited"
                broker_diag["quotes_error"] = str(exc)
                diagnostics["rate_limited"].append(f"{connector.name}:quotes")
            except NotSupported as exc:
                broker_diag["quotes_status"] = "not_supported"
                broker_diag["quotes_error"] = str(exc)
                diagnostics["not_supported"].append(f"{connector.name}:quotes")
            except Exception as exc:  # noqa: BLE001
                broker_diag["quotes_status"] = "error"
                broker_diag["quotes_error"] = str(exc)
                diagnostics["other_errors"].append(
                    {"broker": connector.name, "endpoint": "quotes", "error": str(exc)}
                )
        else:
            broker_diag["quotes_status"] = "skipped"
            broker_diag["quotes_rows"] = 0
            broker_diag["quotes_details"] = {"ok": [], "errors": []}

        bars_status = broker_diag["bars_status"]
        quotes_status = broker_diag["quotes_status"]
        error_like_statuses = {status for status in (bars_status, quotes_status) if status not in {"ok", "skipped"}}

        if error_like_statuses:
            if bars_status in {"ok", "skipped"} or quotes_status in {"ok", "skipped"}:
                broker_diag["status"] = "partial"
            else:
                broker_diag["status"] = "error"
        if broker_diag.get("bars_details", {}).get("errors") and broker_diag["status"] == "ok":
            broker_diag["status"] = "partial"

        diagnostics["brokers"][connector.name] = broker_diag

    return _safe_concat(bars_frames), _safe_concat(quotes_frames), diagnostics
