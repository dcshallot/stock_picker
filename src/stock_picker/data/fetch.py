"""Market data fetching orchestration.

This module coordinates broker calls and cache usage. It does not perform
normalization or strategy logic.
"""

from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from stock_picker.brokers.base import (
    BrokerConnector,
    NotSupported,
    PermissionDenied,
    RateLimited,
)
from stock_picker.data.cache import load_or_fetch


def _safe_concat(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def fetch_market_data(
    connectors: Sequence[BrokerConnector],
    universe_df: pd.DataFrame,
    cache_dir: str,
    force_refresh: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Fetch bars/quotes from all connectors with cache fallback.

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
        broker_diag: dict[str, Any] = {
            "status": "ok",
            "bars_rows": 0,
            "quotes_rows": 0,
            "bars_status": "ok",
            "quotes_status": "ok",
        }

        try:
            mapping = connector.resolve_instruments(universe_df)
            request = {"universe": universe_df, "mapping": mapping}
        except Exception as exc:  # noqa: BLE001
            broker_diag["status"] = "error"
            broker_diag["error"] = f"resolve_instruments_failed: {exc}"
            diagnostics["other_errors"].append({"broker": connector.name, "error": str(exc)})
            diagnostics["brokers"][connector.name] = broker_diag
            continue

        try:
            bars_df = load_or_fetch(
                cache_dir=cache_dir,
                broker=connector.name,
                dataset="bars",
                fetcher=lambda c=connector, r=request: c.fetch_bars(r),
                force_refresh=force_refresh,
            )
            if not bars_df.empty and "source" not in bars_df.columns:
                bars_df = bars_df.assign(source=connector.name)
            broker_diag["bars_rows"] = int(len(bars_df))
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

        try:
            quotes_df = load_or_fetch(
                cache_dir=cache_dir,
                broker=connector.name,
                dataset="quotes",
                fetcher=lambda c=connector, r=request: c.fetch_quotes(r),
                force_refresh=force_refresh,
            )
            if not quotes_df.empty and "source" not in quotes_df.columns:
                quotes_df = quotes_df.assign(source=connector.name)
            broker_diag["quotes_rows"] = int(len(quotes_df))
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

        if broker_diag["bars_status"] != "ok" or broker_diag["quotes_status"] != "ok":
            if broker_diag["bars_status"] == "ok" or broker_diag["quotes_status"] == "ok":
                broker_diag["status"] = "partial"
            else:
                broker_diag["status"] = "error"

        diagnostics["brokers"][connector.name] = broker_diag

    return _safe_concat(bars_frames), _safe_concat(quotes_frames), diagnostics
