"""Prophet model integration placeholder.

The function is optional and must fail gracefully when Prophet package
is unavailable.
"""

from __future__ import annotations

import importlib
from typing import Any

import pandas as pd


def run_prophet(
    data_df: pd.DataFrame,
    *,
    enabled: bool,
    params: dict[str, Any] | None = None,
    diagnostics: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Run optional Prophet-based forecast step.

    Current implementation is a lightweight placeholder with deterministic
    outputs if Prophet is installed.
    """

    params = params or {}
    diagnostics = diagnostics if diagnostics is not None else {}
    diagnostics.setdefault("models", {})
    model_diag = diagnostics["models"].setdefault("prophet", {})

    columns = ["symbol", "forecast_return_5d", "model"]

    if not enabled:
        model_diag.update({"enabled": False, "status": "skipped_disabled"})
        return pd.DataFrame(columns=columns)

    try:
        importlib.import_module("prophet")
    except ImportError:
        model_diag.update(
            {
                "enabled": True,
                "status": "skipped_missing_dependency",
                "message": "prophet package is not installed",
            }
        )
        return pd.DataFrame(columns=columns)

    if data_df is None or data_df.empty:
        model_diag.update(
            {
                "enabled": True,
                "status": "skipped_no_data",
                "params": params,
            }
        )
        return pd.DataFrame(columns=columns)

    symbols = sorted(set(data_df["symbol"].astype(str).tolist()))
    rows: list[dict[str, Any]] = []

    for i, symbol in enumerate(symbols):
        # Deterministic placeholder score, to be replaced by real model output.
        pseudo = ((i % 5) - 2) * 0.01
        rows.append(
            {
                "symbol": symbol,
                "forecast_return_5d": pseudo,
                "model": "prophet_stub",
            }
        )

    model_diag.update(
        {
            "enabled": True,
            "status": "ok_stub",
            "params": params,
            "rows": len(rows),
        }
    )

    return pd.DataFrame(rows, columns=columns)
