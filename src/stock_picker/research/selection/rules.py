"""Hard filter selection rules.

This module applies deterministic gating rules before scoring/ranking.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def apply_hard_filters(
    features_df: pd.DataFrame,
    model_outputs_df: pd.DataFrame,
    hard_filters: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Apply hard constraints and return filtered candidates."""

    hard_filters = hard_filters or {}

    if features_df is None or features_df.empty:
        return pd.DataFrame()

    df = features_df.copy()

    if model_outputs_df is not None and not model_outputs_df.empty:
        df = df.merge(model_outputs_df[["symbol", "forecast_return_5d"]], on="symbol", how="left")
    else:
        df["forecast_return_5d"] = 0.0

    if "market_whitelist" in hard_filters:
        allowed = {str(v).upper() for v in hard_filters.get("market_whitelist", [])}
        if allowed:
            df = df[df["market"].astype(str).str.upper().isin(allowed)]

    min_close = hard_filters.get("min_close")
    if min_close is not None:
        df = df[pd.to_numeric(df["close"], errors="coerce") >= float(min_close)]

    min_volume = hard_filters.get("min_volume")
    if min_volume is not None:
        df = df[pd.to_numeric(df["volume"], errors="coerce") >= float(min_volume)]

    max_volatility = hard_filters.get("max_volatility_3")
    if max_volatility is not None and "volatility_3" in df.columns:
        vol = pd.to_numeric(df["volatility_3"], errors="coerce")
        df = df[vol.fillna(0.0) <= float(max_volatility)]

    min_forecast = hard_filters.get("min_forecast_return_5d")
    if min_forecast is not None:
        fc = pd.to_numeric(df["forecast_return_5d"], errors="coerce")
        df = df[fc.fillna(0.0) >= float(min_forecast)]

    return df.reset_index(drop=True)
