"""Feature engineering stubs.

Strategy/selection consumes only processed data and generated features.
"""

from __future__ import annotations

import pandas as pd


def build_features(bars_df: pd.DataFrame) -> pd.DataFrame:
    """Build demo feature set from normalized bars."""

    columns = [
        "symbol",
        "market",
        "currency",
        "ts_utc",
        "close",
        "volume",
        "ret_1d",
        "sma_3_gap",
        "volatility_3",
        "quality_flags",
    ]

    if bars_df is None or bars_df.empty:
        return pd.DataFrame(columns=columns)

    df = bars_df.copy()
    df = df.sort_values(["symbol", "ts_utc"])

    df["ret_1d"] = df.groupby("symbol")["close"].pct_change()
    rolling_mean = df.groupby("symbol")["close"].rolling(3).mean().reset_index(level=0, drop=True)
    rolling_std = (
        df.groupby("symbol")["close"].pct_change().rolling(3).std().reset_index(level=0, drop=True)
    )

    df["sma_3_gap"] = (df["close"] / rolling_mean) - 1.0
    df["volatility_3"] = rolling_std

    latest = df.groupby("symbol", as_index=False).tail(1)
    return latest[columns].reset_index(drop=True)
