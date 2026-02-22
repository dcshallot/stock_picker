"""Normalization utilities for unified data schemas.

Output schema families:
- bars
- quotes

All timestamps are converted to UTC (`ts_utc`).
"""

from __future__ import annotations

from typing import Iterable

import pandas as pd

BARS_SCHEMA_COLUMNS = [
    "ts_utc",
    "symbol",
    "market",
    "currency",
    "timeframe",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
    "quality_flags",
]

QUOTES_SCHEMA_COLUMNS = [
    "ts_utc",
    "symbol",
    "market",
    "currency",
    "bid",
    "ask",
    "last",
    "source",
    "quality_flags",
]


def _first_existing(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _series_or_default(df: pd.DataFrame, column: str, default: object) -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series([default] * len(df), index=df.index)


def normalize_bars(
    df: pd.DataFrame,
    source: str,
    source_symbol_col: str = "symbol",
) -> pd.DataFrame:
    """Normalize bar dataframe to the unified bars schema."""

    if df is None or df.empty:
        return pd.DataFrame(columns=BARS_SCHEMA_COLUMNS)

    out = pd.DataFrame(index=df.index)

    ts_col = _first_existing(df, ["ts_utc", "timestamp", "ts", "datetime"])
    ts_values = df[ts_col] if ts_col else pd.Series([pd.NaT] * len(df), index=df.index)
    out["ts_utc"] = pd.to_datetime(ts_values, errors="coerce", utc=True)

    symbol_col = source_symbol_col if source_symbol_col in df.columns else "symbol"
    out["symbol"] = _series_or_default(df, symbol_col, "UNKNOWN").astype(str)
    out["market"] = _series_or_default(df, "market", "UNKNOWN").astype(str)
    out["currency"] = _series_or_default(df, "currency", "UNKNOWN").astype(str)
    out["timeframe"] = _series_or_default(df, "timeframe", "1D").astype(str)

    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(df.get(col), errors="coerce")

    source_series = df.get("source")
    if source_series is not None:
        out["source"] = source_series.astype(str).replace("", source)
    else:
        out["source"] = source

    out["quality_flags"] = _series_or_default(df, "quality_flags", "").astype(str)

    for col in BARS_SCHEMA_COLUMNS:
        if col not in out.columns:
            out[col] = ""

    return out[BARS_SCHEMA_COLUMNS]


def normalize_quotes(
    df: pd.DataFrame,
    source: str,
    source_symbol_col: str = "symbol",
) -> pd.DataFrame:
    """Normalize quote dataframe to the unified quotes schema."""

    if df is None or df.empty:
        return pd.DataFrame(columns=QUOTES_SCHEMA_COLUMNS)

    out = pd.DataFrame(index=df.index)
    ts_col = _first_existing(df, ["ts_utc", "timestamp", "ts", "datetime"])

    ts_values = df[ts_col] if ts_col else pd.Series([pd.NaT] * len(df), index=df.index)
    out["ts_utc"] = pd.to_datetime(ts_values, errors="coerce", utc=True)

    symbol_col = source_symbol_col if source_symbol_col in df.columns else "symbol"
    out["symbol"] = _series_or_default(df, symbol_col, "UNKNOWN").astype(str)
    out["market"] = _series_or_default(df, "market", "UNKNOWN").astype(str)
    out["currency"] = _series_or_default(df, "currency", "UNKNOWN").astype(str)

    for col in ["bid", "ask", "last"]:
        out[col] = pd.to_numeric(df.get(col), errors="coerce")

    source_series = df.get("source")
    if source_series is not None:
        out["source"] = source_series.astype(str).replace("", source)
    else:
        out["source"] = source

    out["quality_flags"] = _series_or_default(df, "quality_flags", "").astype(str)

    for col in QUOTES_SCHEMA_COLUMNS:
        if col not in out.columns:
            out[col] = ""

    return out[QUOTES_SCHEMA_COLUMNS]
