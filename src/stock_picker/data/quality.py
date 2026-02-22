"""Quality tagging for normalized data.

This module tags per-row issues and provides simple diagnostics summary.
"""

from __future__ import annotations

from collections import Counter

import pandas as pd


def tag_quality_flags(bars_df: pd.DataFrame, max_missing_ratio: float = 0.1) -> pd.DataFrame:
    """Tag quality flags on normalized bars rows."""

    if bars_df is None or bars_df.empty:
        return pd.DataFrame(columns=getattr(bars_df, "columns", []))

    out = bars_df.copy()
    required_numeric = ["open", "high", "low", "close", "volume"]

    flags_all: list[str] = []
    for _, row in out.iterrows():
        flags: list[str] = []

        missing_cnt = int(row[required_numeric].isna().sum())
        missing_ratio = missing_cnt / len(required_numeric)
        if missing_ratio > max_missing_ratio:
            flags.append("missing_ratio_exceeded")

        if pd.notna(row.get("high")) and pd.notna(row.get("low")) and row["high"] < row["low"]:
            flags.append("invalid_ohlc")

        existing = str(row.get("quality_flags", "")).strip()
        if existing:
            flags.insert(0, existing)

        flags_all.append(";".join(flags))

    out["quality_flags"] = flags_all
    return out


def summarize_quality_flags(bars_df: pd.DataFrame) -> dict[str, int]:
    """Aggregate quality flag counts for diagnostics."""

    if bars_df is None or bars_df.empty:
        return {"total_rows": 0, "flagged_rows": 0}

    counter: Counter[str] = Counter()
    flagged_rows = 0

    for raw in bars_df["quality_flags"].fillna("").astype(str).tolist():
        parts = [p for p in raw.split(";") if p]
        if parts:
            flagged_rows += 1
            counter.update(parts)

    summary: dict[str, int] = {
        "total_rows": int(len(bars_df)),
        "flagged_rows": int(flagged_rows),
    }
    for key, value in counter.items():
        summary[key] = int(value)
    return summary
