"""Candidate scoring and ranking.

This module converts filtered candidates into a ranked portfolio list.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


DEFAULT_WEIGHTS = {
    "forecast_return_5d": 0.5,
    "ret_1d": 0.2,
    "volume": 0.2,
    "volatility_3": 0.1,
}


def _rank_feature(series: pd.Series, reverse: bool = False) -> pd.Series:
    ranked = pd.to_numeric(series, errors="coerce").rank(pct=True, method="average")
    if reverse:
        return 1.0 - ranked.fillna(0.0)
    return ranked.fillna(0.0)


def score_and_rank(
    candidates_df: pd.DataFrame,
    score_weights: dict[str, Any] | None = None,
    top_n: int = 20,
) -> pd.DataFrame:
    """Score and rank filtered candidates."""

    if candidates_df is None or candidates_df.empty:
        return pd.DataFrame()

    df = candidates_df.copy()
    weights = DEFAULT_WEIGHTS.copy()
    if score_weights:
        for key, value in score_weights.items():
            try:
                weights[key] = float(value)
            except (TypeError, ValueError):
                continue

    df["score"] = 0.0

    if "forecast_return_5d" in df.columns:
        df["score"] += weights.get("forecast_return_5d", 0.0) * _rank_feature(df["forecast_return_5d"])
    if "ret_1d" in df.columns:
        df["score"] += weights.get("ret_1d", 0.0) * _rank_feature(df["ret_1d"])
    if "volume" in df.columns:
        df["score"] += weights.get("volume", 0.0) * _rank_feature(df["volume"])
    if "volatility_3" in df.columns:
        df["score"] += weights.get("volatility_3", 0.0) * _rank_feature(df["volatility_3"], reverse=True)

    df = df.sort_values("score", ascending=False).reset_index(drop=True)
    df["rank"] = range(1, len(df) + 1)

    return df.head(max(top_n, 1))
