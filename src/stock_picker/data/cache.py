"""Caching helpers for raw and processed datasets.

Cache layout convention:
- data/cache/raw/{broker}/{dataset}.json
- data/cache/processed/bars.parquet
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import pandas as pd


def raw_cache_path(cache_dir: str | Path, broker: str, dataset: str) -> Path:
    """Build raw cache path for one broker/dataset pair."""

    return Path(cache_dir) / "raw" / broker / f"{dataset}.json"


def processed_bars_path(cache_dir: str | Path) -> Path:
    """Build processed bars parquet path."""

    return Path(cache_dir) / "processed" / "bars.parquet"


def load_or_fetch(
    cache_dir: str | Path,
    broker: str,
    dataset: str,
    fetcher: Callable[[], pd.DataFrame],
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Load cached raw data, or fetch and persist.

    This is a framework function for future enhancements such as TTL,
    request signatures, and schema versioning.
    """

    target = raw_cache_path(cache_dir, broker, dataset)
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists() and not force_refresh:
        try:
            return pd.read_json(target, orient="records")
        except ValueError:
            # Corrupted cache or empty file: refresh from source.
            pass

    df = fetcher()
    if df is None:
        df = pd.DataFrame()

    df.to_json(target, orient="records", date_format="iso", force_ascii=False)
    return df


def save_processed_bars(df: pd.DataFrame, cache_dir: str | Path) -> Path:
    """Persist normalized bars to parquet."""

    target = processed_bars_path(cache_dir)
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(target, index=False)
    return target
