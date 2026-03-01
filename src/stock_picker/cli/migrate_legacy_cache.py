"""Migrate legacy request-scoped bars cache into the Parquet history store."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from stock_picker.config.loader import load_config
from stock_picker.data.historical_store import HistoricalStore
from stock_picker.data.normalize import normalize_bars


def _read_legacy_bars_json(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_json(path, orient="records")
    except ValueError:
        return pd.DataFrame()
    if df.empty:
        return df
    legacy_source = f"legacy_{path.parent.name}"
    df["source"] = legacy_source
    return normalize_bars(df, source=legacy_source)


def _read_legacy_processed_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    if df.empty:
        return df
    df["source"] = "legacy_processed"
    if "adjustment" not in df.columns:
        df["adjustment"] = "forward"
    return normalize_bars(df, source="legacy_processed")


def migrate_legacy_bars_cache(
    legacy_cache_dir: str | Path,
    store_dir: str | Path,
) -> dict[str, int]:
    """Migrate legacy bars cache files into the history store."""

    cache_root = Path(legacy_cache_dir)
    store = HistoricalStore(store_dir)
    frames: list[pd.DataFrame] = []
    json_files = sorted(cache_root.glob("raw/*/bars*.json"))

    for path in json_files:
        frame = _read_legacy_bars_json(path)
        if not frame.empty:
            frames.append(frame)

    processed_path = cache_root / "processed" / "bars.parquet"
    processed_frame = _read_legacy_processed_parquet(processed_path)
    if not processed_frame.empty:
        frames.append(processed_frame)

    if not frames:
        return {
            "legacy_json_files": int(len(json_files)),
            "migrated_rows": 0,
            "coverage_rows": int(len(store.load_coverage())),
        }

    merged = pd.concat(frames, ignore_index=True)
    merged["adjustment"] = merged.get("adjustment", "forward")
    coverage = store.upsert_bars(merged)
    return {
        "legacy_json_files": int(len(json_files)),
        "migrated_rows": int(len(merged)),
        "coverage_rows": int(len(coverage)),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Migrate legacy bars cache into history_store.")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML")
    parser.add_argument(
        "--legacy-cache-dir",
        default="data/cache",
        help="Legacy request cache directory (default: data/cache)",
    )
    parser.add_argument(
        "--store-dir",
        default="",
        help="Override target history store directory (default: config.data.history_bars.store_dir)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = load_config(args.config)
    store_dir = args.store_dir or config.data.history_bars.store_dir
    summary = migrate_legacy_bars_cache(args.legacy_cache_dir, store_dir)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
