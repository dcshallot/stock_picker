"""Parquet-backed history store for normalized daily bars."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel

from stock_picker.data.normalize import BARS_SCHEMA_COLUMNS

SCHEMA_VERSION = 1

COVERAGE_COLUMNS = [
    "dataset",
    "timeframe",
    "adjustment",
    "market",
    "symbol",
    "provider",
    "min_date",
    "max_date",
    "row_count",
    "last_synced_at_utc",
    "last_success_at_utc",
    "has_gaps",
    "schema_version",
]

SYNC_RUN_COLUMNS = [
    "run_id",
    "symbol",
    "provider",
    "requested_start",
    "requested_end",
    "fetched_start",
    "fetched_end",
    "rows_fetched",
    "status",
    "error_type",
    "error_message",
    "started_at_utc",
    "finished_at_utc",
]

PROVIDER_HEALTH_COLUMNS = [
    "provider",
    "supports_history_bars",
    "available",
    "requires_network",
    "notes",
    "checked_at_utc",
]


class CoverageRecord(BaseModel):
    """Metadata row describing current local coverage for one symbol."""

    dataset: str = "history_bars"
    timeframe: str = "1D"
    adjustment: str = "forward"
    market: str
    symbol: str
    provider: str
    min_date: date
    max_date: date
    row_count: int
    last_synced_at_utc: str
    last_success_at_utc: str
    has_gaps: bool = False
    schema_version: int = SCHEMA_VERSION


class HistoricalStore:
    """Symbol/year partitioned history store for normalized bars."""

    def __init__(self, store_dir: str | Path) -> None:
        self.root = Path(store_dir)

    @staticmethod
    def _sanitize_segment(value: str) -> str:
        safe = []
        for char in str(value):
            if char.isalnum() or char in {"-", "_", "."}:
                safe.append(char)
            else:
                safe.append("_")
        return "".join(safe).strip("_") or "unknown"

    def bars_root(self) -> Path:
        return self.root / "bars"

    def meta_root(self) -> Path:
        return self.root / "meta"

    def coverage_path(self) -> Path:
        return self.meta_root() / "coverage.parquet"

    def sync_runs_path(self) -> Path:
        return self.meta_root() / "sync_runs.parquet"

    def provider_health_path(self) -> Path:
        return self.meta_root() / "provider_health.parquet"

    def year_path(
        self,
        timeframe: str,
        adjustment: str,
        market: str,
        symbol: str,
        year: int,
    ) -> Path:
        symbol_part = self._sanitize_segment(symbol)
        market_part = self._sanitize_segment(market.upper())
        return (
            self.bars_root()
            / f"timeframe={self._sanitize_segment(timeframe)}"
            / f"adjustment={self._sanitize_segment(adjustment)}"
            / f"market={market_part}"
            / f"symbol={symbol_part}"
            / f"year={int(year)}.parquet"
        )

    def _empty_bars(self) -> pd.DataFrame:
        return pd.DataFrame(columns=BARS_SCHEMA_COLUMNS)

    def _normalize_frame(self, df: pd.DataFrame | None) -> pd.DataFrame:
        if df is None or df.empty:
            return self._empty_bars()

        out = df.copy()
        for column in BARS_SCHEMA_COLUMNS:
            if column not in out.columns:
                out[column] = pd.NA

        out["ts_utc"] = pd.to_datetime(out["ts_utc"], errors="coerce", utc=True)
        out["symbol"] = out["symbol"].fillna("UNKNOWN").astype(str)
        out["market"] = out["market"].fillna("UNKNOWN").astype(str).str.upper()
        out["currency"] = out["currency"].fillna("UNKNOWN").astype(str).str.upper()
        out["timeframe"] = out["timeframe"].fillna("1D").astype(str)
        out["adjustment"] = out["adjustment"].fillna("forward").astype(str)
        out["source"] = out["source"].fillna("").astype(str)
        daily_mask = out["timeframe"].str.upper() == "1D"
        if daily_mask.any():
            out.loc[daily_mask, "ts_utc"] = out.loc[daily_mask, "ts_utc"].dt.normalize()
            out = out.loc[~(daily_mask & (out["ts_utc"].dt.dayofweek >= 5))].copy()
        out["quality_flags"] = out["quality_flags"].fillna("").astype(str)
        return out[BARS_SCHEMA_COLUMNS]

    def load_coverage(self) -> pd.DataFrame:
        path = self.coverage_path()
        if not path.exists():
            return pd.DataFrame(columns=COVERAGE_COLUMNS)

        df = pd.read_parquet(path)
        for column in COVERAGE_COLUMNS:
            if column not in df.columns:
                df[column] = pd.NA
        return df[COVERAGE_COLUMNS]

    def load_coverage_record(
        self,
        *,
        symbol: str,
        market: str,
        timeframe: str,
        adjustment: str,
    ) -> CoverageRecord | None:
        df = self.load_coverage()
        if df.empty:
            return None

        mask = (
            (df["dataset"] == "history_bars")
            & (df["symbol"].astype(str) == str(symbol))
            & (df["market"].astype(str).str.upper() == str(market).upper())
            & (df["timeframe"].astype(str) == str(timeframe))
            & (df["adjustment"].astype(str) == str(adjustment))
        )
        subset = df.loc[mask]
        if subset.empty:
            return None

        row = subset.iloc[-1].to_dict()
        for field in ("min_date", "max_date"):
            raw_value = row.get(field)
            if pd.notna(raw_value):
                row[field] = pd.Timestamp(raw_value).date()
        row["row_count"] = int(row.get("row_count", 0) or 0)
        row["has_gaps"] = bool(row.get("has_gaps", False))
        row["schema_version"] = int(row.get("schema_version", SCHEMA_VERSION) or SCHEMA_VERSION)
        return CoverageRecord.model_validate(row)

    def load_symbol_bars(
        self,
        *,
        symbol: str,
        market: str,
        timeframe: str,
        adjustment: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        start_year = start_date.year if start_date else None
        end_year = end_date.year if end_date else None
        symbol_dir = self.year_path(timeframe, adjustment, market, symbol, 0).parent
        if not symbol_dir.exists():
            return self._empty_bars()

        frames: list[pd.DataFrame] = []
        for path in sorted(symbol_dir.glob("year=*.parquet")):
            stem = path.stem
            try:
                year = int(stem.split("=", 1)[1])
            except (IndexError, ValueError):
                continue
            if start_year is not None and year < start_year:
                continue
            if end_year is not None and year > end_year:
                continue
            frames.append(pd.read_parquet(path))

        if not frames:
            return self._empty_bars()

        out = self._normalize_frame(pd.concat(frames, ignore_index=True))
        if start_date is not None:
            out = out[out["ts_utc"].dt.date >= start_date]
        if end_date is not None:
            out = out[out["ts_utc"].dt.date <= end_date]
        return out.sort_values(["symbol", "ts_utc"]).reset_index(drop=True)

    def read_bars(
        self,
        universe_df: pd.DataFrame,
        *,
        timeframe: str,
        adjustment: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        if universe_df is None or universe_df.empty:
            return self._empty_bars()

        frames: list[pd.DataFrame] = []
        for _, row in universe_df.iterrows():
            frame = self.load_symbol_bars(
                symbol=str(row.get("symbol", "")),
                market=str(row.get("market", "")),
                timeframe=timeframe,
                adjustment=adjustment,
                start_date=start_date,
                end_date=end_date,
            )
            if not frame.empty:
                frames.append(frame)

        if not frames:
            return self._empty_bars()
        return pd.concat(frames, ignore_index=True).sort_values(["symbol", "ts_utc"]).reset_index(drop=True)

    def _write_table(self, path: Path, df: pd.DataFrame, columns: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        out = df.copy()
        for column in columns:
            if column not in out.columns:
                out[column] = pd.NA
        out[columns].to_parquet(path, index=False)

    def record_sync_runs(self, records: list[dict[str, Any]]) -> None:
        if not records:
            return

        path = self.sync_runs_path()
        existing = pd.read_parquet(path) if path.exists() else pd.DataFrame(columns=SYNC_RUN_COLUMNS)
        out = pd.concat([existing, pd.DataFrame(records)], ignore_index=True)
        self._write_table(path, out, SYNC_RUN_COLUMNS)

    def record_provider_health(self, records: list[dict[str, Any]]) -> None:
        if not records:
            return

        path = self.provider_health_path()
        existing = (
            pd.read_parquet(path)
            if path.exists()
            else pd.DataFrame(columns=PROVIDER_HEALTH_COLUMNS)
        )
        out = pd.concat([existing, pd.DataFrame(records)], ignore_index=True)
        out = out.sort_values(["provider", "checked_at_utc"]).drop_duplicates(
            subset=["provider"],
            keep="last",
        )
        self._write_table(path, out, PROVIDER_HEALTH_COLUMNS)

    def _dedupe_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        out = self._normalize_frame(df)
        out["_source_rank"] = (~out["source"].str.startswith("legacy_")).astype(int)
        out = out.sort_values(["symbol", "ts_utc", "_source_rank"], kind="mergesort")
        out = out.drop_duplicates(
            subset=["symbol", "ts_utc", "timeframe", "adjustment"],
            keep="last",
        ).reset_index(drop=True)
        return out.drop(columns="_source_rank")

    def _has_business_gaps(self, df: pd.DataFrame) -> bool:
        if df is None or df.empty:
            return False

        local_dates = sorted({ts.date() for ts in pd.to_datetime(df["ts_utc"], utc=True)})
        if len(local_dates) <= 1:
            return False

        expected = {stamp.date() for stamp in pd.bdate_range(local_dates[0], local_dates[-1])}
        actual = set(local_dates)
        return expected != actual

    def _build_coverage_record(
        self,
        df: pd.DataFrame,
        *,
        timeframe: str,
        adjustment: str,
        market: str,
        symbol: str,
    ) -> CoverageRecord | None:
        if df is None or df.empty:
            return None

        out = self._normalize_frame(df)
        min_date = out["ts_utc"].min().date()
        max_date = out["ts_utc"].max().date()
        synced_at = datetime.now(timezone.utc).isoformat()
        latest_provider = str(out.sort_values("ts_utc").iloc[-1]["source"])
        return CoverageRecord(
            dataset="history_bars",
            timeframe=timeframe,
            adjustment=adjustment,
            market=market,
            symbol=symbol,
            provider=latest_provider,
            min_date=min_date,
            max_date=max_date,
            row_count=int(len(out)),
            last_synced_at_utc=synced_at,
            last_success_at_utc=synced_at,
            has_gaps=self._has_business_gaps(out),
            schema_version=SCHEMA_VERSION,
        )

    def upsert_bars(self, df: pd.DataFrame) -> list[CoverageRecord]:
        out = self._normalize_frame(df)
        if out.empty:
            return []

        partitions = out.assign(_year=out["ts_utc"].dt.year)
        affected_keys: set[tuple[str, str, str, str]] = set()

        for keys, frame in partitions.groupby(
            ["timeframe", "adjustment", "market", "symbol", "_year"],
            sort=True,
        ):
            timeframe, adjustment, market, symbol, year = keys
            target = self.year_path(timeframe, adjustment, market, symbol, int(year))
            existing = pd.read_parquet(target) if target.exists() else self._empty_bars()
            merged = self._dedupe_rows(pd.concat([existing, frame[BARS_SCHEMA_COLUMNS]], ignore_index=True))
            target.parent.mkdir(parents=True, exist_ok=True)
            merged.to_parquet(target, index=False)
            affected_keys.add((str(timeframe), str(adjustment), str(market), str(symbol)))

        coverage_df = self.load_coverage()
        coverage_records: list[CoverageRecord] = []
        if not coverage_df.empty:
            mask = pd.Series(False, index=coverage_df.index)
            for timeframe, adjustment, market, symbol in affected_keys:
                mask = mask | (
                    (coverage_df["dataset"] == "history_bars")
                    & (coverage_df["timeframe"].astype(str) == timeframe)
                    & (coverage_df["adjustment"].astype(str) == adjustment)
                    & (coverage_df["market"].astype(str) == market)
                    & (coverage_df["symbol"].astype(str) == symbol)
                )
            coverage_df = coverage_df.loc[~mask].reset_index(drop=True)

        for timeframe, adjustment, market, symbol in sorted(affected_keys):
            current = self.load_symbol_bars(
                symbol=symbol,
                market=market,
                timeframe=timeframe,
                adjustment=adjustment,
            )
            record = self._build_coverage_record(
                current,
                timeframe=timeframe,
                adjustment=adjustment,
                market=market,
                symbol=symbol,
            )
            if record is not None:
                coverage_records.append(record)

        merged_coverage = pd.concat(
            [coverage_df, pd.DataFrame([record.model_dump(mode="json") for record in coverage_records])],
            ignore_index=True,
        )
        self._write_table(self.coverage_path(), merged_coverage, COVERAGE_COLUMNS)
        return coverage_records

    def find_missing_windows(
        self,
        *,
        symbol: str,
        market: str,
        timeframe: str,
        adjustment: str,
        start_date: date,
        end_date: date,
    ) -> list[tuple[date, date, int]]:
        if start_date > end_date:
            return []

        bars = self.load_symbol_bars(
            symbol=symbol,
            market=market,
            timeframe=timeframe,
            adjustment=adjustment,
            start_date=start_date,
            end_date=end_date,
        )
        local_dates = {ts.date() for ts in pd.to_datetime(bars["ts_utc"], utc=True)}
        missing_dates = [
            stamp.date()
            for stamp in pd.bdate_range(start_date, end_date, inclusive="both")
            if stamp.date() not in local_dates
        ]
        if not missing_dates:
            return []

        windows: list[tuple[date, date, int]] = []
        window_start = missing_dates[0]
        window_end = missing_dates[0]
        window_count = 1

        for current in missing_dates[1:]:
            expected_next = pd.bdate_range(window_end, periods=2)[1].date()
            if current == expected_next:
                window_end = current
                window_count += 1
                continue
            windows.append((window_start, window_end, window_count))
            window_start = current
            window_end = current
            window_count = 1

        windows.append((window_start, window_end, window_count))
        return windows
