"""Sync planning and execution for historical daily bars."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Sequence

import pandas as pd
from pydantic import BaseModel, Field

from stock_picker.brokers.base import NotSupported, PermissionDenied, RateLimited
from stock_picker.data.historical_store import HistoricalStore
from stock_picker.data.normalize import BARS_SCHEMA_COLUMNS, normalize_bars
from stock_picker.providers.base import HistoricalBarsRequest, ProviderRegistry


class SyncWindow(BaseModel):
    """One date window to fetch for a symbol."""

    start_date: date
    end_date: date
    reason: str


class SyncPlan(BaseModel):
    """Complete sync plan for one symbol."""

    symbol: str
    market: str
    currency: str
    timeframe: str = "1D"
    adjustment: str = "forward"
    requested_start: date
    requested_end: date
    providers: list[str] = Field(default_factory=list)
    windows: list[SyncWindow] = Field(default_factory=list)


def _merge_windows(windows: list[SyncWindow]) -> list[SyncWindow]:
    if not windows:
        return []

    ordered = sorted(windows, key=lambda item: (item.start_date, item.end_date, item.reason))
    merged: list[SyncWindow] = [ordered[0]]

    for item in ordered[1:]:
        current = merged[-1]
        if item.start_date <= current.end_date + timedelta(days=1):
            current.end_date = max(current.end_date, item.end_date)
            if item.reason not in current.reason:
                current.reason = f"{current.reason}+{item.reason}"
            continue
        merged.append(item)

    return merged


def build_sync_plans(
    universe_df: pd.DataFrame,
    routing_diagnostics: dict[str, Any],
    *,
    store: HistoricalStore,
    timeframe: str,
    adjustment: str,
    start_date: date | None,
    end_date: date | None,
    bootstrap_start_date: date,
    repair_window_days: int,
    max_gap_days_before_full_resync: int,
    force_refresh: bool = False,
) -> list[SyncPlan]:
    """Build symbol-level sync plans from current local coverage and routing."""

    if universe_df is None or universe_df.empty:
        return []

    route_map = {
        str(item.get("symbol", "")): [
            str(item.get("provider", "")),
            *[str(name) for name in item.get("fallback_providers", []) if str(name).strip()],
        ]
        for item in routing_diagnostics.get("assigned_symbols", [])
        if str(item.get("symbol", "")).strip()
    }

    plans: list[SyncPlan] = []
    requested_end = end_date or datetime.now(timezone.utc).date()

    for _, row in universe_df.iterrows():
        symbol = str(row.get("symbol", "")).strip()
        if not symbol or symbol not in route_map:
            continue

        market = str(row.get("market", "UNKNOWN")).upper()
        currency = str(row.get("currency", "UNKNOWN")).upper()
        requested_start = max(start_date or bootstrap_start_date, bootstrap_start_date)
        coverage = store.load_coverage_record(
            symbol=symbol,
            market=market,
            timeframe=timeframe,
            adjustment=adjustment,
        )

        windows: list[SyncWindow] = []
        force_full_resync = False

        if force_refresh:
            windows.append(
                SyncWindow(
                    start_date=requested_start,
                    end_date=requested_end,
                    reason="force_refresh",
                )
            )
        elif coverage is None:
            windows.append(
                SyncWindow(
                    start_date=requested_start,
                    end_date=requested_end,
                    reason="bootstrap",
                )
            )
        else:
            gap_windows = store.find_missing_windows(
                symbol=symbol,
                market=market,
                timeframe=timeframe,
                adjustment=adjustment,
                start_date=requested_start,
                end_date=min(coverage.max_date, requested_end),
            )
            gap_days = sum(window[2] for window in gap_windows)
            if gap_days > int(max_gap_days_before_full_resync):
                force_full_resync = True
            else:
                for gap_start, gap_end, _count in gap_windows:
                    windows.append(
                        SyncWindow(
                            start_date=gap_start,
                            end_date=gap_end,
                            reason="gap_fill",
                        )
                    )

            if force_full_resync:
                windows = [
                    SyncWindow(
                        start_date=requested_start,
                        end_date=requested_end,
                        reason="full_resync_due_to_gaps",
                    )
                ]
            elif coverage.max_date >= requested_end:
                repair_start = max(
                    coverage.max_date - timedelta(days=max(repair_window_days, 0)),
                    requested_start,
                )
                windows.append(
                    SyncWindow(
                        start_date=repair_start,
                        end_date=requested_end,
                        reason="repair_only",
                    )
                )
            else:
                repair_anchor = coverage.max_date - timedelta(days=max(repair_window_days, 0) - 1)
                append_start = min(repair_anchor, requested_start)
                windows.append(
                    SyncWindow(
                        start_date=append_start,
                        end_date=requested_end,
                        reason="repair_and_append",
                    )
                )

        merged = [
            window
            for window in _merge_windows(windows)
            if window.start_date <= window.end_date
        ]
        plans.append(
            SyncPlan(
                symbol=symbol,
                market=market,
                currency=currency,
                timeframe=timeframe,
                adjustment=adjustment,
                requested_start=requested_start,
                requested_end=requested_end,
                providers=[name for name in route_map[symbol] if name],
                windows=merged,
            )
        )

    return plans


def _empty_bars() -> pd.DataFrame:
    return pd.DataFrame(columns=BARS_SCHEMA_COLUMNS)


def _provider_diag(diag: dict[str, Any], provider_name: str) -> dict[str, Any]:
    providers = diag.setdefault("brokers", {})
    provider_diag = providers.setdefault(
        provider_name,
        {
            "status": "ok",
            "bars_rows": 0,
            "quotes_rows": 0,
            "bars_status": "ok",
            "quotes_status": "skipped",
            "bars_details": {"ok": [], "errors": []},
            "quotes_details": {"ok": [], "errors": []},
        },
    )
    return provider_diag


def _is_retryable_error(exc: Exception) -> bool:
    if isinstance(exc, (RateLimited, TimeoutError, ConnectionError)):
        return True
    if isinstance(exc, (PermissionDenied, NotSupported, ValueError)):
        return False
    text = str(exc).lower()
    if "empty_response" in text:
        return True
    if "timeout" in text or "temporar" in text or "rate" in text:
        return True
    return False


def sync_historical_bars(
    plans: Sequence[SyncPlan],
    *,
    registry: ProviderRegistry,
    store: HistoricalStore,
    run_id: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Execute sync plans with provider fallback and persist into the history store."""

    diagnostics: dict[str, Any] = {
        "brokers": {},
        "providers": {},
        "permission_denied": [],
        "rate_limited": [],
        "not_supported": [],
        "other_errors": [],
        "sync_plans": [plan.model_dump(mode="json") for plan in plans],
    }
    sync_run_records: list[dict[str, Any]] = []
    bars_frames: list[pd.DataFrame] = []

    for plan in plans:
        symbol_loaded = False

        for provider_name in plan.providers:
            provider = registry.get(provider_name)
            started_at = datetime.now(timezone.utc)
            provider_diag = _provider_diag(diagnostics, provider_name)

            if provider is None:
                finished_at = datetime.now(timezone.utc)
                provider_diag["bars_status"] = "error"
                provider_diag["status"] = "partial"
                provider_diag["bars_details"]["errors"].append(
                    {
                        "symbol": plan.symbol,
                        "error": "provider_not_registered",
                        "error_type": "ProviderNotRegistered",
                    }
                )
                diagnostics["other_errors"].append(
                    {"provider": provider_name, "symbol": plan.symbol, "error": "provider_not_registered"}
                )
                sync_run_records.append(
                    {
                        "run_id": run_id,
                        "symbol": plan.symbol,
                        "provider": provider_name,
                        "requested_start": plan.requested_start.isoformat(),
                        "requested_end": plan.requested_end.isoformat(),
                        "fetched_start": "",
                        "fetched_end": "",
                        "rows_fetched": 0,
                        "status": "failed",
                        "error_type": "ProviderNotRegistered",
                        "error_message": "provider_not_registered",
                        "started_at_utc": started_at.isoformat(),
                        "finished_at_utc": finished_at.isoformat(),
                    }
                )
                continue

            try:
                fetched_frames: list[pd.DataFrame] = []
                for window in plan.windows:
                    request = HistoricalBarsRequest(
                        symbol=plan.symbol,
                        market=plan.market,
                        currency=plan.currency,
                        start_date=window.start_date,
                        end_date=window.end_date,
                        timeframe=plan.timeframe,
                        adjustment=plan.adjustment,
                    )
                    result = provider.fetch_daily_bars(request)
                    normalized = normalize_bars(result.rows, source=provider.name)
                    if not normalized.empty:
                        normalized["adjustment"] = plan.adjustment
                    fetched_frames.append(normalized)

                fetched_df = (
                    pd.concat([frame for frame in fetched_frames if not frame.empty], ignore_index=True)
                    if any(not frame.empty for frame in fetched_frames)
                    else _empty_bars()
                )
                existing_before = store.load_symbol_bars(
                    symbol=plan.symbol,
                    market=plan.market,
                    timeframe=plan.timeframe,
                    adjustment=plan.adjustment,
                    start_date=plan.requested_start,
                    end_date=plan.requested_end,
                )
                if fetched_df.empty and existing_before.empty:
                    raise RuntimeError("empty_response")

                if not fetched_df.empty:
                    store.upsert_bars(fetched_df)

                loaded = store.load_symbol_bars(
                    symbol=plan.symbol,
                    market=plan.market,
                    timeframe=plan.timeframe,
                    adjustment=plan.adjustment,
                    start_date=plan.requested_start,
                    end_date=plan.requested_end,
                )
                if not loaded.empty:
                    bars_frames.append(loaded)
                rows_fetched = int(len(fetched_df))
                provider_diag["bars_rows"] += rows_fetched
                provider_diag["bars_status"] = "ok"
                provider_diag["status"] = "ok"
                provider_diag["bars_details"]["ok"].append(
                    {
                        "symbol": plan.symbol,
                        "rows": int(len(loaded)),
                    }
                )
                finished_at = datetime.now(timezone.utc)
                sync_run_records.append(
                    {
                        "run_id": run_id,
                        "symbol": plan.symbol,
                        "provider": provider_name,
                        "requested_start": plan.requested_start.isoformat(),
                        "requested_end": plan.requested_end.isoformat(),
                        "fetched_start": plan.windows[0].start_date.isoformat() if plan.windows else "",
                        "fetched_end": plan.windows[-1].end_date.isoformat() if plan.windows else "",
                        "rows_fetched": rows_fetched,
                        "status": "success",
                        "error_type": "",
                        "error_message": "",
                        "started_at_utc": started_at.isoformat(),
                        "finished_at_utc": finished_at.isoformat(),
                    }
                )
                symbol_loaded = True
                break
            except Exception as exc:  # noqa: BLE001
                finished_at = datetime.now(timezone.utc)
                error_type = exc.__class__.__name__
                message = str(exc)
                provider_diag["bars_status"] = "error"
                provider_diag["status"] = "partial"
                provider_diag["bars_details"]["errors"].append(
                    {
                        "symbol": plan.symbol,
                        "error": message,
                        "error_type": error_type,
                    }
                )
                sync_run_records.append(
                    {
                        "run_id": run_id,
                        "symbol": plan.symbol,
                        "provider": provider_name,
                        "requested_start": plan.requested_start.isoformat(),
                        "requested_end": plan.requested_end.isoformat(),
                        "fetched_start": plan.windows[0].start_date.isoformat() if plan.windows else "",
                        "fetched_end": plan.windows[-1].end_date.isoformat() if plan.windows else "",
                        "rows_fetched": 0,
                        "status": "failed",
                        "error_type": error_type,
                        "error_message": message,
                        "started_at_utc": started_at.isoformat(),
                        "finished_at_utc": finished_at.isoformat(),
                    }
                )

                if isinstance(exc, PermissionDenied):
                    diagnostics["permission_denied"].append(f"{provider_name}:bars")
                elif isinstance(exc, RateLimited):
                    diagnostics["rate_limited"].append(f"{provider_name}:bars")
                elif isinstance(exc, NotSupported):
                    diagnostics["not_supported"].append(f"{provider_name}:bars")
                else:
                    diagnostics["other_errors"].append(
                        {"provider": provider_name, "symbol": plan.symbol, "error": message}
                    )

                if not _is_retryable_error(exc):
                    break

        if not symbol_loaded:
            existing = store.load_symbol_bars(
                symbol=plan.symbol,
                market=plan.market,
                timeframe=plan.timeframe,
                adjustment=plan.adjustment,
                start_date=plan.requested_start,
                end_date=plan.requested_end,
            )
            if not existing.empty:
                bars_frames.append(existing)

    if sync_run_records:
        store.record_sync_runs(sync_run_records)

    diagnostics["providers"] = dict(diagnostics["brokers"])
    if not bars_frames:
        return _empty_bars(), diagnostics

    out = pd.concat(bars_frames, ignore_index=True)
    out = out.sort_values(["symbol", "ts_utc"]).drop_duplicates(
        subset=["symbol", "ts_utc", "timeframe", "adjustment"],
        keep="last",
    )
    return out.reset_index(drop=True), diagnostics
