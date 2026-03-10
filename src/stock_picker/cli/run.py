"""CLI entrypoint for one-shot stock picking pipeline.

Pipeline stages:
1. load config
2. build universe
3. route symbols to providers and initialize implemented connectors
4. fetch data with cache
5. normalize and quality tagging
6. feature engineering
7. optional model step (Prophet)
8. selection
9. report and artifacts
"""

from __future__ import annotations

import argparse
import json
import logging
import platform
import shutil
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from rich.console import Console

from stock_picker.brokers.base import BrokerConnector
from stock_picker.brokers.futu import FutuConnector
from stock_picker.brokers.ibkr_cp import IbkrCpConnector
from stock_picker.brokers.ibkr_tws import IbkrTwsConnector
from stock_picker.config.loader import apply_cli_overrides, load_config
from stock_picker.data.fetch import build_sync_plans, fetch_market_data, sync_historical_bars
from stock_picker.data.historical_store import HistoricalStore
from stock_picker.data.normalize import normalize_quotes
from stock_picker.data.quality import summarize_quality_flags, tag_quality_flags
from stock_picker.data.router import build_symbol_fetch_results, resolve_provider_assignments
from stock_picker.providers.base import HistoricalBarsProvider, ProviderRegistry
from stock_picker.providers.futu import FutuHistoricalBarsProvider
from stock_picker.providers.yahoo import YahooHistoricalBarsProvider
from stock_picker.report.render_md import render_portfolio_markdown, write_markdown_report
from stock_picker.research.features import build_features
from stock_picker.research.models.prophet import run_prophet
from stock_picker.research.selection.rules import apply_hard_filters
from stock_picker.research.selection.scorer import score_and_rank
from stock_picker.universe.load_watchlist import load_symbols, load_watchlist
from stock_picker.universe.futu_filter_loader import load_from_futu_filter
from stock_picker.universe.rule_screener import screen_universe_from_rules

LOGGER = logging.getLogger("stock_picker")
CONSOLE = Console()
IMPLEMENTED_PROVIDERS = ("futu", "yahoo", "ibkr_tws", "ibkr_cp")
LEGACY_BROKER_CHOICES = ("futu", "ibkr_tws", "ibkr_cp")
MAX_RECENT_RUNS = 3


def build_parser() -> argparse.ArgumentParser:
    """Build CLI parser."""

    parser = argparse.ArgumentParser(description="Run stock_picker one-shot pipeline.")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML (default: config.yaml)")
    parser.add_argument("--watchlist", help="Override watchlist path")
    parser.add_argument(
        "--symbol",
        action="append",
        help="Explicit symbol spec, repeatable. Format: US.AAPL or HK.00700:HKD",
    )
    parser.add_argument("--rules", help="Override rules path")
    parser.add_argument(
        "--universe-mode",
        choices=["watchlist", "rules", "futu_filter"],
        help="Universe source mode. Default comes from config.",
    )
    parser.add_argument("--filter-spec", help="Override futu filter JSON spec path")
    parser.add_argument("--filter-market", help="Override futu filter market, e.g. HK")
    parser.add_argument("--filter-plate-code", help="Override futu filter plate code")
    parser.add_argument(
        "--provider",
        action="append",
        help="Provider to use; repeatable. Examples: futu, us_primary, cn_primary",
    )
    parser.add_argument(
        "--broker",
        action="append",
        choices=LEGACY_BROKER_CHOICES,
        help="Deprecated alias of --provider for legacy connector names",
    )
    parser.add_argument(
        "--allowed-market",
        action="append",
        help="Restrict this run to the given market(s), repeatable. Example: HK",
    )
    parser.add_argument("--start-date", help="Override start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Override end date (YYYY-MM-DD)")
    parser.add_argument("--out", default="outputs", help="Output root dir (default: outputs)")
    parser.add_argument("--force-refresh", action="store_true", help="Ignore cache and refetch")
    parser.add_argument("--bars-only", action="store_true", help="Fetch historical bars only and skip quotes")
    parser.add_argument("--dry-run", action="store_true", help="Print execution plan only")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log verbosity",
    )
    return parser


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _parse_date(value: str | None, arg_name: str) -> date | None:
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{arg_name} must be in YYYY-MM-DD format, got: {value}") from exc


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _collect_env() -> dict[str, Any]:
    return {
        "python": sys.version,
        "platform": platform.platform(),
        "cwd": str(Path.cwd()),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def _build_universe(
    config: Any,
    cli_symbols: list[str] | None = None,
    futu_connector: FutuConnector | None = None,
) -> tuple[pd.DataFrame, str, dict[str, Any], pd.DataFrame, dict[str, Any]]:
    if cli_symbols:
        return load_symbols(cli_symbols), "cli_symbols", {}, pd.DataFrame(), {}

    mode = str(getattr(config.universe, "mode", "") or "").strip().lower()
    watchlist_path = config.universe.watchlist_path
    rules_path = config.universe.rules_path

    if mode == "futu_filter":
        if futu_connector is None:
            raise ValueError("universe.mode=futu_filter requires futu connector configuration.")
        universe_df, filter_diag, raw_filter_df, request_payload = load_from_futu_filter(
            config,
            futu_connector,
        )
        return universe_df, "futu_filter", filter_diag, raw_filter_df, request_payload

    if mode == "watchlist":
        if watchlist_path:
            return load_watchlist(watchlist_path), "watchlist", {}, pd.DataFrame(), {}
        raise ValueError("universe.mode=watchlist requires universe.watchlist_path")

    if mode == "rules":
        if rules_path:
            return screen_universe_from_rules(rules_path), "rules", {}, pd.DataFrame(), {}
        raise ValueError("universe.mode=rules requires universe.rules_path")

    if watchlist_path:
        return load_watchlist(watchlist_path), "watchlist", {}, pd.DataFrame(), {}
    if rules_path:
        return screen_universe_from_rules(rules_path), "rules", {}, pd.DataFrame(), {}

    raise ValueError(
        "No universe source configured. Please set universe.mode and corresponding paths."
    )


def _normalize_names(values: list[str] | None) -> list[str]:
    names = [str(value).strip() for value in values or []]
    return [name for name in names if name]


def _selected_provider_names(args: argparse.Namespace) -> list[str] | None:
    names = _normalize_names(args.provider) + _normalize_names(args.broker)
    if not names:
        return None

    deduped: list[str] = []
    for name in names:
        if name not in deduped:
            deduped.append(name)
    return deduped


def _init_connectors(config: Any, provider_names: list[str]) -> list[BrokerConnector]:
    instances: list[BrokerConnector] = []

    for provider_name in provider_names:
        provider_cfg = config.get_provider_config(provider_name)
        if provider_cfg is None:
            continue

        if provider_name == "futu":
            instances.append(FutuConnector(provider_cfg.model_dump()))
        elif provider_name == "ibkr_tws":
            instances.append(IbkrTwsConnector(provider_cfg.model_dump()))
        elif provider_name == "ibkr_cp":
            instances.append(IbkrCpConnector(provider_cfg.model_dump()))

    return instances


def _all_routed_provider_names(routing_diagnostics: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for item in routing_diagnostics.get("assigned_symbols", []):
        for provider_name in [
            str(item.get("provider", "")),
            *[str(name) for name in item.get("fallback_providers", [])],
        ]:
            if provider_name and provider_name not in names:
                names.append(provider_name)
    return names


def _init_historical_providers(config: Any, provider_names: list[str]) -> ProviderRegistry:
    instances: list[HistoricalBarsProvider] = []

    for provider_name in provider_names:
        provider_cfg = config.get_provider_config(provider_name)
        if provider_cfg is None:
            continue

        payload = provider_cfg.model_dump()
        if provider_name == "futu":
            instances.append(FutuHistoricalBarsProvider(payload))
        elif provider_name == "yahoo":
            instances.append(YahooHistoricalBarsProvider(payload))

    return ProviderRegistry(instances)


def _build_demo_candidates(universe_df: pd.DataFrame) -> pd.DataFrame:
    if universe_df is None or universe_df.empty:
        return pd.DataFrame(
            [
                {
                    "rank": 1,
                    "symbol": "DEMO.US",
                    "market": "US",
                    "currency": "USD",
                    "close": 100.0,
                    "volume": 100000,
                    "ret_1d": 0.01,
                    "forecast_return_5d": 0.02,
                    "score": 1.0,
                }
            ]
        )

    rows: list[dict[str, Any]] = []
    for i, row in universe_df.head(10).reset_index(drop=True).iterrows():
        rows.append(
            {
                "rank": i + 1,
                "symbol": row["symbol"],
                "market": row.get("market", "UNKNOWN"),
                "currency": row.get("currency", "UNKNOWN"),
                "close": float(100 + i * 3),
                "volume": int(100000 + i * 5000),
                "ret_1d": round(0.005 * (i + 1), 4),
                "forecast_return_5d": round(0.01 * (i + 1), 4),
                "score": round(1.0 - i * 0.05, 4),
            }
        )

    return pd.DataFrame(rows)


def _build_passthrough_candidates(features_df: pd.DataFrame) -> pd.DataFrame:
    if features_df.empty:
        return pd.DataFrame()

    out = features_df.copy()
    out["forecast_return_5d"] = 0.0
    out["score"] = 0.0
    out = out.sort_values(["market", "symbol"]).reset_index(drop=True)
    out["rank"] = range(1, len(out) + 1)
    return out


def _bars_date_range_utc(bars_df: pd.DataFrame) -> tuple[str, str]:
    """Return min/max UTC dates (YYYY-MM-DD) for bars_df.ts_utc."""

    if bars_df.empty or "ts_utc" not in bars_df.columns:
        return "", ""

    ts = pd.to_datetime(bars_df["ts_utc"], utc=True, errors="coerce").dropna()
    if ts.empty:
        return "", ""

    return ts.min().date().isoformat(), ts.max().date().isoformat()


def _bars_max_date_lag_trading_days(
    bars_df: pd.DataFrame,
    *,
    reference_date: date | None = None,
) -> int | None:
    """Trading-day lag between bars max date and reference date."""

    if bars_df.empty or "ts_utc" not in bars_df.columns:
        return None

    ts = pd.to_datetime(bars_df["ts_utc"], utc=True, errors="coerce").dropna()
    if ts.empty:
        return None

    max_date = ts.max().date()
    ref = reference_date or datetime.now(timezone.utc).date()
    if max_date >= ref:
        return 0

    start = max_date + timedelta(days=1)
    if start > ref:
        return 0
    return int(len(pd.bdate_range(start=start, end=ref, inclusive="both")))


def _empty_fetch_diagnostics() -> dict[str, Any]:
    return {
        "brokers": {},
        "providers": {},
        "permission_denied": [],
        "rate_limited": [],
        "not_supported": [],
        "other_errors": [],
    }


def _merge_fetch_diagnostics(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    merged_brokers = dict(merged.get("brokers", {}))

    for name, provider_diag in (extra.get("brokers") or {}).items():
        if name not in merged_brokers:
            merged_brokers[name] = provider_diag
            continue

        current = dict(merged_brokers[name])
        current["quotes_rows"] = int(provider_diag.get("quotes_rows", current.get("quotes_rows", 0)) or 0)
        current["quotes_status"] = str(
            provider_diag.get("quotes_status", current.get("quotes_status", "skipped"))
        )
        current["quotes_details"] = provider_diag.get(
            "quotes_details",
            current.get("quotes_details", {"ok": [], "errors": []}),
        )

        bars_status = current.get("bars_status", "skipped")
        quotes_status = current.get("quotes_status", "skipped")
        error_like = {item for item in (bars_status, quotes_status) if item not in {"ok", "skipped"}}
        if error_like:
            current["status"] = (
                "partial" if {"ok", "skipped"} & {bars_status, quotes_status} else "error"
            )
        elif current.get("bars_details", {}).get("errors"):
            current["status"] = "partial"
        else:
            current["status"] = "ok"

        merged_brokers[name] = current

    merged["brokers"] = merged_brokers
    for key in ("permission_denied", "rate_limited", "not_supported", "other_errors"):
        merged[key] = list(merged.get(key, [])) + list(extra.get(key, []))
    merged["providers"] = dict(merged_brokers)
    return merged


def _collect_provider_limits(
    config: Any,
    connectors: list[BrokerConnector],
    assignment_frames: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    """Run provider-specific quota/limit preflight checks."""

    results: dict[str, Any] = {}

    for connector in connectors:
        if connector.name != "futu":
            continue

        provider_cfg = config.get_provider_config(connector.name)
        if provider_cfg is None:
            continue

        history_cfg = getattr(provider_cfg, "history_kline", None)
        if history_cfg is None or not getattr(history_cfg, "check_quota_before_run", False):
            results[connector.name] = {
                "history_kline_quota": {
                    "status": "skipped",
                    "message": "quota check disabled by config",
                }
            }
            continue

        quota_fn = getattr(connector, "get_history_kline_quota", None)
        if not callable(quota_fn):
            results[connector.name] = {
                "history_kline_quota": {
                    "status": "skipped",
                    "message": "provider does not implement quota preflight",
                }
            }
            continue

        quota_result = quota_fn(get_detail=True)
        if quota_result.get("status") == "ok":
            assigned_df = assignment_frames.get(connector.name, pd.DataFrame())
            requested_codes: list[str] = []
            if not assigned_df.empty:
                try:
                    mapping = connector.resolve_instruments(assigned_df)
                    requested_codes = sorted(
                        {
                            str(item.get("broker_symbol", "")).strip()
                            for item in mapping.values()
                            if str(item.get("broker_symbol", "")).strip()
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    quota_result.setdefault("warnings", []).append(
                        f"failed_to_estimate_requested_codes:{exc}"
                    )

            detail_codes = {
                str(item.get("code", "")).strip()
                for item in quota_result.get("detail_list", [])
                if str(item.get("code", "")).strip()
            }
            estimated_new_codes = [code for code in requested_codes if code not in detail_codes]
            remain_quota = int(quota_result.get("remain_quota", 0) or 0)
            quota_budget_30d = int(getattr(history_cfg, "quota_budget_30d", 0) or 0)
            warn_remaining_below = int(getattr(history_cfg, "warn_remaining_below", 0) or 0)

            quota_result["configured_quota_budget_30d"] = quota_budget_30d
            quota_result["configured_warn_remaining_below"] = warn_remaining_below
            quota_result["requested_codes"] = requested_codes
            quota_result["estimated_new_quota_symbols"] = len(estimated_new_codes)
            quota_result["estimated_new_quota_codes"] = estimated_new_codes
            quota_result.setdefault("warnings", [])

            if warn_remaining_below and remain_quota <= warn_remaining_below:
                quota_result["warnings"].append(
                    f"remaining_quota_below_threshold:{remain_quota}<={warn_remaining_below}"
                )
            if quota_budget_30d and int(quota_result.get("used_quota", 0)) >= quota_budget_30d:
                quota_result["warnings"].append(
                    f"used_quota_reached_configured_budget:{quota_result.get('used_quota', 0)}>={quota_budget_30d}"
                )
            if len(estimated_new_codes) > remain_quota:
                quota_result["warnings"].append(
                    "estimated_new_symbols_exceed_remaining_quota:"
                    f"{len(estimated_new_codes)}>{remain_quota}"
                )

            if quota_result["warnings"]:
                quota_result["status"] = "warning"

        results[connector.name] = {
            "history_kline_quota": quota_result,
        }

    return results


def _combine_assignment_frames(
    assignment_frames: dict[str, pd.DataFrame],
    columns: list[str],
) -> pd.DataFrame:
    if not assignment_frames:
        return pd.DataFrame(columns=columns)

    frames = [frame for frame in assignment_frames.values() if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)


def _prune_old_runs(out_root: Path, keep: int = MAX_RECENT_RUNS) -> list[str]:
    """Delete old run directories and keep only the newest N by run-id name."""

    if keep < 0:
        keep = 0

    run_dirs = sorted(
        [
            path
            for path in out_root.iterdir()
            if path.is_dir() and path.name.startswith("run_")
        ],
        key=lambda path: path.name,
        reverse=True,
    )

    removed: list[str] = []
    for path in run_dirs[keep:]:
        shutil.rmtree(path, ignore_errors=False)
        removed.append(path.name)

    return removed


def _dry_run_print(
    config: Any,
    provider_names: list[str],
    universe_df: pd.DataFrame,
    routed_universe_df: pd.DataFrame,
    routing_diagnostics: dict[str, Any],
    out_dir: Path,
    universe_source: str,
) -> None:
    CONSOLE.print("[bold]Dry-run plan[/bold]")
    CONSOLE.print(f"- Output root: {out_dir}")
    CONSOLE.print(f"- Providers: {', '.join(provider_names) if provider_names else '(none)'}")
    CONSOLE.print(f"- Universe rows: {len(universe_df)}")
    CONSOLE.print(f"- Universe source: {universe_source}")
    CONSOLE.print(f"- Routed rows: {len(routed_universe_df)}")
    CONSOLE.print(
        f"- Skipped by routing: {len(routing_diagnostics.get('skipped_symbols', []))}"
    )
    CONSOLE.print(f"- Time range: {config.run.start_date} -> {config.run.end_date}")
    CONSOLE.print(
        "- Steps: load_config -> universe -> routing -> sync_history_bars -> normalize -> "
        "features -> models -> selection -> report"
    )


def main(argv: list[str] | None = None) -> int:
    """CLI main."""

    parser = build_parser()
    args = parser.parse_args(argv)

    _setup_logging(args.log_level)

    diagnostics: dict[str, Any] = {
        "pipeline_steps": [],
        "universe_filter": {},
        "routing": {},
        "provider_limits": {},
        "capabilities": {},
        "fetch": {},
        "quality": {},
        "models": {},
        "selection": {},
    }

    try:
        start_date = _parse_date(args.start_date, "--start-date")
        end_date = _parse_date(args.end_date, "--end-date")

        config = load_config(args.config)
        diagnostics["pipeline_steps"].append("1_load_config")

        config = apply_cli_overrides(
            config,
            watchlist_path=args.watchlist,
            rules_path=args.rules,
            universe_mode=args.universe_mode,
            filter_spec_path=args.filter_spec,
            filter_market=args.filter_market,
            filter_plate_code=args.filter_plate_code,
            start_date=start_date,
            end_date=end_date,
            out_dir=args.out,
        )

        futu_filter_connector: FutuConnector | None = None
        if str(config.universe.mode).strip().lower() == "futu_filter":
            futu_cfg = config.get_provider_config("futu")
            if futu_cfg is None:
                raise ValueError("universe.mode=futu_filter requires providers.futu config.")
            futu_filter_connector = FutuConnector(futu_cfg.model_dump())

        try:
            universe_df, universe_source, universe_filter_diag, filter_results_df, filter_request_payload = _build_universe(
                config,
                cli_symbols=args.symbol,
                futu_connector=futu_filter_connector,
            )
        except Exception as exc:  # noqa: BLE001
            diagnostics["universe_filter"] = {
                "status": "error",
                "source": "futu_filter",
                "error_type": exc.__class__.__name__,
                "error_message": str(exc),
            }
            raise ValueError(f"Failed to build universe: {exc}") from exc
        diagnostics["universe_filter"] = universe_filter_diag
        diagnostics["pipeline_steps"].append("2_build_universe")

        selected_provider_names = _selected_provider_names(args)
        allowed_markets = _normalize_names(args.allowed_market)
        assignment_frames, routing_diagnostics = resolve_provider_assignments(
            universe_df,
            config,
            dataset="history_bars",
            implemented_providers=set(IMPLEMENTED_PROVIDERS),
            selected_providers=selected_provider_names,
            allowed_markets=allowed_markets,
        )
        diagnostics["routing"] = routing_diagnostics
        diagnostics["pipeline_steps"].append("3_route_providers")

        routed_universe_df = _combine_assignment_frames(assignment_frames, list(universe_df.columns))
        provider_names = _all_routed_provider_names(routing_diagnostics)
        connectors = _init_connectors(config, provider_names)
        history_registry = _init_historical_providers(config, provider_names)
        history_store = HistoricalStore(config.data.history_bars.store_dir)

        diagnostics["pipeline_steps"].append("4_init_connectors")
        provider_health_records: list[dict[str, Any]] = []
        for provider in history_registry.values():
            cap = provider.capabilities_check()
            diagnostics["capabilities"][provider.name] = cap.model_dump(mode="json")
            provider_health_records.append(
                {
                    "provider": provider.name,
                    "supports_history_bars": cap.supports_history_bars,
                    "available": cap.available,
                    "requires_network": cap.requires_network,
                    "notes": ";".join(cap.notes),
                    "checked_at_utc": datetime.now(timezone.utc).isoformat(),
                }
            )

        for connector in connectors:
            if connector.name in diagnostics["capabilities"]:
                continue
            cap = connector.capabilities_check()
            diagnostics["capabilities"][connector.name] = cap.model_dump(mode="json")

    except FileNotFoundError as exc:
        LOGGER.error(str(exc))
        if str(args.config).endswith("config.yaml"):
            LOGGER.error("Hint: copy config.example.yaml to config.yaml and adjust parameters.")
        return 2
    except ValueError as exc:
        LOGGER.error(str(exc))
        return 2

    out_root = Path(config.run.out_dir)
    run_id = datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M")
    run_dir = out_root / run_id

    if args.dry_run:
        _dry_run_print(
            config,
            provider_names,
            universe_df,
            routed_universe_df,
            routing_diagnostics,
            out_root,
            universe_source,
        )
        return 0

    diagnostics["provider_limits"] = _collect_provider_limits(config, connectors, assignment_frames)
    diagnostics["pipeline_steps"].append("5_provider_limits")
    history_store.record_provider_health(provider_health_records)

    include_quotes = bool(config.data.quotes.enabled) and not args.bars_only
    sync_plans = build_sync_plans(
        routed_universe_df,
        routing_diagnostics,
        store=history_store,
        timeframe=config.data.history_bars.timeframe,
        adjustment=config.data.history_bars.adjustment,
        start_date=config.run.start_date,
        end_date=config.run.end_date,
        bootstrap_start_date=config.data.history_bars.bootstrap_start_date,
        repair_window_days=config.data.history_bars.repair_window_days,
        max_gap_days_before_full_resync=config.data.history_bars.max_gap_days_before_full_resync,
        force_refresh=args.force_refresh,
    )

    bars_df, fetch_diag = sync_historical_bars(
        sync_plans,
        registry=history_registry,
        store=history_store,
        run_id=run_id,
    )
    raw_quotes_df = pd.DataFrame()

    if include_quotes and connectors:
        _unused_bars_df, raw_quotes_df, quote_diag = fetch_market_data(
            connectors=connectors,
            universe_df=routed_universe_df,
            universe_by_connector=assignment_frames,
            timeframe=config.data.history_bars.timeframe,
            adjustment=config.data.history_bars.adjustment,
            start_date=config.run.start_date,
            end_date=config.run.end_date,
            include_bars=False,
            include_quotes=True,
        )
        fetch_diag = _merge_fetch_diagnostics(fetch_diag, quote_diag)

    diagnostics["pipeline_steps"].append("6_sync_history_bars")
    diagnostics["fetch"] = fetch_diag

    bars_df = tag_quality_flags(bars_df, max_missing_ratio=config.data.quality.max_missing_ratio)
    quotes_df = normalize_quotes(raw_quotes_df, source="multi")
    quality_summary = summarize_quality_flags(bars_df)
    total_rows = int(quality_summary.get("total_rows", 0))
    flagged_rows = int(quality_summary.get("flagged_rows", 0))
    quality_summary["missing_ratio"] = (flagged_rows / total_rows) if total_rows else 0.0
    quality_summary["delayed_markers"] = (
        int(raw_quotes_df["delayed"].fillna(False).astype(bool).sum())
        if "delayed" in raw_quotes_df.columns
        else 0
    )
    diagnostics["quality"] = quality_summary
    diagnostics["pipeline_steps"].append("7_normalize_quality")

    features_df = build_features(bars_df)
    diagnostics["pipeline_steps"].append("8_features")

    model_outputs_df = run_prophet(
        features_df if not features_df.empty else bars_df,
        enabled=config.models.prophet.enable,
        params=config.models.prophet.params,
        diagnostics=diagnostics,
    )
    diagnostics["pipeline_steps"].append("9_models")

    if config.selection.enabled:
        filtered_df = apply_hard_filters(features_df, model_outputs_df, config.selection.hard_filters)
        candidates_df = score_and_rank(filtered_df, config.selection.score_weights)
        diagnostics["selection"]["mode"] = "ranked"
    else:
        candidates_df = _build_passthrough_candidates(features_df)
        diagnostics["selection"]["mode"] = "passthrough"

    diagnostics["pipeline_steps"].append("10_selection")

    if candidates_df.empty:
        fallback_universe = routed_universe_df if not routed_universe_df.empty else universe_df
        candidates_df = _build_demo_candidates(fallback_universe)
        diagnostics["selection"]["used_demo_candidates"] = True
    else:
        diagnostics["selection"]["used_demo_candidates"] = False

    diagnostics["selection"]["candidate_rows"] = int(len(candidates_df))

    min_date, max_date = _bars_date_range_utc(bars_df)
    bars_lag_trading_days = _bars_max_date_lag_trading_days(
        bars_df,
        reference_date=config.run.end_date or datetime.now(timezone.utc).date(),
    )
    run_summary = {
        "run_id": run_id,
        "output_timezone": config.run.timezone,
        "brokers": provider_names,
        "providers": provider_names,
        "universe_source": universe_source,
        "universe_size": int(len(universe_df)),
        "routed_universe_size": int(len(routed_universe_df)),
        "bars_rows": int(len(bars_df)),
        "min_date": min_date,
        "max_date": max_date,
        "bars_max_date_lag_trading_days": bars_lag_trading_days,
        "quotes_rows": int(len(quotes_df)),
        "features_rows": int(len(features_df)),
        "candidates_rows": int(len(candidates_df)),
    }

    report_md = render_portfolio_markdown(run_summary, candidates_df, diagnostics)
    diagnostics["pipeline_steps"].append("11_report_artifacts")

    bar_summary_df = pd.DataFrame()
    if not bars_df.empty:
        bar_summary_df = (
            bars_df.sort_values(["symbol", "ts_utc"])
            .groupby("symbol", as_index=False)
            .tail(1)
            .sort_values(["market", "symbol"])
            .reset_index(drop=True)
        )

    symbol_fetch_results_df = build_symbol_fetch_results(
        universe_df=universe_df,
        routing_diagnostics=routing_diagnostics,
        fetch_diagnostics=fetch_diag,
        bars_df=bars_df,
    )

    run_dir.mkdir(parents=True, exist_ok=True)
    _write_json(run_dir / "run_config.json", config.model_dump(mode="json"))
    _write_json(run_dir / "env.json", _collect_env())
    _write_json(run_dir / "diagnostics.json", diagnostics)
    bars_df.to_parquet(run_dir / "bars_snapshot.parquet", index=False)
    if universe_source == "futu_filter":
        _write_json(run_dir / "filter_request.json", filter_request_payload)
        filter_results_df.to_csv(run_dir / "filter_results.csv", index=False)
        _write_json(run_dir / "filter_meta.json", diagnostics.get("universe_filter", {}))

    candidates_df.to_csv(run_dir / "candidates.csv", index=False)
    symbol_fetch_results_df.to_csv(run_dir / "symbol_fetch_results.csv", index=False)
    if not bar_summary_df.empty:
        bar_summary_df.to_csv(run_dir / "bar_data_summary.csv", index=False)
    write_markdown_report(run_dir / "portfolio_candidates.md", report_md)

    removed_runs = _prune_old_runs(out_root, keep=MAX_RECENT_RUNS)
    diagnostics["retention"] = {
        "keep_latest_runs": MAX_RECENT_RUNS,
        "removed_runs": removed_runs,
    }
    _write_json(run_dir / "diagnostics.json", diagnostics)

    LOGGER.info("Run completed: %s", run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
