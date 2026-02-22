"""CLI entrypoint for one-shot stock picking pipeline.

Pipeline stages:
1. load config
2. build universe
3. initialize connectors and capability checks
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
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from rich.console import Console

from stock_picker.brokers.base import BrokerConnector
from stock_picker.brokers.futu import FutuConnector
from stock_picker.brokers.ibkr_cp import IbkrCpConnector
from stock_picker.brokers.ibkr_tws import IbkrTwsConnector
from stock_picker.config.loader import apply_cli_overrides, load_config
from stock_picker.data.cache import save_processed_bars
from stock_picker.data.fetch import fetch_market_data
from stock_picker.data.normalize import normalize_bars, normalize_quotes
from stock_picker.data.quality import summarize_quality_flags, tag_quality_flags
from stock_picker.report.render_md import render_portfolio_markdown, write_markdown_report
from stock_picker.research.features import build_features
from stock_picker.research.models.prophet import run_prophet
from stock_picker.research.selection.rules import apply_hard_filters
from stock_picker.research.selection.scorer import score_and_rank
from stock_picker.universe.load_watchlist import load_watchlist
from stock_picker.universe.rule_screener import screen_universe_from_rules

LOGGER = logging.getLogger("stock_picker")
CONSOLE = Console()
SUPPORTED_BROKERS = ("futu", "ibkr_tws", "ibkr_cp")


def build_parser() -> argparse.ArgumentParser:
    """Build CLI parser."""

    parser = argparse.ArgumentParser(description="Run stock_picker one-shot pipeline.")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML (default: config.yaml)")
    parser.add_argument("--watchlist", help="Override watchlist path")
    parser.add_argument("--rules", help="Override rules path")
    parser.add_argument(
        "--broker",
        action="append",
        choices=SUPPORTED_BROKERS,
        help="Broker connector to use; pass multiple times for multiple brokers",
    )
    parser.add_argument("--start-date", help="Override start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Override end date (YYYY-MM-DD)")
    parser.add_argument("--out", default="outputs", help="Output root dir (default: outputs)")
    parser.add_argument("--force-refresh", action="store_true", help="Ignore cache and refetch")
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


def _build_universe(config: Any) -> tuple[pd.DataFrame, str]:
    watchlist_path = config.universe.watchlist_path
    rules_path = config.universe.rules_path

    if watchlist_path:
        return load_watchlist(watchlist_path), "watchlist"
    if rules_path:
        return screen_universe_from_rules(rules_path), "rules"

    raise ValueError(
        "No universe source configured. Please set `universe.watchlist_path` or `universe.rules_path`."
    )


def _init_connectors(config: Any, selected: list[str] | None) -> list[BrokerConnector]:
    brokers = selected or ["futu"]
    instances: list[BrokerConnector] = []

    for broker in brokers:
        if broker == "futu":
            instances.append(FutuConnector(config.brokers.futu.model_dump()))
        elif broker == "ibkr_tws":
            instances.append(IbkrTwsConnector(config.brokers.ibkr_tws.model_dump()))
        elif broker == "ibkr_cp":
            instances.append(IbkrCpConnector(config.brokers.ibkr_cp.model_dump()))

    if not instances:
        raise ValueError("No valid brokers selected.")

    return instances


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


def _dry_run_print(config: Any, brokers: list[str], universe_df: pd.DataFrame, out_dir: Path) -> None:
    CONSOLE.print("[bold]Dry-run plan[/bold]")
    CONSOLE.print(f"- Output root: {out_dir}")
    CONSOLE.print(f"- Brokers: {', '.join(brokers)}")
    CONSOLE.print(f"- Universe rows: {len(universe_df)}")
    CONSOLE.print(f"- Time range: {config.run.start_date} -> {config.run.end_date}")
    CONSOLE.print("- Steps: load_config -> universe -> capabilities -> fetch -> normalize -> features -> models -> selection -> report")


def main(argv: list[str] | None = None) -> int:
    """CLI main."""

    parser = build_parser()
    args = parser.parse_args(argv)

    _setup_logging(args.log_level)

    diagnostics: dict[str, Any] = {
        "pipeline_steps": [],
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
            start_date=start_date,
            end_date=end_date,
            out_dir=args.out,
        )

        universe_df, universe_source = _build_universe(config)
        diagnostics["pipeline_steps"].append("2_build_universe")

        connectors = _init_connectors(config, args.broker)
        diagnostics["pipeline_steps"].append("3_init_connectors")
        for connector in connectors:
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

    selected_brokers = [c.name for c in connectors]

    if args.dry_run:
        _dry_run_print(config, selected_brokers, universe_df, out_root)
        return 0

    raw_bars_df, raw_quotes_df, fetch_diag = fetch_market_data(
        connectors=connectors,
        universe_df=universe_df,
        cache_dir=config.run.cache_dir,
        force_refresh=args.force_refresh,
    )
    diagnostics["pipeline_steps"].append("4_fetch_data")
    diagnostics["fetch"] = fetch_diag

    bars_df = normalize_bars(raw_bars_df, source="multi")
    bars_df = tag_quality_flags(bars_df, max_missing_ratio=config.data.max_missing_ratio)
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
    diagnostics["pipeline_steps"].append("5_normalize_quality")

    save_processed_bars(bars_df, config.run.cache_dir)

    features_df = build_features(bars_df)
    diagnostics["pipeline_steps"].append("6_features")

    model_outputs_df = run_prophet(
        features_df if not features_df.empty else bars_df,
        enabled=config.models.prophet.enable,
        params=config.models.prophet.params,
        diagnostics=diagnostics,
    )
    diagnostics["pipeline_steps"].append("7_models")

    filtered_df = apply_hard_filters(features_df, model_outputs_df, config.selection.hard_filters)
    candidates_df = score_and_rank(filtered_df, config.selection.score_weights)
    diagnostics["pipeline_steps"].append("8_selection")

    if candidates_df.empty:
        candidates_df = _build_demo_candidates(universe_df)
        diagnostics["selection"]["used_demo_candidates"] = True
    else:
        diagnostics["selection"]["used_demo_candidates"] = False

    diagnostics["selection"]["candidate_rows"] = int(len(candidates_df))

    run_summary = {
        "run_id": run_id,
        "output_timezone": config.run.timezone,
        "brokers": selected_brokers,
        "universe_source": universe_source,
        "universe_size": int(len(universe_df)),
        "bars_rows": int(len(bars_df)),
        "quotes_rows": int(len(quotes_df)),
        "features_rows": int(len(features_df)),
        "candidates_rows": int(len(candidates_df)),
    }

    report_md = render_portfolio_markdown(run_summary, candidates_df, diagnostics)
    diagnostics["pipeline_steps"].append("9_report_artifacts")

    run_dir.mkdir(parents=True, exist_ok=True)
    _write_json(run_dir / "run_config.json", config.model_dump(mode="json"))
    _write_json(run_dir / "env.json", _collect_env())
    _write_json(run_dir / "diagnostics.json", diagnostics)

    candidates_df.to_csv(run_dir / "candidates.csv", index=False)
    write_markdown_report(run_dir / "portfolio_candidates.md", report_md)

    LOGGER.info("Run completed: %s", run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
