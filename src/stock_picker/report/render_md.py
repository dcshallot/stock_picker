"""Markdown report rendering.

This module generates user-facing run reports from pipeline artifacts.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def _format_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.4f}"
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            value = value.tz_localize("UTC")
        return value.tz_convert("UTC").isoformat()
    text = str(value)
    return text


def _markdown_table(df: pd.DataFrame, columns: list[str]) -> str:
    if df.empty:
        return "No candidates available."

    available_cols = [c for c in columns if c in df.columns]
    if not available_cols:
        return "No display columns available."

    header = "| " + " | ".join(available_cols) + " |"
    separator = "| " + " | ".join(["---"] * len(available_cols)) + " |"

    rows = [header, separator]
    for _, row in df[available_cols].iterrows():
        cells = [_format_cell(row[col]) for col in available_cols]
        rows.append("| " + " | ".join(cells) + " |")
    return "\n".join(rows)


def render_portfolio_markdown(
    run_summary: dict[str, Any],
    candidates_df: pd.DataFrame,
    diagnostics: dict[str, Any],
) -> str:
    """Render markdown report for one run."""

    generated_at = datetime.now(timezone.utc).isoformat()
    top_df = candidates_df.head(20)
    table = _markdown_table(
        top_df,
        columns=[
            "rank",
            "symbol",
            "market",
            "currency",
            "score",
            "forecast_return_5d",
            "ret_1d",
            "close",
            "volume",
        ],
    )

    quality = diagnostics.get("quality", {})
    fetch = diagnostics.get("fetch", {})
    provider_limits = diagnostics.get("provider_limits", {})
    universe_filter = diagnostics.get("universe_filter", {})

    diagnostics_lines = [
        f"- quality.total_rows: {quality.get('total_rows', 0)}",
        f"- quality.flagged_rows: {quality.get('flagged_rows', 0)}",
        f"- quality.missing_ratio: {quality.get('missing_ratio', 0):.4f}",
        f"- quality.delayed_markers: {quality.get('delayed_markers', 0)}",
        f"- fetch.permission_denied: {len(fetch.get('permission_denied', []))}",
        f"- fetch.rate_limited: {len(fetch.get('rate_limited', []))}",
        f"- fetch.not_supported: {len(fetch.get('not_supported', []))}",
    ]

    provider_limit_lines = ["- provider_limits: none"]
    futu_limits = provider_limits.get("futu", {}).get("history_kline_quota", {})
    if futu_limits:
        provider_limit_lines = [
            f"- futu.history_kline_quota.status: {futu_limits.get('status', 'unknown')}",
            f"- futu.history_kline_quota.used_quota: {futu_limits.get('used_quota', 0)}",
            f"- futu.history_kline_quota.remain_quota: {futu_limits.get('remain_quota', 0)}",
            f"- futu.history_kline_quota.detail_count: {futu_limits.get('detail_count', 0)}",
            "- futu.history_kline_quota.configured_quota_budget_30d: "
            f"{futu_limits.get('configured_quota_budget_30d', 0)}",
            "- futu.history_kline_quota.configured_warn_remaining_below: "
            f"{futu_limits.get('configured_warn_remaining_below', 0)}",
            "- futu.history_kline_quota.estimated_new_quota_symbols: "
            f"{futu_limits.get('estimated_new_quota_symbols', 0)}",
        ]
        warnings = futu_limits.get("warnings", [])
        if warnings:
            for warning in warnings:
                provider_limit_lines.append(f"- futu.history_kline_quota.warning: {warning}")

    lines = [
        "# Portfolio Candidates",
        "",
        "## Run Summary",
        f"- run_id: {run_summary.get('run_id', '')}",
        f"- generated_at_utc: {generated_at}",
        f"- output_timezone: {run_summary.get('output_timezone', 'UTC')} (placeholder)",
        f"- brokers: {', '.join(run_summary.get('brokers', []))}",
        f"- universe_source: {run_summary.get('universe_source', 'unknown')}",
        f"- universe_size: {run_summary.get('universe_size', 0)}",
        f"- bars_rows: {run_summary.get('bars_rows', 0)}",
        f"- min_date: {run_summary.get('min_date', '')}",
        f"- max_date: {run_summary.get('max_date', '')}",
        "- bars_max_date_lag_trading_days: "
        f"{run_summary.get('bars_max_date_lag_trading_days', '')}",
        f"- quotes_rows: {run_summary.get('quotes_rows', 0)}",
        f"- features_rows: {run_summary.get('features_rows', 0)}",
        f"- candidates_rows: {run_summary.get('candidates_rows', 0)}",
    ]

    if universe_filter:
        lines.extend(
            [
                "",
                "## Universe Filter Summary",
                f"- status: {universe_filter.get('status', 'unknown')}",
                f"- source: {universe_filter.get('source', '')}",
                f"- name: {universe_filter.get('name', '')}",
                f"- market: {universe_filter.get('market', '')}",
                f"- plate_code: {universe_filter.get('plate_code', '')}",
                f"- results_count: {universe_filter.get('results_count', 0)}",
                f"- universe_rows: {universe_filter.get('universe_rows', 0)}",
            ]
        )

    lines.extend(
        [
        "",
        "## Top Candidates",
        table,
        "",
        "## Diagnostics",
        *diagnostics_lines,
        "",
        "## Provider Limits",
        *provider_limit_lines,
        "",
        ]
    )

    return "\n".join(lines)


def write_markdown_report(path: str | Path, content: str) -> Path:
    """Write markdown content to disk."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return target
