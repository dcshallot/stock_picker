"""Config loader and CLI override utilities.

This module handles YAML loading, schema validation, and runtime overrides.
It keeps parsing logic out of CLI orchestration.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import yaml
from pydantic import ValidationError

from stock_picker.config.schema import AppConfig


def _upgrade_legacy_config(raw: dict) -> dict:
    """Upgrade older config shapes to the current schema."""

    data = dict(raw)

    if "providers" not in data and "brokers" in data:
        brokers = data.get("brokers") or {}
        data["providers"] = {
            "futu": brokers.get("futu", {}),
            "ibkr_tws": brokers.get("ibkr_tws", {}),
            "ibkr_cp": brokers.get("ibkr_cp", {}),
        }

    flat_data = data.get("data")
    if isinstance(flat_data, dict) and any(
        key in flat_data for key in ("timeframe", "adjustment", "max_missing_ratio")
    ):
        data["data"] = {
            "history_bars": {
                "timeframe": flat_data.get("timeframe", "1D"),
                "adjustment": flat_data.get("adjustment", "forward"),
                "include_turnover": True,
            },
            "quotes": {
                "enabled": bool(flat_data.get("quotes_enabled", False)),
            },
            "quality": {
                "max_missing_ratio": flat_data.get("max_missing_ratio", 0.1),
                "fail_on_empty_primary": False,
                "require_min_success_symbols": 1,
            },
        }

    return data


def load_config(path: str | Path) -> AppConfig:
    """Load and validate pipeline config from YAML.

    Raises:
        FileNotFoundError: If the config path does not exist.
        ValueError: If schema validation fails.
    """

    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config not found: {config_path}. "
            "Please create it from config.example.yaml."
        )

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    raw = _upgrade_legacy_config(raw)
    try:
        return AppConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"Invalid config file: {config_path}\n{exc}") from exc


def apply_cli_overrides(
    config: AppConfig,
    *,
    watchlist_path: str | None = None,
    rules_path: str | None = None,
    universe_mode: str | None = None,
    filter_spec_path: str | None = None,
    filter_market: str | None = None,
    filter_plate_code: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    out_dir: str | None = None,
) -> AppConfig:
    """Apply CLI overrides and return a validated config object."""

    data = config.model_dump(mode="python")

    if watchlist_path:
        data.setdefault("universe", {})["watchlist_path"] = watchlist_path
    if rules_path:
        data.setdefault("universe", {})["rules_path"] = rules_path
    if universe_mode:
        data.setdefault("universe", {})["mode"] = universe_mode
    if filter_spec_path:
        data.setdefault("universe", {})["filter_spec_path"] = filter_spec_path
    if filter_market:
        data.setdefault("universe", {})["filter_market"] = filter_market
    if filter_plate_code is not None:
        data.setdefault("universe", {})["filter_plate_code"] = filter_plate_code
    if start_date:
        data.setdefault("run", {})["start_date"] = start_date
    if end_date:
        data.setdefault("run", {})["end_date"] = end_date
    if out_dir:
        data.setdefault("run", {})["out_dir"] = out_dir

    return AppConfig.model_validate(data)
