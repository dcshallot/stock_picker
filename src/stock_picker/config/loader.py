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
    try:
        return AppConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"Invalid config file: {config_path}\n{exc}") from exc


def apply_cli_overrides(
    config: AppConfig,
    *,
    watchlist_path: str | None = None,
    rules_path: str | None = None,
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
    if start_date:
        data.setdefault("run", {})["start_date"] = start_date
    if end_date:
        data.setdefault("run", {})["end_date"] = end_date
    if out_dir:
        data.setdefault("run", {})["out_dir"] = out_dir

    return AppConfig.model_validate(data)
