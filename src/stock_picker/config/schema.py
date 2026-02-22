"""Configuration schema definitions.

This module defines typed configuration models for the pipeline.
All models are intentionally explicit so future broker/model expansion
can be validated early with clear errors.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class RunConfig(BaseModel):
    """Runtime window and artifact locations."""

    start_date: date | None = None
    end_date: date | None = None
    timezone: str = "UTC"
    out_dir: str = "outputs"
    cache_dir: str = "data/cache"


class FutuConfig(BaseModel):
    """Futu OpenD connection options (stub)."""

    host: str = "127.0.0.1"
    port: int = 11111
    unlock_trade_password: str = ""


class IbkrTwsConfig(BaseModel):
    """IBKR TWS/Gateway connection options (stub)."""

    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 101
    read_only: bool = True


class IbkrCpConfig(BaseModel):
    """IBKR Client Portal API options (stub)."""

    base_url: str = "https://api.ibkr.com/v1/api"
    account_id: str = ""
    api_key: str = ""


class BrokersConfig(BaseModel):
    """All supported broker configs."""

    futu: FutuConfig = Field(default_factory=FutuConfig)
    ibkr_tws: IbkrTwsConfig = Field(default_factory=IbkrTwsConfig)
    ibkr_cp: IbkrCpConfig = Field(default_factory=IbkrCpConfig)


class UniverseConfig(BaseModel):
    """Universe source settings.

    watchlist_path and rules_path may coexist. The pipeline gives watchlist
    higher priority when both are provided.
    """

    watchlist_path: str | None = None
    rules_path: str | None = None
    prefer_watchlist: bool = True


class DataConfig(BaseModel):
    """Data retrieval and quality constraints."""

    timeframe: str = "1D"
    adjustment: str = "forward"
    max_missing_ratio: float = 0.1
    rate_limit: dict[str, Any] = Field(
        default_factory=lambda: {
            "requests_per_second": 5,
            "backoff_seconds": [1, 2, 5],
        }
    )


class ProphetConfig(BaseModel):
    """Optional Prophet model configuration."""

    enable: bool = False
    params: dict[str, Any] = Field(default_factory=dict)


class ModelsConfig(BaseModel):
    """Container for model configs."""

    prophet: ProphetConfig = Field(default_factory=ProphetConfig)


class SelectionConfig(BaseModel):
    """Hard filters, scoring weights, and quota constraints."""

    hard_filters: dict[str, Any] = Field(default_factory=dict)
    score_weights: dict[str, float] = Field(default_factory=dict)
    constraints: dict[str, Any] = Field(default_factory=dict)


class AppConfig(BaseModel):
    """Top-level application configuration."""

    model_config = ConfigDict(extra="allow")

    run: RunConfig = Field(default_factory=RunConfig)
    brokers: BrokersConfig = Field(default_factory=BrokersConfig)
    universe: UniverseConfig = Field(default_factory=UniverseConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    models: ModelsConfig = Field(default_factory=ModelsConfig)
    selection: SelectionConfig = Field(default_factory=SelectionConfig)
