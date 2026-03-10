"""Configuration schema definitions.

This module defines typed configuration models for the pipeline.
The schema now separates:
- provider connection/configuration details
- market/data routing rules
- data-type specific runtime options
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
    mode: str = "pipeline"


class ProviderBaseConfig(BaseModel):
    """Shared provider metadata and entitlement hints."""

    kind: str = "generic"
    enabled: bool = False
    allowed_markets: list[str] = Field(default_factory=list)
    datasets: list[str] = Field(default_factory=list)


class FutuHistoryKlineConfig(BaseModel):
    """Futu history-kline specific controls."""

    timeframe_allowlist: list[str] = Field(default_factory=lambda: ["1D"])
    adjustment: str = "forward"
    max_count_per_request: int = 1000
    quota_budget_30d: int = 1000
    warn_remaining_below: int = 100
    check_quota_before_run: bool = True


class FutuConfig(ProviderBaseConfig):
    """Futu OpenD connection options and current market scope."""

    kind: str = "futu"
    enabled: bool = True
    allowed_markets: list[str] = Field(default_factory=lambda: ["HK"])
    datasets: list[str] = Field(default_factory=lambda: ["history_bars"])
    enable_quotes: bool = False
    host: str = "127.0.0.1"
    port: int = 11112
    websocket_port: int | None = None
    websocket_key: str = ""
    unlock_trade_password: str = ""
    history_kline: FutuHistoryKlineConfig = Field(default_factory=FutuHistoryKlineConfig)


class IbkrTwsConfig(ProviderBaseConfig):
    """IBKR TWS/Gateway connection options (stub)."""

    kind: str = "ibkr_tws"
    enabled: bool = False
    datasets: list[str] = Field(default_factory=lambda: ["history_bars", "quotes"])
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 101
    read_only: bool = True


class IbkrCpConfig(ProviderBaseConfig):
    """IBKR Client Portal API options (stub)."""

    kind: str = "ibkr_cp"
    enabled: bool = False
    datasets: list[str] = Field(default_factory=lambda: ["quotes"])
    base_url: str = "https://api.ibkr.com/v1/api"
    account_id: str = ""
    api_key: str = ""


class GenericApiProviderConfig(ProviderBaseConfig):
    """Placeholder configuration for future non-broker data providers."""

    base_url: str = ""
    api_key: str = ""
    root_dir: str = ""
    rate_limit: dict[str, Any] = Field(default_factory=dict)


class YahooConfig(ProviderBaseConfig):
    """Yahoo Finance historical-bars provider options."""

    kind: str = "yahoo"
    enabled: bool = True
    allowed_markets: list[str] = Field(default_factory=lambda: ["US", "HK", "CN", "EU"])
    datasets: list[str] = Field(default_factory=lambda: ["history_bars"])
    timeout_seconds: int = 20
    max_retries: int = 3
    repair_window_days: int = 30
    batch_size: int = 1


class ProvidersConfig(BaseModel):
    """All current and planned data providers."""

    futu: FutuConfig = Field(default_factory=FutuConfig)
    yahoo: YahooConfig = Field(default_factory=YahooConfig)
    ibkr_tws: IbkrTwsConfig = Field(default_factory=IbkrTwsConfig)
    ibkr_cp: IbkrCpConfig = Field(default_factory=IbkrCpConfig)
    us_primary: GenericApiProviderConfig = Field(
        default_factory=lambda: GenericApiProviderConfig(
            kind="rest_api",
            enabled=False,
            allowed_markets=["US"],
            datasets=["history_bars", "quotes", "fundamentals"],
            base_url="https://api.example.com/us",
            rate_limit={"requests_per_minute": 60, "backoff_seconds": [1, 2, 5]},
        )
    )
    us_backup: GenericApiProviderConfig = Field(
        default_factory=lambda: GenericApiProviderConfig(
            kind="file_or_rest",
            enabled=False,
            allowed_markets=["US"],
            datasets=["history_bars"],
        )
    )
    cn_primary: GenericApiProviderConfig = Field(
        default_factory=lambda: GenericApiProviderConfig(
            kind="rest_api",
            enabled=False,
            allowed_markets=["CN"],
            datasets=["history_bars", "quotes"],
            base_url="https://api.example.com/cn",
        )
    )
    cn_backup: GenericApiProviderConfig = Field(
        default_factory=lambda: GenericApiProviderConfig(
            kind="csv_or_parquet",
            enabled=False,
            allowed_markets=["CN"],
            datasets=["history_bars"],
            root_dir="data/input/cn_cache",
        )
    )
    eu_primary: GenericApiProviderConfig = Field(
        default_factory=lambda: GenericApiProviderConfig(
            kind="rest_api",
            enabled=False,
            allowed_markets=["EU"],
            datasets=["history_bars"],
            base_url="https://api.example.com/eu",
        )
    )


class RoutingConfig(BaseModel):
    """Market -> provider ordering per dataset family."""

    history_bars: dict[str, list[str]] = Field(
        default_factory=lambda: {
            "HK": ["futu", "yahoo"],
            "US": ["yahoo"],
            "CN": ["yahoo"],
            "EU": ["yahoo"],
        }
    )
    quotes: dict[str, list[str]] = Field(
        default_factory=lambda: {
            "HK": [],
            "US": ["us_primary"],
            "CN": ["cn_primary"],
            "EU": ["eu_primary"],
        }
    )
    fundamentals: dict[str, list[str]] = Field(
        default_factory=lambda: {
            "HK": [],
            "US": ["us_primary"],
            "CN": ["cn_primary"],
            "EU": [],
        }
    )


class UniverseConfig(BaseModel):
    """Universe source settings.

    watchlist_path and rules_path may coexist. The pipeline gives watchlist
    higher priority when both are provided.
    """

    mode: str = "futu_filter"
    watchlist_path: str | None = None
    rules_path: str | None = None
    filter_spec_path: str = "data/input/futu_filter_spec.json"
    filter_market: str = "HK"
    filter_plate_code: str | None = None
    filter_page_size: int = 200
    max_filter_pages: int = 200
    prefer_watchlist: bool = True


class HistoryBarsDataConfig(BaseModel):
    """History-bars runtime controls."""

    timeframe: str = "1D"
    adjustment: str = "forward"
    include_turnover: bool = True
    store_dir: str = "data/history_store"
    bootstrap_start_date: date = date(2018, 1, 1)
    repair_window_days: int = 30
    max_gap_days_before_full_resync: int = 365


class QuotesDataConfig(BaseModel):
    """Quote runtime controls."""

    enabled: bool = False


class QualityPolicyConfig(BaseModel):
    """Generic quality and validation policies."""

    max_missing_ratio: float = 0.1
    fail_on_empty_primary: bool = False
    require_min_success_symbols: int = 1


class DataConfig(BaseModel):
    """Data retrieval and quality constraints by dataset family."""

    history_bars: HistoryBarsDataConfig = Field(default_factory=HistoryBarsDataConfig)
    quotes: QuotesDataConfig = Field(default_factory=QuotesDataConfig)
    quality: QualityPolicyConfig = Field(default_factory=QualityPolicyConfig)


class ProphetConfig(BaseModel):
    """Optional Prophet model configuration."""

    enable: bool = False
    params: dict[str, Any] = Field(default_factory=dict)


class ModelsConfig(BaseModel):
    """Container for model configs."""

    prophet: ProphetConfig = Field(default_factory=ProphetConfig)


class SelectionConfig(BaseModel):
    """Hard filters, scoring weights, and quota constraints."""

    enabled: bool = True
    hard_filters: dict[str, Any] = Field(default_factory=dict)
    score_weights: dict[str, float] = Field(default_factory=dict)
    constraints: dict[str, Any] = Field(default_factory=dict)


class AppConfig(BaseModel):
    """Top-level application configuration."""

    model_config = ConfigDict(extra="allow")

    run: RunConfig = Field(default_factory=RunConfig)
    providers: ProvidersConfig = Field(default_factory=ProvidersConfig)
    routing: RoutingConfig = Field(default_factory=RoutingConfig)
    universe: UniverseConfig = Field(default_factory=UniverseConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    models: ModelsConfig = Field(default_factory=ModelsConfig)
    selection: SelectionConfig = Field(default_factory=SelectionConfig)

    def get_provider_config(self, name: str) -> ProviderBaseConfig | None:
        """Return provider config by name if defined."""

        return getattr(self.providers, name, None)
