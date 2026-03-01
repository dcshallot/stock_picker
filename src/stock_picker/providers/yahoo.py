"""Yahoo Finance provider for daily historical bars."""

from __future__ import annotations

from datetime import timedelta, timezone
from typing import Any

import pandas as pd

from stock_picker.providers.base import (
    HistoricalBarsProvider,
    HistoricalBarsRequest,
    HistoricalBarsResult,
    ProviderCapabilities,
)


class YahooHistoricalBarsProvider(HistoricalBarsProvider):
    """Historical daily-bar provider backed by yfinance when available."""

    name = "yahoo"

    def _import_yfinance(self) -> Any | None:
        try:
            import yfinance as yf  # type: ignore

            return yf
        except Exception as exc:  # noqa: BLE001
            self.last_fetch_notes["yfinance_import_error"] = str(exc)
            return None

    def capabilities_check(self) -> ProviderCapabilities:
        yf = self._import_yfinance()
        notes: list[str] = []
        available = bool(self.config.get("enabled", True))
        if yf is None:
            notes.append("yfinance_unavailable_fallback_stub")
            error = self.last_fetch_notes.get("yfinance_import_error")
            if error:
                notes.append(f"import_error:{error}")
        else:
            notes.append("yfinance_available")
        return ProviderCapabilities(
            provider_name=self.name,
            supports_history_bars=True,
            available=available,
            requires_network=True,
            notes=notes,
        )

    def resolve_symbol(self, symbol: str, market: str, currency: str) -> str:
        raw_symbol = str(symbol).strip().upper()
        market = str(market).strip().upper()

        if market == "US":
            if raw_symbol.startswith("US."):
                return raw_symbol.split(".", 1)[1]
            return raw_symbol

        if market == "HK":
            digits = "".join(ch for ch in raw_symbol if ch.isdigit())
            if digits:
                return f"{digits.zfill(4)}.HK"
            if raw_symbol.endswith(".HK"):
                return raw_symbol
            return f"{raw_symbol}.HK"

        if market == "CN":
            left = raw_symbol
            right = ""
            if "." in raw_symbol:
                left, right = raw_symbol.split(".", 1)
                right = right.upper()
            if right in {"SS", "SH"}:
                return f"{left}.SS"
            if right == "SZ":
                return f"{left}.SZ"
            if left.startswith("6"):
                return f"{left}.SS"
            if left.startswith(("0", "3")):
                return f"{left}.SZ"
            return raw_symbol

        return raw_symbol

    def _build_stub_rows(self, request: HistoricalBarsRequest) -> pd.DataFrame:
        dates = pd.bdate_range(request.start_date, request.end_date, inclusive="both")
        rows: list[dict[str, Any]] = []
        base = sum(ord(char) for char in request.symbol) % 50 + 80
        for offset, ts in enumerate(dates):
            price = base + offset
            rows.append(
                {
                    "timestamp": ts.tz_localize(timezone.utc).isoformat(),
                    "symbol": request.symbol,
                    "market": request.market,
                    "currency": request.currency,
                    "timeframe": request.timeframe,
                    "adjustment": request.adjustment,
                    "open": price,
                    "high": price + 1.2,
                    "low": price - 1.0,
                    "close": price + 0.6,
                    "volume": 120000 + (offset * 1000),
                    "turnover": None,
                    "source": self.name,
                    "live_mode": False,
                }
            )
        return pd.DataFrame(rows)

    def _download_live_rows(self, request: HistoricalBarsRequest, provider_symbol: str) -> pd.DataFrame:
        yf = self._import_yfinance()
        if yf is None:
            stub = self._build_stub_rows(request)
            self.last_fetch_notes["bars"] = {
                "ok": [
                    {
                        "symbol": request.symbol,
                        "code": provider_symbol,
                        "rows": int(len(stub)),
                        "mode": "stub",
                    }
                ],
                "errors": [],
            }
            return stub

        auto_adjust = str(request.adjustment).lower() == "forward"
        end_exclusive = request.end_date + timedelta(days=1)
        timeout = int(self.config.get("timeout_seconds", 20) or 20)
        self.last_fetch_notes["bars"] = {"ok": [], "errors": []}

        history = yf.download(
            tickers=provider_symbol,
            start=request.start_date.isoformat(),
            end=end_exclusive.isoformat(),
            interval="1d",
            auto_adjust=auto_adjust,
            progress=False,
            threads=False,
            timeout=timeout,
        )

        if history is None or history.empty:
            self.last_fetch_notes["bars"]["errors"].append(
                {"symbol": request.symbol, "code": provider_symbol, "error": "empty_response"}
            )
            return pd.DataFrame()

        if isinstance(history.columns, pd.MultiIndex):
            history = history.droplevel(-1, axis=1)

        rows: list[dict[str, Any]] = []
        for ts, row in history.iterrows():
            stamp = pd.Timestamp(ts)
            if stamp.tzinfo is None:
                stamp = stamp.tz_localize(timezone.utc)
            else:
                stamp = stamp.tz_convert(timezone.utc)
            rows.append(
                {
                    "timestamp": stamp.isoformat(),
                    "symbol": request.symbol,
                    "market": request.market,
                    "currency": request.currency,
                    "timeframe": request.timeframe,
                    "adjustment": request.adjustment,
                    "open": row.get("Open"),
                    "high": row.get("High"),
                    "low": row.get("Low"),
                    "close": row.get("Close"),
                    "volume": row.get("Volume"),
                    "turnover": None,
                    "source": self.name,
                    "live_mode": True,
                }
            )

        self.last_fetch_notes["bars"]["ok"].append(
            {"symbol": request.symbol, "code": provider_symbol, "rows": int(len(rows))}
        )
        return pd.DataFrame(rows)

    def fetch_daily_bars(self, request: HistoricalBarsRequest) -> HistoricalBarsResult:
        provider_symbol = self.resolve_symbol(
            request.symbol,
            request.market,
            request.currency,
        )
        rows = self._download_live_rows(request, provider_symbol)
        if rows is None:
            rows = pd.DataFrame()
        if rows.empty and "bars" not in self.last_fetch_notes:
            self.last_fetch_notes["bars"] = {
                "ok": [],
                "errors": [{"symbol": request.symbol, "code": provider_symbol, "error": "empty_response"}],
            }
        return HistoricalBarsResult(
            rows=rows,
            provider=self.name,
            partial=False,
            coverage_start=request.start_date,
            coverage_end=request.end_date,
            notes=self.last_fetch_notes.get("bars", {}),
        )
