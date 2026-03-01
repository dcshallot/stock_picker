"""Futu connector stub implementation.

This adapter now exposes a stricter request boundary so later replacement with
real OpenD calls is mostly localized to this module.
"""

from __future__ import annotations

import importlib
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from stock_picker.brokers.base import (
    BrokerCapabilities,
    BrokerConnector,
    BrokerDataRequest,
    PermissionDenied,
    RateLimited,
)


class FutuConnector(BrokerConnector):
    """Stub connector for Futu OpenD.

    Future implementation point:
    - replace generated demo data with real OpenD calls
    - apply permission checks by market
    - respect timeframe/adjustment options
    """

    name = "futu"

    def _import_futu(self) -> Any | None:
        try:
            return importlib.import_module("futu")
        except Exception as exc:  # noqa: BLE001
            self.last_fetch_notes["sdk_import_error"] = str(exc)
            return None

    def _can_use_live_sdk(self) -> bool:
        return self._import_futu() is not None

    def _validate_config(self) -> None:
        host = str(self.config.get("host", "")).strip()
        port = self.config.get("port")
        if not host:
            raise ValueError("Futu host is required.")
        if not isinstance(port, int) or port <= 0:
            raise ValueError("Futu port must be a positive integer.")

    def _request_frame(self, request: BrokerDataRequest) -> pd.DataFrame:
        return pd.DataFrame(request.universe)

    def _to_futu_code(self, symbol: str, market: str) -> str:
        raw_symbol = str(symbol).strip()
        market = str(market).strip().upper()

        if not raw_symbol:
            return raw_symbol

        if raw_symbol.startswith(f"{market}."):
            return raw_symbol

        if "." not in raw_symbol:
            if market == "HK" and raw_symbol.isdigit():
                return f"HK.{raw_symbol.zfill(5)}"
            return f"{market}.{raw_symbol}"

        left, right = raw_symbol.split(".", 1)
        left = left.strip()
        right = right.strip().upper()

        if market == "HK":
            code = left if left.isdigit() else right
            return f"HK.{code.zfill(5)}"

        if market == "CN":
            exchange_map = {"SS": "SH", "SH": "SH", "SZ": "SZ"}
            exchange = exchange_map.get(right, right)
            return f"{exchange}.{left}"

        if market in {"US", "EU"}:
            return f"{market if market == 'US' else right}.{left}"

        return f"{market}.{left}"

    def build_cache_key(self, dataset: str, request: BrokerDataRequest) -> str:
        symbols = request.symbol_list()
        market_set = sorted({str(item.get("market", "")).upper() for item in request.universe})
        market_part = "-".join([m for m in market_set if m]) or "UNKNOWN"
        start = request.start_date.isoformat() if request.start_date else "na"
        end = request.end_date.isoformat() if request.end_date else "na"
        return (
            f"{dataset}__futu__{market_part}__{request.timeframe}"
            f"__{request.adjustment}__{start}__{end}__n{len(symbols)}"
        )

    def capabilities_check(self) -> BrokerCapabilities:
        try:
            self._validate_config()
        except ValueError as exc:
            return BrokerCapabilities(
                broker_name=self.name,
                supports_bars=True,
                supports_quotes=True,
                delayed_data=False,
                permission_ok=False,
                notes=["stub_connector", f"invalid_config:{exc}"],
            )

        notes = ["stub_connector"]
        if self._can_use_live_sdk():
            notes.append("live_sdk_available")
        else:
            notes.append("live_sdk_unavailable_fallback_stub")
            sdk_import_error = self.last_fetch_notes.get("sdk_import_error")
            if sdk_import_error:
                notes.append(f"sdk_import_error:{sdk_import_error}")

        return BrokerCapabilities(
            broker_name=self.name,
            supports_bars=True,
            supports_quotes=True,
            delayed_data=False,
            permission_ok=True,
            notes=notes,
        )

    def resolve_instruments(self, universe_df: pd.DataFrame) -> dict[str, Any]:
        self._validate_config()
        mapping: dict[str, Any] = {}
        for _, row in universe_df.iterrows():
            symbol = str(row["symbol"])
            market = str(row.get("market", "UNKNOWN")).upper()
            broker_symbol = self._to_futu_code(symbol, market)
            mapping[symbol] = {"broker_symbol": broker_symbol, "market": market}
        return mapping

    def _map_futu_error(self, message: str) -> Exception:
        text = str(message).lower()
        if "permission" in text or "权限" in text:
            return PermissionDenied(str(message))
        if "rate" in text or "频率" in text or "too many" in text:
            return RateLimited(str(message))
        return RuntimeError(str(message))

    def _map_ktype(self, futu_module: Any, timeframe: str) -> Any:
        timeframe_map = {
            "1D": "K_DAY",
            "1W": "K_WEEK",
            "1M": "K_MON",
            "60M": "K_60M",
            "30M": "K_30M",
            "15M": "K_15M",
            "5M": "K_5M",
            "1M_BAR": "K_1M",
        }
        key = timeframe_map.get(str(timeframe).upper(), "K_DAY")
        return getattr(getattr(futu_module, "KLType"), key)

    def _map_autype(self, futu_module: Any, adjustment: str) -> Any:
        adjustment_map = {
            "forward": "QFQ",
            "backward": "HFQ",
            "none": "NONE",
        }
        key = adjustment_map.get(str(adjustment).lower(), "QFQ")
        return getattr(getattr(futu_module, "AuType"), key)

    @staticmethod
    def _normalize_history_kline_quota_data(data: Any) -> dict[str, Any]:
        """Normalize Futu quota payload to a stable dict shape.

        Official Python docs describe the successful return payload as a tuple:
        `(used_quota, remain_quota, *detail_items)`.
        """

        used_quota = 0
        remain_quota = 0
        detail_list: list[dict[str, Any]] = []

        if isinstance(data, dict):
            used_quota = int(data.get("used_quota", 0) or 0)
            remain_quota = int(data.get("remain_quota", 0) or 0)
            raw_detail = data.get("detail_list", [])
            if isinstance(raw_detail, list):
                detail_list = [item for item in raw_detail if isinstance(item, dict)]
        elif isinstance(data, (tuple, list)):
            if len(data) >= 1:
                used_quota = int(data[0] or 0)
            if len(data) >= 2:
                remain_quota = int(data[1] or 0)
            if len(data) >= 3:
                if len(data) == 3 and isinstance(data[2], list):
                    detail_list = [item for item in data[2] if isinstance(item, dict)]
                else:
                    detail_list = [item for item in data[2:] if isinstance(item, dict)]

        return {
            "used_quota": used_quota,
            "remain_quota": remain_quota,
            "detail_count": len(detail_list),
            "detail_list": detail_list,
            "used_ratio": (used_quota / (used_quota + remain_quota))
            if (used_quota + remain_quota) > 0
            else 0.0,
        }

    def get_history_kline_quota(self, get_detail: bool = True) -> dict[str, Any]:
        """Fetch history-kline quota usage from OpenD.

        This preflight diagnostic does not raise on provider/runtime failures.
        Instead it returns a structured status block for diagnostics.json.
        """

        self._validate_config()
        futu_module = self._import_futu()
        if futu_module is None:
            result = {
                "status": "skipped",
                "message": "futu SDK is unavailable in the current runtime",
            }
            self.last_fetch_notes["history_kline_quota"] = result
            return result

        quote_ctx = futu_module.OpenQuoteContext(
            host=self.config["host"],
            port=self.config["port"],
        )
        try:
            ret, data = quote_ctx.get_history_kl_quota(get_detail=get_detail)
            if ret != getattr(futu_module, "RET_OK"):
                result = {
                    "status": "error",
                    "message": str(data),
                    "error_type": self._map_futu_error(data).__class__.__name__,
                }
                self.last_fetch_notes["history_kline_quota"] = result
                return result

            normalized = self._normalize_history_kline_quota_data(data)
            result = {
                "status": "ok",
                "get_detail": bool(get_detail),
                **normalized,
            }
            self.last_fetch_notes["history_kline_quota"] = result
            return result
        except Exception as exc:  # noqa: BLE001
            result = {
                "status": "error",
                "message": str(exc),
                "error_type": exc.__class__.__name__,
            }
            self.last_fetch_notes["history_kline_quota"] = result
            return result
        finally:
            quote_ctx.close()

    def _fetch_bars_live(self, request: BrokerDataRequest) -> pd.DataFrame:
        futu_module = self._import_futu()
        if futu_module is None:
            return self._fetch_bars_stub(request)

        self.last_fetch_notes["bars"] = {"ok": [], "errors": []}
        quote_ctx = futu_module.OpenQuoteContext(
            host=self.config["host"],
            port=self.config["port"],
        )
        try:
            rows: list[dict[str, Any]] = []
            ktype = self._map_ktype(futu_module, request.timeframe)
            autype = self._map_autype(futu_module, request.adjustment)
            start = request.start_date.isoformat() if request.start_date else None
            end = request.end_date.isoformat() if request.end_date else None

            for item in request.universe:
                symbol = str(item.get("symbol", ""))
                mapping = request.mapping.get(symbol, {})
                code = str(mapping.get("broker_symbol") or "")
                if not code:
                    self.last_fetch_notes["bars"]["errors"].append(
                        {"symbol": symbol, "code": code, "error": "missing_broker_symbol"}
                    )
                    continue

                try:
                    result = quote_ctx.request_history_kline(
                        code=code,
                        start=start,
                        end=end,
                        ktype=ktype,
                        autype=autype,
                        max_count=1000,
                    )
                    if not isinstance(result, tuple) or len(result) < 2:
                        raise RuntimeError(f"Unexpected Futu response for {code}: {result!r}")

                    ret = result[0]
                    data = result[1]
                    if ret != getattr(futu_module, "RET_OK"):
                        raise self._map_futu_error(data)
                    if data is None or data.empty:
                        self.last_fetch_notes["bars"]["errors"].append(
                            {"symbol": symbol, "code": code, "error": "no_data"}
                        )
                        continue
                except Exception as exc:  # noqa: BLE001
                    self.last_fetch_notes["bars"]["errors"].append(
                        {
                            "symbol": symbol,
                            "code": code,
                            "error": str(exc),
                            "error_type": exc.__class__.__name__,
                        }
                    )
                    continue

                for _, row in data.iterrows():
                    rows.append(
                        {
                            "timestamp": row.get("time_key"),
                            "symbol": symbol,
                            "market": item.get("market", "UNKNOWN"),
                            "currency": item.get("currency", "USD"),
                            "timeframe": request.timeframe or "1D",
                            "open": row.get("open"),
                            "high": row.get("high"),
                            "low": row.get("low"),
                            "close": row.get("close"),
                            "volume": row.get("volume"),
                            "turnover": row.get("turnover"),
                            "source": self.name,
                            "live_mode": True,
                            "cache_key": self.build_cache_key("bars", request),
                        }
                    )
                self.last_fetch_notes["bars"]["ok"].append(
                    {"symbol": symbol, "code": code, "rows": int(len(data))}
                )

            return pd.DataFrame(rows)
        finally:
            quote_ctx.close()

    def _fetch_bars_stub(self, request: BrokerDataRequest) -> pd.DataFrame:
        self.last_fetch_notes["bars"] = {"ok": [], "errors": []}
        universe_df = self._request_frame(request)
        rows: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        timeframe = request.timeframe or "1D"

        for i, row in universe_df.reset_index(drop=True).iterrows():
            base = 100 + i * 5
            for d in range(5):
                ts = now - timedelta(days=5 - d)
                rows.append(
                    {
                        "timestamp": ts.isoformat(),
                        "symbol": row["symbol"],
                        "market": row.get("market", "UNKNOWN"),
                        "currency": row.get("currency", "USD"),
                        "timeframe": timeframe,
                        "open": base + d,
                        "high": base + d + 1.5,
                        "low": base + d - 1.5,
                        "close": base + d + 0.7,
                        "volume": 100000 + (d * 5000),
                        "turnover": (base + d + 0.7) * (100000 + (d * 5000)),
                        "source": self.name,
                        "live_mode": False,
                        "cache_key": self.build_cache_key("bars", request),
                    }
                )
            self.last_fetch_notes["bars"]["ok"].append(
                {"symbol": row["symbol"], "code": row["symbol"], "rows": 5}
            )

        return pd.DataFrame(rows)

    def _fetch_quotes_live(self, request: BrokerDataRequest) -> pd.DataFrame:
        futu_module = self._import_futu()
        if futu_module is None:
            return self._fetch_quotes_stub(request)

        self.last_fetch_notes["quotes"] = {"ok": [], "errors": []}
        codes = []
        item_by_symbol = {str(item.get("symbol", "")): item for item in request.universe}
        for symbol in request.symbol_list():
            code = str(request.mapping.get(symbol, {}).get("broker_symbol") or "")
            if code:
                codes.append(code)

        if not codes:
            return pd.DataFrame()

        quote_ctx = futu_module.OpenQuoteContext(
            host=self.config["host"],
            port=self.config["port"],
        )
        try:
            ret, data = quote_ctx.get_stock_quote(codes)
            if ret != getattr(futu_module, "RET_OK"):
                raise self._map_futu_error(data)
            if data is None or data.empty:
                return pd.DataFrame()

            rows: list[dict[str, Any]] = []
            for _, row in data.iterrows():
                code = str(row.get("code", ""))
                if "." in code:
                    _, symbol = code.split(".", 1)
                else:
                    symbol = code
                item = item_by_symbol.get(symbol, {})
                rows.append(
                    {
                        "timestamp": row.get("update_time") or datetime.now(timezone.utc).isoformat(),
                        "symbol": symbol,
                        "market": item.get("market", "UNKNOWN"),
                        "currency": item.get("currency", "USD"),
                        "bid": row.get("bid_price"),
                        "ask": row.get("ask_price"),
                        "last": row.get("last_price"),
                        "source": self.name,
                        "live_mode": True,
                        "cache_key": self.build_cache_key("quotes", request),
                    }
                )
                self.last_fetch_notes["quotes"]["ok"].append({"symbol": symbol, "code": code})

            return pd.DataFrame(rows)
        except Exception as exc:  # noqa: BLE001
            for code in codes:
                symbol = code.split(".", 1)[1] if "." in code else code
                self.last_fetch_notes["quotes"]["errors"].append(
                    {
                        "symbol": symbol,
                        "code": code,
                        "error": str(exc),
                        "error_type": exc.__class__.__name__,
                    }
                )
            raise
        finally:
            quote_ctx.close()

    def fetch_bars(self, request: BrokerDataRequest) -> pd.DataFrame:
        self._validate_config()
        return self._fetch_bars_live(request)

    def fetch_quotes(self, request: BrokerDataRequest) -> pd.DataFrame:
        self._validate_config()
        return self._fetch_quotes_live(request)

    def _fetch_quotes_stub(self, request: BrokerDataRequest) -> pd.DataFrame:
        self.last_fetch_notes["quotes"] = {"ok": [], "errors": []}
        universe_df = self._request_frame(request)
        rows: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc).isoformat()

        for i, row in universe_df.reset_index(drop=True).iterrows():
            px = 100 + i * 5
            rows.append(
                {
                    "timestamp": now,
                    "symbol": row["symbol"],
                    "market": row.get("market", "UNKNOWN"),
                    "currency": row.get("currency", "USD"),
                    "bid": px - 0.1,
                    "ask": px + 0.1,
                    "last": px,
                    "source": self.name,
                    "live_mode": False,
                    "cache_key": self.build_cache_key("quotes", request),
                }
            )
            self.last_fetch_notes["quotes"]["ok"].append(
                {"symbol": row["symbol"], "code": row["symbol"]}
            )

        return pd.DataFrame(rows)
