"""Symbol mapping placeholders.

This module keeps broker-specific contract mapping logic out of strategy code.
"""

from __future__ import annotations

import pandas as pd


def map_symbols_for_broker(universe_df: pd.DataFrame, broker_name: str) -> pd.DataFrame:
    """Return symbol mapping dataframe for a broker.

    For IBKR variants we expose a contract_hint placeholder. Future versions
    should replace this with true contract resolution.
    """

    if universe_df.empty:
        return pd.DataFrame(columns=["symbol", "broker_symbol", "contract_hint"])

    out = universe_df[["symbol", "market", "currency"]].copy()
    out["broker_symbol"] = out["symbol"]

    if broker_name.startswith("ibkr"):
        out["contract_hint"] = out["market"].astype(str) + ":" + out["symbol"].astype(str)
    else:
        out["contract_hint"] = ""

    return out[["symbol", "broker_symbol", "contract_hint"]]
