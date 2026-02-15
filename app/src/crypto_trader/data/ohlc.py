from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional

import pandas as pd
from coingecko_sdk import APIError, NotFoundError, RateLimitError


@dataclass(frozen=True)
class OhlcQuery:
    coin_id: str
    vs_currency: str = "usd"
    days: int = 1
    interval: Optional[str] = None  # e.g. "hourly" or "daily" if your plan supports it


def fetch_ohlc_rows(client: Any, q: OhlcQuery) -> List[List[float]]:
    """
    Calls CoinGecko /coins/{id}/ohlc via coingecko_sdk and returns raw rows:
      [[timestamp_ms, open, high, low, close], ...]
    """
    try:
        # Most common SDK shape is: client.coins.ohlc.get(...)
        if hasattr(client, "coins") and hasattr(client.coins, "ohlc") and hasattr(client.coins.ohlc, "get"):
            params = {"id": q.coin_id, "vs_currency": q.vs_currency, "days": q.days}
            if q.interval:
                params["interval"] = q.interval
            payload = client.coins.ohlc.get(**params)

        # Alternate SDK shape: client.coins.get_ohlc(...)
        elif hasattr(client, "coins") and hasattr(client.coins, "get_ohlc"):
            params = {"id": q.coin_id, "vs_currency": q.vs_currency, "days": q.days}
            if q.interval:
                params["interval"] = q.interval
            payload = client.coins.get_ohlc(**params)

        else:
            raise AttributeError(
                "Could not locate an OHLC method on the coingecko_sdk client. "
                "Inspect your client for something like client.coins.ohlc.get(...)"
            )

        # SDK may return list-of-lists directly, or a Pydantic model
        if hasattr(payload, "model_dump"):
            payload = payload.model_dump()

        # Some SDK responses wrap data; support both styles
        if isinstance(payload, dict) and "data" in payload:
            payload = payload["data"]

        return payload or []

    except RateLimitError:
        # SDK is already configured with max_retries per your rules
        raise
    except NotFoundError:
        raise
    except APIError:
        raise


def ohlc_rows_to_df(rows: List[List[float]], q: OhlcQuery) -> pd.DataFrame:
    """
    Converts raw OHLC rows -> canonical DataFrame.

    CoinGecko OHLC rows are:
      [timestamp_ms, open, high, low, close] :contentReference[oaicite:2]{index=2}
    """
    if not rows:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "coin_id",
                "vs_currency",
                "days",
                "interval",
            ]
        )

    df = pd.DataFrame(rows, columns=["timestamp_ms", "open", "high", "low", "close"])
    df["timestamp"] = pd.to_datetime(df["timestamp_ms"].astype("int64"), unit="ms", utc=True)
    df = df.drop(columns=["timestamp_ms"])

    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["coin_id"] = q.coin_id
    df["vs_currency"] = q.vs_currency
    df["days"] = q.days
    df["interval"] = q.interval

    return df[
        ["timestamp", "open", "high", "low", "close", "coin_id", "vs_currency", "days", "interval"]
    ].sort_values(["coin_id", "timestamp"]).reset_index(drop=True)


def fetch_ohlc_df(client: Any, q: OhlcQuery) -> pd.DataFrame:
    rows = fetch_ohlc_rows(client, q)
    return ohlc_rows_to_df(rows, q)
