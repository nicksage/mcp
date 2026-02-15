from __future__ import annotations

import pandas as pd


def add_bollinger_bands(
    df: pd.DataFrame,
    price_col: str = "close",
    window: int = 20,
    num_std: float = 2.0,
) -> pd.DataFrame:
    out = df.copy()
    ma = out[price_col].rolling(window=window, min_periods=window).mean()
    std = out[price_col].rolling(window=window, min_periods=window).std(ddof=0)

    out["bb_mid"] = ma
    out["bb_upper"] = ma + num_std * std
    out["bb_lower"] = ma - num_std * std
    out["bb_width"] = (out["bb_upper"] - out["bb_lower"]) / out["bb_mid"]

    return out


def add_macd(
    df: pd.DataFrame,
    price_col: str = "close",
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> pd.DataFrame:
    out = df.copy()

    ema_fast = out[price_col].ewm(span=fast, adjust=False).mean()
    ema_slow = out[price_col].ewm(span=slow, adjust=False).mean()

    out["macd"] = ema_fast - ema_slow
    out["macd_signal"] = out["macd"].ewm(span=signal, adjust=False).mean()
    out["macd_hist"] = out["macd"] - out["macd_signal"]

    return out


def add_bbands_and_macd(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convenience function you asked for:
    takes OHLC df -> adds Bollinger Bands + MACD -> returns df
    """
    out = add_bollinger_bands(df)
    out = add_macd(out)
    return out
