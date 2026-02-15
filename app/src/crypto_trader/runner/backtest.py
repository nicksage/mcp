from __future__ import annotations

import argparse

import pandas as pd
from coingecko_sdk import APIError, NotFoundError, RateLimitError

from crypto_trader.data.coingecko_client import client
from crypto_trader.data.ohlc import OhlcQuery, fetch_ohlc_df
from crypto_trader.indicators.basic import add_bbands_and_macd


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch OHLC from CoinGecko, compute indicators, print latest rows.")
    parser.add_argument("--coin-id", default="ethereum", help="CoinGecko coin id, e.g. bitcoin, ethereum")
    parser.add_argument("--vs-currency", default="usd", help="Quote currency, e.g. usd")
    parser.add_argument("--days", type=int, default=1, help="Days window supported by CoinGecko OHLC endpoint")
    parser.add_argument("--interval", default="hourly", help="Optional: hourly or daily (depends on plan)")
    args = parser.parse_args()

    q = OhlcQuery(
        coin_id=args.coin_id,
        vs_currency=args.vs_currency,
        days=args.days,
        interval=args.interval,
    )

    try:
        ohlc_df = fetch_ohlc_df(client, q)
        if ohlc_df.empty:
            print("No OHLC rows returned.")
            return

        df = add_bbands_and_macd(ohlc_df)

        # Show latest candles with indicators
        print(df.tail(10).to_string(index=False))

        # Example “strategy placeholder”:
        last = df.iloc[-1]
        if pd.notna(last.get("bb_upper")) and last["close"] > last["bb_upper"]:
            print("\nSignal: PRICE ABOVE UPPER BAND (mean-reversion watch)")
        elif pd.notna(last.get("bb_lower")) and last["close"] < last["bb_lower"]:
            print("\nSignal: PRICE BELOW LOWER BAND (mean-reversion watch)")
        else:
            print("\nSignal: NO BBAND EXTREME")

    except RateLimitError:
        print("Rate limit exceeded (SDK retries already applied). Try again later.")
    except NotFoundError:
        print(f"Coin id not found: {q.coin_id}")
    except APIError as e:
        print(f"CoinGecko API error: {e}")


if __name__ == "__main__":
    main()
