"""Yahoo Finance client for historical stock price data."""

import pandas as pd
import requests
from datetime import datetime, timedelta
from typing import Optional
from dataclasses import dataclass


@dataclass
class YahooConfig:
    """Configuration for Yahoo Finance API."""
    base_url: str = "https://query1.finance.yahoo.com/v8/finance/chart"


class YahooClient:
    """Client for Yahoo Finance historical price data."""

    def __init__(self, config: Optional[YahooConfig] = None):
        self.config = config or YahooConfig()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
        })

    def get_historical_prices(
        self,
        ticker: str,
        years: int = 10,
        end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Fetch historical daily prices for a ticker.

        Args:
            ticker: Stock symbol (e.g., "MSFT")
            years: Number of years of history
            end_date: End date (defaults to today)

        Returns:
            DataFrame with columns: date, open, high, low, close, volume, adj_close
        """
        end_date = end_date or datetime.now()
        start_date = end_date - timedelta(days=years * 365)

        # Convert to Unix timestamps
        period1 = int(start_date.timestamp())
        period2 = int(end_date.timestamp())

        url = f"{self.config.base_url}/{ticker}"
        params = {
            "period1": period1,
            "period2": period2,
            "interval": "1d",
            "events": "history"
        }

        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        result = data["chart"]["result"][0]

        timestamps = result["timestamp"]
        quotes = result["indicators"]["quote"][0]
        adj_close = result["indicators"]["adjclose"][0]["adjclose"]

        df = pd.DataFrame({
            "date": pd.to_datetime(timestamps, unit="s").date,
            "open": quotes["open"],
            "high": quotes["high"],
            "low": quotes["low"],
            "close": quotes["close"],
            "volume": quotes["volume"],
            "adj_close": adj_close
        })

        # Remove any rows with missing data
        df = df.dropna(subset=["close", "adj_close"])
        df = df.reset_index(drop=True)

        return df

    def calculate_returns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate daily and cumulative returns.

        Args:
            df: DataFrame with adj_close column

        Returns:
            DataFrame with additional return columns
        """
        df = df.copy()

        # Daily returns
        df["daily_return"] = df["adj_close"].pct_change()

        # Cumulative returns
        df["cumulative_return"] = (1 + df["daily_return"]).cumprod() - 1

        return df


def main():
    """Example usage."""
    client = YahooClient()

    ticker = "MSFT"
    print(f"Fetching 10 years of daily prices for {ticker}...")

    df = client.get_historical_prices(ticker, years=10)
    df = client.calculate_returns(df)

    print(f"\nData range: {df['date'].min()} to {df['date'].max()}")
    print(f"Trading days: {len(df)}")
    print(f"Latest close: ${df['adj_close'].iloc[-1]:.2f}")
    print(f"Total return: {df['cumulative_return'].iloc[-1]:.1%}")


if __name__ == "__main__":
    main()
