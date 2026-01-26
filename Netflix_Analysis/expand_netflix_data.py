#!/usr/bin/env python3
"""
Netflix Market Analysis - Data Expansion
Author: Mboya Jeffers, Data Engineer & Analyst
Date: January 2026

Expand Netflix dataset to 5M+ rows through:
1. Hourly data for all tickers (730 days)
2. Minute data for key tickers (7 days)
3. Expanded correlation matrix (all pairs, all windows)
4. Additional FRED economic series
5. Expanded options data
"""

import yfinance as yf
import pandas as pd
import numpy as np
import requests
import os
import json
from datetime import datetime, timedelta
import time
import warnings
warnings.filterwarnings('ignore')

OUTPUT_DIR = "/Users/mboyajeffers/Downloads/LinkedIn_Proof_Package/Netflix_Analysis/data"

# All tickers for expansion
ALL_TICKERS = [
    "NFLX", "DIS", "WBD", "PARA", "CMCSA", "AMZN", "AAPL", "ROKU", "FUBO",
    "GOOGL", "META", "MSFT", "CRM", "ADBE", "SPOT",
    "SONY", "FOXA", "NWSA", "AMC", "CNK", "IMAX", "LYV",
    "T", "VZ", "TMUS", "CHTR",
    "SPY", "QQQ", "IWM", "VTI", "VOO",
    "VGT", "XLC", "VOX", "ARKK"
]

# Key tickers for minute data
KEY_TICKERS = ["NFLX", "DIS", "AMZN", "AAPL", "GOOGL", "META", "MSFT", "ROKU", "SPY", "QQQ"]

# Additional FRED series
FRED_API_KEY = "1a8b93463acad0ef6940634da35cfc7a"
ADDITIONAL_FRED = [
    "GDP",        # Gross Domestic Product
    "GDPC1",      # Real GDP
    "FEDFUNDS",   # Federal Funds Rate
    "TB3MS",      # 3-Month Treasury
    "DGS2",       # 2-Year Treasury
    "DGS5",       # 5-Year Treasury
    "DGS30",      # 30-Year Treasury
    "MORTGAGE30US", # 30-Year Mortgage
    "VIXCLS",     # VIX
    "DTWEXBGS",   # Trade Weighted Dollar
    "BOGMBASE",   # Monetary Base
    "M2SL",       # M2 Money Stock
    "PSAVERT",    # Personal Saving Rate
    "TOTALSA",    # Total Vehicle Sales
    "JTSJOL",     # Job Openings
]

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def pull_hourly_data():
    """Pull hourly data for all tickers"""
    log("\n" + "="*60)
    log("EXPANSION 1: Hourly Data (730 days)")
    log("="*60)

    all_data = []

    for ticker in ALL_TICKERS:
        try:
            log(f"  Pulling {ticker} hourly...")
            stock = yf.Ticker(ticker)
            df = stock.history(period="730d", interval="1h")
            if len(df) > 0:
                df['ticker'] = ticker
                df['interval'] = '1h'
                df = df.reset_index()
                all_data.append(df)
                log(f"    Got {len(df):,} rows")
            time.sleep(0.2)
        except Exception as e:
            log(f"    Error: {e}")

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_csv(f"{OUTPUT_DIR}/hourly_data_expanded.csv", index=False)
        log(f"\nHourly data saved: {len(combined):,} rows")
        return combined
    return None

def pull_minute_data():
    """Pull minute data for key tickers"""
    log("\n" + "="*60)
    log("EXPANSION 2: Minute Data (7 days)")
    log("="*60)

    all_data = []

    for ticker in KEY_TICKERS:
        try:
            log(f"  Pulling {ticker} minute...")
            stock = yf.Ticker(ticker)
            df = stock.history(period="7d", interval="1m")
            if len(df) > 0:
                df['ticker'] = ticker
                df['interval'] = '1m'
                df = df.reset_index()
                all_data.append(df)
                log(f"    Got {len(df):,} rows")
            time.sleep(0.3)
        except Exception as e:
            log(f"    Error: {e}")

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_csv(f"{OUTPUT_DIR}/minute_data_expanded.csv", index=False)
        log(f"\nMinute data saved: {len(combined):,} rows")
        return combined
    return None

def pull_additional_fred():
    """Pull additional FRED economic series"""
    log("\n" + "="*60)
    log("EXPANSION 3: Additional FRED Data")
    log("="*60)

    all_data = []

    # Load existing FRED data
    existing_file = f"{OUTPUT_DIR}/fred_data.csv"
    if os.path.exists(existing_file):
        existing = pd.read_csv(existing_file)
        all_data.append(existing)
        log(f"  Loaded existing: {len(existing):,} rows")

    for series_id in ADDITIONAL_FRED:
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations"
            params = {
                'series_id': series_id,
                'api_key': FRED_API_KEY,
                'file_type': 'json',
                'observation_start': '1980-01-01'  # Longer history
            }

            log(f"  Pulling {series_id}...")
            response = requests.get(url, params=params)

            if response.status_code == 200:
                data = response.json()
                observations = data.get('observations', [])

                if observations:
                    df = pd.DataFrame(observations)
                    df['series_id'] = series_id
                    all_data.append(df)
                    log(f"    Got {len(df):,} observations")

            time.sleep(0.2)
        except Exception as e:
            log(f"    Error: {e}")

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_csv(f"{OUTPUT_DIR}/fred_expanded.csv", index=False)
        log(f"\nFRED expanded saved: {len(combined):,} rows")
        return combined
    return None

def expand_correlations():
    """Expand correlation matrix to all pairs across all windows"""
    log("\n" + "="*60)
    log("EXPANSION 4: Full Correlation Matrix")
    log("="*60)

    # Load stock data
    stock_file = f"{OUTPUT_DIR}/stock_data.csv"
    if not os.path.exists(stock_file):
        log("  No stock data found")
        return None

    stock_df = pd.read_csv(stock_file, low_memory=False)
    daily_df = stock_df[stock_df['interval'] == '1d'].copy()

    if len(daily_df) == 0:
        log("  No daily data")
        return None

    try:
        pivot_df = daily_df.pivot_table(
            index='Date',
            columns='ticker',
            values='Close',
            aggfunc='first'
        )

        returns = pivot_df.pct_change()
        tickers = list(returns.columns)

        log(f"  Computing all-pairs correlations for {len(tickers)} tickers...")

        correlation_rows = []
        windows = [10, 20, 30, 60, 90, 120, 180, 252]

        # All unique pairs
        total_pairs = 0
        for i, ticker1 in enumerate(tickers):
            for ticker2 in tickers[i+1:]:
                total_pairs += 1

        log(f"  Total pairs to compute: {total_pairs}")

        pair_count = 0
        for i, ticker1 in enumerate(tickers):
            for ticker2 in tickers[i+1:]:
                pair_count += 1
                if pair_count % 50 == 0:
                    log(f"    Processing pair {pair_count}/{total_pairs}...")

                for window in windows:
                    try:
                        rolling_corr = returns[ticker1].rolling(window).corr(returns[ticker2])

                        for date, corr_value in rolling_corr.items():
                            if pd.notna(corr_value):
                                correlation_rows.append({
                                    'date': date,
                                    'ticker_1': ticker1,
                                    'ticker_2': ticker2,
                                    'window': window,
                                    'correlation': corr_value
                                })
                    except:
                        continue

        if correlation_rows:
            df = pd.DataFrame(correlation_rows)
            df.to_csv(f"{OUTPUT_DIR}/correlations_expanded.csv", index=False)
            log(f"\nExpanded correlations saved: {len(df):,} rows")
            return df

    except Exception as e:
        log(f"  Error: {e}")
        return None

def expand_technical_indicators():
    """Compute technical indicators on hourly data"""
    log("\n" + "="*60)
    log("EXPANSION 5: Hourly Technical Indicators")
    log("="*60)

    hourly_file = f"{OUTPUT_DIR}/hourly_data_expanded.csv"
    if not os.path.exists(hourly_file):
        log("  No hourly data found")
        return None

    hourly_df = pd.read_csv(hourly_file, low_memory=False)
    log(f"  Loaded {len(hourly_df):,} hourly rows")

    all_indicators = []

    for ticker in hourly_df['ticker'].unique():
        ticker_df = hourly_df[hourly_df['ticker'] == ticker].copy()

        if 'Datetime' in ticker_df.columns:
            ticker_df = ticker_df.sort_values('Datetime')
        elif 'Date' in ticker_df.columns:
            ticker_df = ticker_df.sort_values('Date')

        if len(ticker_df) < 50:
            continue

        close = ticker_df['Close'].values
        high = ticker_df['High'].values
        low = ticker_df['Low'].values
        volume = ticker_df['Volume'].values

        # Moving averages (shorter windows for hourly)
        for window in [5, 10, 20, 50]:
            if len(close) >= window:
                ticker_df[f'SMA_{window}'] = pd.Series(close).rolling(window).mean().values
                ticker_df[f'EMA_{window}'] = pd.Series(close).ewm(span=window).mean().values

        # RSI
        delta = pd.Series(close).diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        ticker_df['RSI_14'] = (100 - (100 / (1 + rs))).values

        # MACD
        ema12 = pd.Series(close).ewm(span=12).mean()
        ema26 = pd.Series(close).ewm(span=26).mean()
        ticker_df['MACD'] = (ema12 - ema26).values

        # Bollinger Bands
        sma20 = pd.Series(close).rolling(20).mean()
        std20 = pd.Series(close).rolling(20).std()
        ticker_df['BB_upper'] = (sma20 + 2 * std20).values
        ticker_df['BB_lower'] = (sma20 - 2 * std20).values

        # Volume
        ticker_df['volume_sma_20'] = pd.Series(volume).rolling(20).mean().values

        # Momentum
        for period in [1, 5, 10]:
            if len(close) > period:
                ticker_df[f'return_{period}h'] = pd.Series(close).pct_change(period).values

        all_indicators.append(ticker_df)
        log(f"    {ticker}: {len(ticker_df):,} rows")

    if all_indicators:
        combined = pd.concat(all_indicators, ignore_index=True)
        combined.to_csv(f"{OUTPUT_DIR}/technical_expanded.csv", index=False)
        log(f"\nHourly indicators saved: {len(combined):,} rows")
        return combined
    return None

def pull_more_options():
    """Pull additional options data"""
    log("\n" + "="*60)
    log("EXPANSION 6: Additional Options Data")
    log("="*60)

    additional_tickers = ["SPY", "QQQ", "IWM", "VTI", "ARKK"]
    all_options = []

    # Load existing
    existing_file = f"{OUTPUT_DIR}/options_data.csv"
    if os.path.exists(existing_file):
        existing = pd.read_csv(existing_file)
        all_options.append(existing)
        log(f"  Loaded existing: {len(existing):,} contracts")

    for ticker in additional_tickers:
        try:
            log(f"  Pulling {ticker} options...")
            stock = yf.Ticker(ticker)
            expirations = stock.options

            for exp in expirations[:20]:
                try:
                    opt = stock.option_chain(exp)
                    calls = opt.calls.copy()
                    calls['type'] = 'call'
                    calls['expiration'] = exp
                    calls['ticker'] = ticker

                    puts = opt.puts.copy()
                    puts['type'] = 'put'
                    puts['expiration'] = exp
                    puts['ticker'] = ticker

                    all_options.append(calls)
                    all_options.append(puts)
                    time.sleep(0.1)
                except:
                    continue

            log(f"    Done")
            time.sleep(0.5)
        except Exception as e:
            log(f"    Error: {e}")

    if all_options:
        combined = pd.concat(all_options, ignore_index=True)
        combined['pull_date'] = datetime.now().strftime('%Y-%m-%d')
        combined.to_csv(f"{OUTPUT_DIR}/options_expanded.csv", index=False)
        log(f"\nOptions expanded saved: {len(combined):,} contracts")
        return combined
    return None

def main():
    print("""
    ╔══════════════════════════════════════════════════════════════════════╗
    ║                                                                      ║
    ║   NETFLIX DATA EXPANSION                                             ║
    ║   Target: 5M+ Total Rows                                             ║
    ║                                                                      ║
    ║   Expanding:                                                         ║
    ║   • Hourly data for all tickers                                      ║
    ║   • Minute data for key tickers                                      ║
    ║   • Full correlation matrix                                          ║
    ║   • Additional FRED series                                           ║
    ║   • More options data                                                ║
    ║                                                                      ║
    ╚══════════════════════════════════════════════════════════════════════╝
    """)

    start_time = datetime.now()
    total_rows = 0
    summary = {}

    # Count existing data
    log("Counting existing data...")
    for filename in os.listdir(OUTPUT_DIR):
        if filename.endswith('.csv'):
            filepath = os.path.join(OUTPUT_DIR, filename)
            with open(filepath, 'r') as f:
                count = sum(1 for _ in f) - 1
                total_rows += count
                log(f"  {filename}: {count:,} rows")

    log(f"\nExisting total: {total_rows:,} rows")

    # Expansions
    hourly_df = pull_hourly_data()
    if hourly_df is not None:
        summary['Hourly Expansion'] = len(hourly_df)

    minute_df = pull_minute_data()
    if minute_df is not None:
        summary['Minute Expansion'] = len(minute_df)

    fred_df = pull_additional_fred()
    if fred_df is not None:
        summary['FRED Expansion'] = len(fred_df)

    corr_df = expand_correlations()
    if corr_df is not None:
        summary['Correlation Expansion'] = len(corr_df)

    tech_df = expand_technical_indicators()
    if tech_df is not None:
        summary['Technical Expansion'] = len(tech_df)

    options_df = pull_more_options()
    if options_df is not None:
        summary['Options Expansion'] = len(options_df)

    # Final count
    final_total = 0
    total_size = 0
    log("\n" + "="*60)
    log("FINAL DATA INVENTORY")
    log("="*60)

    for filename in sorted(os.listdir(OUTPUT_DIR)):
        if filename.endswith('.csv'):
            filepath = os.path.join(OUTPUT_DIR, filename)
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            with open(filepath, 'r') as f:
                count = sum(1 for _ in f) - 1
                final_total += count
                total_size += size_mb
                log(f"  {filename}: {count:,} rows ({size_mb:.1f} MB)")

    duration = (datetime.now() - start_time).total_seconds()

    print(f"""

    ╔══════════════════════════════════════════════════════════════════════╗
    ║                      EXPANSION COMPLETE                              ║
    ╠══════════════════════════════════════════════════════════════════════╣
    ║   TOTAL ROWS:                     {final_total:>15,}        ║
    ║   TOTAL SIZE:                     {total_size:>12.1f} MB        ║
    ║   Duration:                       {duration:>12.1f} sec        ║
    ╚══════════════════════════════════════════════════════════════════════╝
    """)

    # Save summary
    summary_data = {
        'expansion_date': datetime.now().isoformat(),
        'total_rows': final_total,
        'total_size_mb': total_size,
        'expansions': summary,
        'duration_seconds': duration
    }

    with open(f"{OUTPUT_DIR}/expansion_summary.json", 'w') as f:
        json.dump(summary_data, f, indent=2)

    return final_total

if __name__ == "__main__":
    main()
