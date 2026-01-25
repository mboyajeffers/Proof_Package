#!/usr/bin/env python3
"""
CMS Enterprise Scale - Data Expansion
Adds more real data to reach 2.5M+ rows

Additional Sources:
1. 1-hour data for all tickers (not just Disney)
2. More media/entertainment tickers
3. All Disney subsidiary/related CIKs from SEC
4. Expanded FRED economic data
5. International Disney-related tickers
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

OUTPUT_DIR = "/Users/mboyajeffers/Downloads/LinkedIn_Proof_Package/Enterprise_Scale/data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# Additional tickers for more data
ADDITIONAL_TICKERS = [
    # More media/entertainment
    "SONY", "SIRI", "IMAX", "LGF.A", "SPOT", "TME", "BILI", "IQ",
    # Theme park/hospitality (Disney competitors)
    "SIX", "FUN", "SEAS", "MAR", "HLT", "H", "WH",
    # Advertising (Disney revenue stream)
    "GOOGL", "META", "TTD", "MGNI", "PUBM",
    # Cable/broadcast
    "CHTR", "DISH", "SATS",
    # Retail (Disney merchandise)
    "TGT", "WMT", "COST", "DG",
]

INTERNATIONAL_TICKERS = [
    # Tokyo Disney (Oriental Land)
    "4661.T",
    # European media
    "VIV.PA", "PUB.PA",
]

# More FRED series for comprehensive economic picture
ADDITIONAL_FRED_SERIES = [
    "INDPRO",      # Industrial Production
    "HOUST",       # Housing Starts
    "RSXFS",       # Retail Sales ex Food Services
    "BOPGSTB",     # Trade Balance
    "M2SL",        # M2 Money Supply
    "DFF",         # Federal Funds Rate
    "MORTGAGE30US", # 30-Year Mortgage
    "UMCSENT",     # Consumer Sentiment
    "DSPIC96",     # Real Disposable Income
    "PCEPI",       # PCE Price Index
    "PAYEMS",      # Nonfarm Payrolls
    "ICSA",        # Initial Jobless Claims
    "CSUSHPINSA",  # Case-Shiller Home Price
    "NASDAQCOM",   # NASDAQ Composite
    "DJIA",        # Dow Jones
    "SP500",       # S&P 500
    "VIXCLS",      # VIX
    "DCOILWTICO",  # Crude Oil WTI
    "DEXUSEU",     # USD/EUR Exchange
    "DEXJPUS",     # JPY/USD Exchange
]

FRED_API_KEY = "1a8b93463acad0ef6940634da35cfc7a"

def pull_hourly_data_batch(tickers):
    """Pull 1-hour data for multiple tickers"""
    log("\n" + "="*60)
    log("EXPANSION PHASE 1: Hourly Data for All Tickers")
    log("="*60)

    all_data = []

    for ticker in tickers:
        try:
            log(f"  Pulling {ticker} (1h)...")
            stock = yf.Ticker(ticker)
            df = stock.history(period="730d", interval="1h")

            if len(df) > 0:
                df['ticker'] = ticker
                df['interval'] = '1h'
                df = df.reset_index()
                log(f"    ✓ {len(df):,} rows")
                all_data.append(df)
            else:
                log(f"    ✗ No data")

            time.sleep(0.3)
        except Exception as e:
            log(f"    ✗ Error: {e}")

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_csv(f"{OUTPUT_DIR}/hourly_data_expanded.csv", index=False)
        log(f"\n✓ Hourly expansion saved: {len(combined):,} rows")
        return combined
    return None

def pull_minute_data_batch(tickers):
    """Pull 1-minute data for key tickers (7 days available)"""
    log("\n" + "="*60)
    log("EXPANSION PHASE 2: Minute Data for Key Tickers")
    log("="*60)

    all_data = []

    for ticker in tickers[:15]:  # Top 15 tickers
        try:
            log(f"  Pulling {ticker} (1m)...")
            stock = yf.Ticker(ticker)
            df = stock.history(period="7d", interval="1m")

            if len(df) > 0:
                df['ticker'] = ticker
                df['interval'] = '1m'
                df = df.reset_index()
                log(f"    ✓ {len(df):,} rows")
                all_data.append(df)
            else:
                log(f"    ✗ No data")

            time.sleep(0.3)
        except Exception as e:
            log(f"    ✗ Error: {e}")

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_csv(f"{OUTPUT_DIR}/minute_data_expanded.csv", index=False)
        log(f"\n✓ Minute expansion saved: {len(combined):,} rows")
        return combined
    return None

def pull_additional_daily(tickers):
    """Pull daily data for additional tickers"""
    log("\n" + "="*60)
    log("EXPANSION PHASE 3: Additional Daily Data")
    log("="*60)

    all_data = []

    for ticker in tickers:
        try:
            log(f"  Pulling {ticker} (1d)...")
            stock = yf.Ticker(ticker)
            df = stock.history(period="max", interval="1d")

            if len(df) > 0:
                df['ticker'] = ticker
                df['interval'] = '1d'
                df = df.reset_index()
                log(f"    ✓ {len(df):,} rows")
                all_data.append(df)
            else:
                log(f"    ✗ No data")

            time.sleep(0.2)
        except Exception as e:
            log(f"    ✗ Error: {e}")

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_csv(f"{OUTPUT_DIR}/daily_data_expanded.csv", index=False)
        log(f"\n✓ Daily expansion saved: {len(combined):,} rows")
        return combined
    return None

def pull_expanded_fred():
    """Pull expanded FRED data"""
    log("\n" + "="*60)
    log("EXPANSION PHASE 4: Expanded FRED Economic Data")
    log("="*60)

    all_data = []

    for series_id in ADDITIONAL_FRED_SERIES:
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations"
            params = {
                'series_id': series_id,
                'api_key': FRED_API_KEY,
                'file_type': 'json',
                'observation_start': '1970-01-01'  # Go back further
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
                    log(f"    ✓ {len(df):,} observations")

            time.sleep(0.2)
        except Exception as e:
            log(f"    ✗ Error: {e}")

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_csv(f"{OUTPUT_DIR}/fred_expanded.csv", index=False)
        log(f"\n✓ FRED expansion saved: {len(combined):,} rows")
        return combined
    return None

def pull_more_options():
    """Pull options for more tickers"""
    log("\n" + "="*60)
    log("EXPANSION PHASE 5: More Options Data")
    log("="*60)

    options_tickers = ["AAPL", "AMZN", "GOOGL", "META", "MSFT", "SPY"]
    all_data = []

    for ticker in options_tickers:
        try:
            log(f"  Pulling {ticker} options...")
            stock = yf.Ticker(ticker)
            expirations = stock.options

            for exp in expirations[:15]:
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

                    all_data.append(calls)
                    all_data.append(puts)
                    time.sleep(0.1)
                except:
                    continue

            log(f"    ✓ Options collected for {ticker}")
            time.sleep(0.5)
        except Exception as e:
            log(f"    ✗ Error: {e}")

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined['pull_date'] = datetime.now().strftime('%Y-%m-%d')
        combined.to_csv(f"{OUTPUT_DIR}/options_expanded.csv", index=False)
        log(f"\n✓ Options expansion saved: {len(combined):,} rows")
        return combined
    return None

def create_expanded_correlations():
    """Create expanded rolling correlations"""
    log("\n" + "="*60)
    log("EXPANSION PHASE 6: Expanded Correlation Analysis")
    log("="*60)

    try:
        # Load all daily data
        files = [
            f"{OUTPUT_DIR}/stock_data.csv",
            f"{OUTPUT_DIR}/daily_data_expanded.csv"
        ]

        all_daily = []
        for f in files:
            if os.path.exists(f):
                df = pd.read_csv(f)
                daily = df[df['interval'] == '1d'] if 'interval' in df.columns else df
                all_daily.append(daily)

        if not all_daily:
            return None

        combined = pd.concat(all_daily, ignore_index=True)

        # Pivot for correlations
        date_col = 'Date' if 'Date' in combined.columns else 'Datetime'
        pivot = combined.pivot_table(index=date_col, columns='ticker', values='Close', aggfunc='first')
        returns = pivot.pct_change()

        # Rolling correlations - all pairs
        correlation_rows = []
        windows = [10, 20, 30, 60, 90, 120, 180, 252]

        log(f"  Computing correlations for {len(returns.columns)} tickers...")

        tickers = list(returns.columns)
        for i, ticker1 in enumerate(tickers):
            if i % 5 == 0:
                log(f"    Processing {ticker1} ({i+1}/{len(tickers)})...")

            for ticker2 in tickers[i+1:]:  # Avoid duplicates
                for window in windows:
                    try:
                        rolling_corr = returns[ticker1].rolling(window).corr(returns[ticker2])

                        # Sample every 5th day to reduce size
                        for idx, (date, corr_value) in enumerate(rolling_corr.items()):
                            if idx % 5 == 0 and pd.notna(corr_value):
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
            log(f"\n✓ Expanded correlations saved: {len(df):,} rows")
            return df
    except Exception as e:
        log(f"  ✗ Error: {e}")
        return None

def create_expanded_technical_indicators():
    """Create technical indicators for all expanded data"""
    log("\n" + "="*60)
    log("EXPANSION PHASE 7: Expanded Technical Analysis")
    log("="*60)

    try:
        # Load hourly data
        hourly_file = f"{OUTPUT_DIR}/hourly_data_expanded.csv"
        if not os.path.exists(hourly_file):
            log("  No hourly data found")
            return None

        df = pd.read_csv(hourly_file)
        log(f"  Processing {len(df):,} hourly rows...")

        all_indicators = []

        for ticker in df['ticker'].unique():
            ticker_df = df[df['ticker'] == ticker].copy()

            if len(ticker_df) < 50:
                continue

            close = ticker_df['Close'].values

            # Technical indicators
            for window in [5, 10, 20, 50]:
                if len(close) >= window:
                    ticker_df[f'SMA_{window}'] = pd.Series(close).rolling(window).mean().values
                    ticker_df[f'EMA_{window}'] = pd.Series(close).ewm(span=window).mean().values

            # Volatility
            for window in [10, 20]:
                if len(close) >= window:
                    ticker_df[f'volatility_{window}'] = pd.Series(close).pct_change().rolling(window).std().values

            # RSI
            delta = pd.Series(close).diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / loss
            ticker_df['RSI_14'] = (100 - (100 / (1 + rs))).values

            # Momentum
            for period in [1, 5, 10]:
                ticker_df[f'momentum_{period}'] = pd.Series(close).pct_change(period).values

            all_indicators.append(ticker_df)

        if all_indicators:
            combined = pd.concat(all_indicators, ignore_index=True)
            combined.to_csv(f"{OUTPUT_DIR}/technical_expanded.csv", index=False)
            log(f"\n✓ Expanded technical indicators saved: {len(combined):,} rows")
            return combined
    except Exception as e:
        log(f"  ✗ Error: {e}")
        return None

def main():
    print("""
    ╔══════════════════════════════════════════════════════════════════════╗
    ║                                                                      ║
    ║   CMS ENTERPRISE SCALE - DATA EXPANSION                              ║
    ║   Target: 2.5M+ Real Data Rows                                       ║
    ║                                                                      ║
    ╚══════════════════════════════════════════════════════════════════════╝
    """)

    start_time = datetime.now()
    total_rows = 0
    summary = {}

    # Existing tickers to expand
    existing_tickers = [
        "DIS", "SPY", "DIA", "XLY", "VTI", "VOO", "IYC", "PEJ", "FDIS",
        "CMCSA", "NFLX", "WBD", "FOXA", "LYV", "MSGS", "AMC", "CNK",
        "AAPL", "GOOGL", "AMZN", "ROKU", "T", "VZ", "TMUS"
    ]

    # Phase 1: Hourly data for existing tickers
    hourly_df = pull_hourly_data_batch(existing_tickers)
    if hourly_df is not None:
        total_rows += len(hourly_df)
        summary['Hourly Expansion'] = len(hourly_df)

    # Phase 2: Minute data for key tickers
    minute_df = pull_minute_data_batch(existing_tickers)
    if minute_df is not None:
        total_rows += len(minute_df)
        summary['Minute Expansion'] = len(minute_df)

    # Phase 3: Additional daily tickers
    daily_df = pull_additional_daily(ADDITIONAL_TICKERS)
    if daily_df is not None:
        total_rows += len(daily_df)
        summary['Daily Expansion'] = len(daily_df)

    # Phase 4: Expanded FRED data
    fred_df = pull_expanded_fred()
    if fred_df is not None:
        total_rows += len(fred_df)
        summary['FRED Expansion'] = len(fred_df)

    # Phase 5: More options
    options_df = pull_more_options()
    if options_df is not None:
        total_rows += len(options_df)
        summary['Options Expansion'] = len(options_df)

    # Phase 6: Expanded correlations
    corr_df = create_expanded_correlations()
    if corr_df is not None:
        total_rows += len(corr_df)
        summary['Correlation Expansion'] = len(corr_df)

    # Phase 7: Technical indicators on hourly data
    tech_df = create_expanded_technical_indicators()
    if tech_df is not None:
        total_rows += len(tech_df)
        summary['Technical Expansion'] = len(tech_df)

    duration = (datetime.now() - start_time).total_seconds()

    print(f"""

    ╔══════════════════════════════════════════════════════════════════════╗
    ║                   DATA EXPANSION COMPLETE                            ║
    ╠══════════════════════════════════════════════════════════════════════╣
    """)

    for source, count in summary.items():
        print(f"    ║   {source:<30} {count:>15,} rows   ║")

    print(f"""    ╠══════════════════════════════════════════════════════════════════════╣
    ║   EXPANSION TOTAL:                    {total_rows:>15,}        ║
    ║   Duration:                           {duration:>12.1f} sec        ║
    ╚══════════════════════════════════════════════════════════════════════╝
    """)

    # Update summary
    summary_file = f"{OUTPUT_DIR}/pull_summary.json"
    if os.path.exists(summary_file):
        with open(summary_file, 'r') as f:
            existing = json.load(f)
        existing['expansion'] = summary
        existing['expansion_total'] = total_rows
        existing['grand_total'] = existing.get('total_rows', 0) + total_rows
        with open(summary_file, 'w') as f:
            json.dump(existing, f, indent=2)

    return total_rows

if __name__ == "__main__":
    main()
