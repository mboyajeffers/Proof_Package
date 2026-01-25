#!/usr/bin/env python3
"""
CMS Enterprise Scale Demo - Real Disney Data Aggregator
Author: Mboya Jeffers
Purpose: Pull REAL Disney-related data from multiple public sources to demonstrate
         CMS pipeline capability at 1M+ and 2M+ row scale

Data Sources (ALL REAL):
1. Yahoo Finance - DIS stock (daily, hourly, minute)
2. Yahoo Finance - Disney-related ETFs (SPY, DIA, XLY, VTI, VOO)
3. Yahoo Finance - Competitor stocks (CMCSA, NFLX, WBD, PARA, FOX)
4. SEC EDGAR - Disney company facts (all reported financials)
5. SEC EDGAR - Insider transactions
6. SEC EDGAR - Institutional holdings (13F)
7. FRED - Economic indicators affecting media/entertainment

Output: Chunked.csv files for safe processing
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

# Disney CIK for SEC EDGAR
DISNEY_CIK = "0001744489"  # The Walt Disney Company

# Tickers to pull
DISNEY_TICKER = "DIS"
DISNEY_ETFS = ["SPY", "DIA", "XLY", "VTI", "VOO", "IYC", "PEJ", "FDIS"]  # ETFs holding Disney
COMPETITORS = ["CMCSA", "NFLX", "WBD", "PARA", "FOXA", "LYV", "MSGS", "AMC", "CNK"]
SUPPLIERS_PARTNERS = ["AAPL", "GOOGL", "AMZN", "ROKU", "T", "VZ", "TMUS"]  # Streaming/distribution

# FRED Series for entertainment industry context
FRED_API_KEY = "1a8b93463acad0ef6940634da35cfc7a"
FRED_SERIES = [
    "RSAFS",      # Retail Sales
    "PCEDG",      # Personal Consumption: Durable Goods
    "PCESVADNS",  # Personal Consumption: Services - Recreation
    "CPIUFDSL",   # CPI
    "UNRATE",     # Unemployment
]

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# ============================================================================
# YAHOO FINANCE DATA PULLS
# ============================================================================

def pull_stock_data(ticker, period="max", interval="1d"):
    """Pull stock data from Yahoo Finance"""
    try:
        log(f"  Pulling {ticker} ({interval})...")
        stock = yf.Ticker(ticker)
        df = stock.history(period=period, interval=interval)
        if len(df) > 0:
            df['ticker'] = ticker
            df['interval'] = interval
            df = df.reset_index()
            log(f"    ✓ {len(df):,} rows")
            return df
        else:
            log(f"    ✗ No data")
            return None
    except Exception as e:
        log(f"    ✗ Error: {e}")
        return None

def pull_options_chain(ticker):
    """Pull current options chain - thousands of real contracts"""
    try:
        log(f"  Pulling {ticker} options chain...")
        stock = yf.Ticker(ticker)

        # Get all expiration dates
        expirations = stock.options
        log(f"    Found {len(expirations)} expiration dates")

        all_options = []
        for exp in expirations[:20]:  # Limit to avoid rate limits
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
                time.sleep(0.1)  # Rate limit
            except:
                continue

        if all_options:
            df = pd.concat(all_options, ignore_index=True)
            df['pull_date'] = datetime.now().strftime('%Y-%m-%d')
            log(f"    ✓ {len(df):,} option contracts")
            return df
        return None
    except Exception as e:
        log(f"    ✗ Error: {e}")
        return None

def pull_all_stock_data():
    """Pull all stock data from Yahoo Finance"""
    log("\n" + "="*60)
    log("PHASE 1: Yahoo Finance Stock Data")
    log("="*60)

    all_data = []
    row_counts = {}

    # Disney at multiple intervals
    log("\n[Disney Primary Data]")
    for interval in ["1d", "1h", "1m"]:
        if interval == "1d":
            df = pull_stock_data(DISNEY_TICKER, period="max", interval=interval)
        elif interval == "1h":
            df = pull_stock_data(DISNEY_TICKER, period="730d", interval=interval)
        else:
            df = pull_stock_data(DISNEY_TICKER, period="7d", interval=interval)

        if df is not None:
            all_data.append(df)
            row_counts[f"DIS_{interval}"] = len(df)

    # ETFs holding Disney
    log("\n[ETFs Holding Disney]")
    for ticker in DISNEY_ETFS:
        df = pull_stock_data(ticker, period="max", interval="1d")
        if df is not None:
            all_data.append(df)
            row_counts[ticker] = len(df)
        time.sleep(0.2)

    # Competitors
    log("\n[Disney Competitors]")
    for ticker in COMPETITORS:
        df = pull_stock_data(ticker, period="max", interval="1d")
        if df is not None:
            all_data.append(df)
            row_counts[ticker] = len(df)
        time.sleep(0.2)

    # Suppliers/Partners
    log("\n[Disney Suppliers & Partners]")
    for ticker in SUPPLIERS_PARTNERS:
        df = pull_stock_data(ticker, period="max", interval="1d")
        if df is not None:
            all_data.append(df)
            row_counts[ticker] = len(df)
        time.sleep(0.2)

    # Combine all stock data
    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_csv(f"{OUTPUT_DIR}/stock_data.csv", index=False)
        log(f"\n✓ Stock data saved: {len(combined):,} rows")
        return combined, row_counts
    return None, row_counts

def pull_all_options():
    """Pull options data for Disney and key related tickers"""
    log("\n" + "="*60)
    log("PHASE 2: Options Chain Data")
    log("="*60)

    all_options = []
    row_counts = {}

    for ticker in [DISNEY_TICKER] + COMPETITORS[:3]:  # Disney + top 3 competitors
        df = pull_options_chain(ticker)
        if df is not None:
            all_options.append(df)
            row_counts[f"{ticker}_options"] = len(df)
        time.sleep(1)  # Rate limit between tickers

    if all_options:
        combined = pd.concat(all_options, ignore_index=True)
        combined.to_csv(f"{OUTPUT_DIR}/options_data.csv", index=False)
        log(f"\n✓ Options data saved: {len(combined):,} rows")
        return combined, row_counts
    return None, row_counts

# ============================================================================
# SEC EDGAR DATA
# ============================================================================

def pull_sec_company_facts(cik):
    """Pull all company facts from SEC EDGAR - comprehensive financial data"""
    log("\n" + "="*60)
    log("PHASE 3: SEC EDGAR Company Facts")
    log("="*60)

    try:
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
        headers = {"User-Agent": "CMS Analytics research@cleanmetricsstudios.com"}

        log(f"  Fetching SEC facts for CIK {cik}...")
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()

            # Extract all facts into rows
            rows = []
            facts = data.get('facts', {})

            for taxonomy, concepts in facts.items():
                for concept, details in concepts.items():
                    units = details.get('units', {})
                    for unit, values in units.items():
                        for v in values:
                            rows.append({
                                'cik': cik,
                                'company': data.get('entityName', 'Disney'),
                                'taxonomy': taxonomy,
                                'concept': concept,
                                'unit': unit,
                                'value': v.get('val'),
                                'start_date': v.get('start'),
                                'end_date': v.get('end'),
                                'filed_date': v.get('filed'),
                                'form': v.get('form'),
                                'fiscal_year': v.get('fy'),
                                'fiscal_period': v.get('fp'),
                                'accession': v.get('accn')
                            })

            if rows:
                df = pd.DataFrame(rows)
                df.to_csv(f"{OUTPUT_DIR}/sec_company_facts.csv", index=False)
                log(f"  ✓ SEC Company Facts: {len(df):,} rows")
                return df
        else:
            log(f"  ✗ SEC API returned {response.status_code}")
            return None
    except Exception as e:
        log(f"  ✗ Error: {e}")
        return None

def pull_sec_submissions(cik):
    """Pull all SEC submissions/filings"""
    try:
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        headers = {"User-Agent": "CMS Analytics research@cleanmetricsstudios.com"}

        log(f"  Fetching SEC submissions...")
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            filings = data.get('filings', {}).get('recent', {})

            if filings:
                df = pd.DataFrame(filings)
                df['cik'] = cik
                df['company'] = data.get('name', 'Disney')
                df.to_csv(f"{OUTPUT_DIR}/sec_submissions.csv", index=False)
                log(f"  ✓ SEC Submissions: {len(df):,} rows")
                return df
        return None
    except Exception as e:
        log(f"  ✗ Error: {e}")
        return None

# ============================================================================
# FRED ECONOMIC DATA
# ============================================================================

def pull_fred_data():
    """Pull economic indicator data from FRED"""
    log("\n" + "="*60)
    log("PHASE 4: FRED Economic Indicators")
    log("="*60)

    all_data = []

    for series_id in FRED_SERIES:
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations"
            params = {
                'series_id': series_id,
                'api_key': FRED_API_KEY,
                'file_type': 'json',
                'observation_start': '1990-01-01'
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
        combined.to_csv(f"{OUTPUT_DIR}/fred_data.csv", index=False)
        log(f"\n✓ FRED data saved: {len(combined):,} rows")
        return combined
    return None

# ============================================================================
# ADDITIONAL DATA EXPANSION (REAL DERIVED DATA)
# ============================================================================

def create_technical_indicators(stock_df):
    """Create technical indicators from real price data - all derived from real data"""
    log("\n" + "="*60)
    log("PHASE 5: Technical Indicators (Derived from Real Data)")
    log("="*60)

    if stock_df is None or len(stock_df) == 0:
        return None

    log("  Computing technical indicators for each ticker...")

    all_indicators = []

    for ticker in stock_df['ticker'].unique():
        ticker_df = stock_df[stock_df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('Date' if 'Date' in ticker_df.columns else 'Datetime')

        if len(ticker_df) < 50:
            continue

        close = ticker_df['Close'].values
        high = ticker_df['High'].values
        low = ticker_df['Low'].values
        volume = ticker_df['Volume'].values

        # Moving averages (multiple windows)
        for window in [5, 10, 20, 50, 100, 200]:
            if len(close) >= window:
                ticker_df[f'SMA_{window}'] = pd.Series(close).rolling(window).mean().values
                ticker_df[f'EMA_{window}'] = pd.Series(close).ewm(span=window).mean().values

        # Volatility measures
        for window in [10, 20, 30]:
            if len(close) >= window:
                ticker_df[f'volatility_{window}d'] = pd.Series(close).pct_change().rolling(window).std().values * np.sqrt(252)

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
        ticker_df['MACD_signal'] = pd.Series(ticker_df['MACD']).ewm(span=9).mean().values

        # Bollinger Bands
        sma20 = pd.Series(close).rolling(20).mean()
        std20 = pd.Series(close).rolling(20).std()
        ticker_df['BB_upper'] = (sma20 + 2 * std20).values
        ticker_df['BB_lower'] = (sma20 - 2 * std20).values
        ticker_df['BB_width'] = ((ticker_df['BB_upper'] - ticker_df['BB_lower']) / sma20.values)

        # Volume indicators
        ticker_df['volume_sma_20'] = pd.Series(volume).rolling(20).mean().values
        ticker_df['volume_ratio'] = volume / ticker_df['volume_sma_20']

        # Price momentum
        for period in [1, 5, 10, 20, 60]:
            if len(close) > period:
                ticker_df[f'return_{period}d'] = pd.Series(close).pct_change(period).values

        # Average True Range
        tr1 = high - low
        tr2 = abs(high - np.roll(close, 1))
        tr3 = abs(low - np.roll(close, 1))
        tr = np.maximum(np.maximum(tr1, tr2), tr3)
        ticker_df['ATR_14'] = pd.Series(tr).rolling(14).mean().values

        all_indicators.append(ticker_df)
        log(f"    {ticker}: {len(ticker_df):,} rows with indicators")

    if all_indicators:
        combined = pd.concat(all_indicators, ignore_index=True)
        combined.to_csv(f"{OUTPUT_DIR}/technical_indicators.csv", index=False)
        log(f"\n✓ Technical indicators saved: {len(combined):,} rows")
        return combined
    return None

def create_cross_asset_correlations(stock_df):
    """Create rolling correlations between Disney and all other assets"""
    log("\n" + "="*60)
    log("PHASE 6: Cross-Asset Correlation Matrix (Real Calculations)")
    log("="*60)

    if stock_df is None:
        return None

    # Filter to daily data only for correlations
    daily_df = stock_df[stock_df['interval'] == '1d'].copy()

    if len(daily_df) == 0:
        log("  No daily data for correlations")
        return None

    # Pivot to get returns by ticker
    try:
        pivot_df = daily_df.pivot_table(
            index='Date',
            columns='ticker',
            values='Close',
            aggfunc='first'
        )

        returns = pivot_df.pct_change()

        # Rolling correlations with Disney
        correlation_rows = []
        windows = [20, 60, 120, 252]

        dis_returns = returns.get('DIS')
        if dis_returns is None:
            log("  No DIS data found")
            return None

        for ticker in returns.columns:
            if ticker == 'DIS':
                continue

            for window in windows:
                rolling_corr = dis_returns.rolling(window).corr(returns[ticker])

                for date, corr_value in rolling_corr.items():
                    if pd.notna(corr_value):
                        correlation_rows.append({
                            'date': date,
                            'ticker_1': 'DIS',
                            'ticker_2': ticker,
                            'window': window,
                            'correlation': corr_value
                        })

        if correlation_rows:
            df = pd.DataFrame(correlation_rows)
            df.to_csv(f"{OUTPUT_DIR}/correlations.csv", index=False)
            log(f"✓ Correlation data saved: {len(df):,} rows")
            return df
    except Exception as e:
        log(f"  ✗ Error computing correlations: {e}")
        return None

def create_intraday_metrics():
    """Create intraday metrics from 1-minute Disney data"""
    log("\n" + "="*60)
    log("PHASE 7: Intraday Metrics (From Real Minute Data)")
    log("="*60)

    try:
        stock_file = f"{OUTPUT_DIR}/stock_data.csv"
        if not os.path.exists(stock_file):
            log("  No stock data file found")
            return None

        df = pd.read_csv(stock_file)
        minute_df = df[(df['ticker'] == 'DIS') & (df['interval'] == '1m')].copy()

        if len(minute_df) == 0:
            log("  No minute data found")
            return None

        log(f"  Processing {len(minute_df):,} minute bars...")

        # Ensure datetime column
        if 'Datetime' in minute_df.columns:
            minute_df['datetime'] = pd.to_datetime(minute_df['Datetime'])
        elif 'Date' in minute_df.columns:
            minute_df['datetime'] = pd.to_datetime(minute_df['Date'])
        else:
            return None

        minute_df['date'] = minute_df['datetime'].dt.date
        minute_df['hour'] = minute_df['datetime'].dt.hour
        minute_df['minute'] = minute_df['datetime'].dt.minute

        # Intraday VWAP
        minute_df['cum_volume'] = minute_df.groupby('date')['Volume'].cumsum()
        minute_df['cum_vol_price'] = minute_df.groupby('date').apply(
            lambda x: (x['Close'] * x['Volume']).cumsum()
        ).reset_index(level=0, drop=True)
        minute_df['VWAP'] = minute_df['cum_vol_price'] / minute_df['cum_volume']

        # Intraday range
        minute_df['intraday_high'] = minute_df.groupby('date')['High'].cummax()
        minute_df['intraday_low'] = minute_df.groupby('date')['Low'].cummin()
        minute_df['intraday_range'] = minute_df['intraday_high'] - minute_df['intraday_low']

        # Momentum (1-min, 5-min, 15-min)
        for period in [1, 5, 15, 30]:
            minute_df[f'momentum_{period}m'] = minute_df['Close'].pct_change(period)

        # Volume intensity
        minute_df['volume_zscore'] = (
            minute_df['Volume'] - minute_df['Volume'].rolling(60).mean()
        ) / minute_df['Volume'].rolling(60).std()

        minute_df.to_csv(f"{OUTPUT_DIR}/intraday_metrics.csv", index=False)
        log(f"✓ Intraday metrics saved: {len(minute_df):,} rows")
        return minute_df

    except Exception as e:
        log(f"  ✗ Error: {e}")
        return None

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("""
    ╔══════════════════════════════════════════════════════════════════════╗
    ║                                                                      ║
    ║   CMS ENTERPRISE SCALE DATA AGGREGATOR                               ║
    ║   Client: The Walt Disney Company (DIS)                              ║
    ║   Analyst: Mboya Jeffers                                             ║
    ║                                                                      ║
    ║   Pulling REAL data from:                                            ║
    ║   • Yahoo Finance (stocks, ETFs, options)                            ║
    ║   • SEC EDGAR (company facts, filings)                               ║
    ║   • FRED (economic indicators)                                       ║
    ║                                                                      ║
    ║   Target: Demonstrate 1M+ and 2M+ row processing capability          ║
    ║                                                                      ║
    ╚══════════════════════════════════════════════════════════════════════╝
    """)

    start_time = datetime.now()
    total_rows = 0
    summary = {}

    # Phase 1: Stock Data
    stock_df, stock_counts = pull_all_stock_data()
    if stock_df is not None:
        total_rows += len(stock_df)
        summary['Stock Data'] = len(stock_df)

    # Phase 2: Options Data
    options_df, options_counts = pull_all_options()
    if options_df is not None:
        total_rows += len(options_df)
        summary['Options Data'] = len(options_df)

    # Phase 3: SEC Company Facts
    sec_facts_df = pull_sec_company_facts(DISNEY_CIK)
    if sec_facts_df is not None:
        total_rows += len(sec_facts_df)
        summary['SEC Company Facts'] = len(sec_facts_df)

    # SEC Submissions
    sec_sub_df = pull_sec_submissions(DISNEY_CIK)
    if sec_sub_df is not None:
        total_rows += len(sec_sub_df)
        summary['SEC Submissions'] = len(sec_sub_df)

    # Phase 4: FRED Data
    fred_df = pull_fred_data()
    if fred_df is not None:
        total_rows += len(fred_df)
        summary['FRED Economic Data'] = len(fred_df)

    # Phase 5: Technical Indicators (derived from real data)
    tech_df = create_technical_indicators(stock_df)
    if tech_df is not None:
        total_rows += len(tech_df)
        summary['Technical Indicators'] = len(tech_df)

    # Phase 6: Correlations (calculated from real data)
    corr_df = create_cross_asset_correlations(stock_df)
    if corr_df is not None:
        total_rows += len(corr_df)
        summary['Cross-Asset Correlations'] = len(corr_df)

    # Phase 7: Intraday Metrics
    intraday_df = create_intraday_metrics()
    if intraday_df is not None:
        total_rows += len(intraday_df)
        summary['Intraday Metrics'] = len(intraday_df)

    # Final Summary
    duration = (datetime.now() - start_time).total_seconds()

    print(f"""

    ╔══════════════════════════════════════════════════════════════════════╗
    ║                      DATA PULL COMPLETE                              ║
    ╠══════════════════════════════════════════════════════════════════════╣
    """)

    for source, count in summary.items():
        print(f"    ║   {source:<30} {count:>15,} rows   ║")

    print(f"""    ╠══════════════════════════════════════════════════════════════════════╣
    ║   TOTAL REAL DATA ROWS:           {total_rows:>15,}        ║
    ║   Duration:                       {duration:>12.1f} sec        ║
    ║   Output:  {OUTPUT_DIR}
    ╚══════════════════════════════════════════════════════════════════════╝
    """)

    # Save summary
    summary_data = {
        'pull_date': datetime.now().isoformat(),
        'total_rows': total_rows,
        'sources': summary,
        'duration_seconds': duration
    }

    with open(f"{OUTPUT_DIR}/pull_summary.json", 'w') as f:
        json.dump(summary_data, f, indent=2)

    return total_rows

if __name__ == "__main__":
    main()
