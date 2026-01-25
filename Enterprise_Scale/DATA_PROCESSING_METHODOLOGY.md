# CMS Enterprise Scale Data Processing Methodology
## Disney Market Analytics Project
### Author: Mboya Jeffers | Date: January 25, 2026

---

## Overview

This document describes the methodology used to pull, process, and analyze **7.7+ million rows of REAL market data** for The Walt Disney Company's competitive landscape analysis.

**Key Achievement:** 100% authentic data - no synthetic or simulated data used.

---

## Data Sources

### 1. Yahoo Finance API
- **Data Types:** OHLCV (Open, High, Low, Close, Volume), Options Chains
- **Granularities:** Daily (max history), Hourly (730 days), Minute (7 days)
- **Library:** `yfinance` Python package
- **Authentication:** None required (public API)

### 2. SEC EDGAR API
- **Data Types:** Company Facts (XBRL), SEC Submissions
- **Endpoint:** `https://data.sec.gov/api/xbrl/companyfacts/`
- **Disney CIK:** 0001744489
- **Authentication:** User-Agent header required

### 3. FRED (Federal Reserve Economic Data)
- **Data Types:** Economic indicators (GDP, unemployment, interest rates, etc.)
- **Endpoint:** `https://api.stlouisfed.org/fred/`
- **API Key:** Required (free registration)
- **Series Pulled:** 25 economic indicators

---

## Securities Universe (46 Tickers)

### Disney Primary
- **DIS** - The Walt Disney Company

### ETFs Holding Disney
- SPY, DIA, XLY, VTI, VOO, IYC, PEJ, FDIS

### Media Competitors
- CMCSA, NFLX, WBD, FOXA, LYV, AMC, CNK, SONY, SIRI, IMAX, SPOT, TME, BILI, IQ

### Tech Partners/Suppliers
- AAPL, GOOGL, AMZN, ROKU, META, TTD, MGNI, PUBM

### Distribution
- T, VZ, TMUS, CHTR, SATS

### Hospitality (Theme Park Competitors)
- MAR, HLT, H, WH, FUN

### Retail (Merchandise)
- TGT, WMT, COST, DG

---

## Processing Pipeline

### Phase 1: Initial Data Pull (`pull_disney_real_data.py`)

```
Step 1: Disney Primary Data
├── DIS daily (1962-present): 16,123 rows
├── DIS hourly (730 days): 5,078 rows
└── DIS minute (7 days): 2,712 rows

Step 2: ETF Data
└── 8 ETFs × daily history: ~47,000 rows

Step 3: Competitor Data
└── 9 competitors × daily history: ~45,000 rows

Step 4: Supplier/Partner Data
└── 7 tickers × daily history: ~52,000 rows

Step 5: Options Chains
└── DIS + 3 competitors: 8,737 contracts

Step 6: SEC EDGAR Data
├── Company Facts: 15,345 rows
└── Submissions: 1,000 rows

Step 7: FRED Economic Data
└── 5 series: 1,702 observations

Step 8: Technical Indicators (computed)
└── 24 tickers × 20+ indicators: 162,694 rows

Step 9: Cross-Asset Correlations
└── Rolling correlations (8 windows): 544,728 rows

PHASE 1 TOTAL: 899,612 rows
```

### Phase 2: Data Expansion (`expand_disney_data.py`)

```
Step 1: Hourly Data Expansion
└── 24 tickers × 730 days × hourly: 121,778 rows

Step 2: Minute Data Expansion
└── 15 key tickers × 7 days × minute: 33,152 rows

Step 3: Additional Daily Tickers
└── 23 new tickers × full history: 127,565 rows

Step 4: Expanded FRED Data
└── 20 additional series: 93,217 rows

Step 5: More Options Data
└── 6 major tickers (AAPL, AMZN, GOOGL, META, MSFT, SPY): 14,172 contracts

Step 6: Expanded Correlations
└── All 46 tickers × 8 windows × full history: 6,320,612 rows

Step 7: Technical Indicators (hourly)
└── 24 tickers × hourly data: 121,778 rows

PHASE 2 TOTAL: 6,832,274 rows
```

### Grand Total: 7,731,886 rows (618.4 MB)

---

## Data Files Produced

| File | Rows | Size | Description |
|------|------|------|-------------|
| correlations_expanded.csv | 6,320,612 | 346.7 MB | Cross-asset rolling correlations |
| technical_indicators.csv | 162,694 | 104.4 MB | Daily technical analysis |
| technical_expanded.csv | 121,778 | 45.6 MB | Hourly technical analysis |
| correlations.csv | 544,728 | 29.7 MB | Base correlation matrix |
| stock_data.csv | 162,694 | 19.5 MB | Primary stock data |
| daily_data_expanded.csv | 127,565 | 14.8 MB | Expanded daily data |
| hourly_data_expanded.csv | 121,778 | 14.1 MB | Hourly OHLCV |
| fred_expanded.csv | 93,217 | 4.3 MB | Economic indicators |
| minute_data_expanded.csv | 35,864 | 4.8 MB | Minute-level data |
| options_expanded.csv | 14,172 | 2.3 MB | Options chains |
| sec_company_facts.csv | 15,345 | 2.3 MB | SEC XBRL data |
| options_data.csv | 8,737 | 1.4 MB | Base options data |
| intraday_metrics.csv | 2,712 | 1.0 MB | Intraday analysis |
| fred_data.csv | 1,702 | 0.1 MB | Base FRED data |
| sec_submissions.csv | 1,000 | 0.2 MB | SEC filings |

---

## Technical Indicators Computed

### Moving Averages
- SMA: 5, 10, 20, 50, 100, 200 periods
- EMA: 5, 10, 20, 50, 100, 200 periods

### Volatility
- Rolling Standard Deviation: 10, 20, 30 periods
- Bollinger Bands: 20-period, 2 standard deviations
- ATR (Average True Range): 14-period

### Momentum
- RSI (Relative Strength Index): 14-period
- MACD: 12/26/9 configuration
- Price Momentum: 1, 5, 10, 20, 60 periods

### Volume
- Volume SMA: 20-period
- Volume Ratio (current vs SMA)
- VWAP (intraday)

### Correlation Analysis
- Rolling windows: 10, 20, 30, 60, 90, 120, 180, 252 days
- All unique asset pairs (1,035 pairs)

---

## Memory Management

### Chunked Processing Strategy
```python
# Large files processed in chunks to prevent memory overflow
chunk_size = 100000  # 100K rows per chunk

for chunk in pd.read_csv(filepath, chunksize=chunk_size):
    # Process chunk
    results.append(process(chunk))

# Combine results
final = pd.concat(results)
```

### Memory Optimization
- Used `low_memory=False` for mixed-type columns
- Dropped unnecessary columns after computation
- Saved intermediate results to disk
- Peak memory usage: <2GB

---

## Report Generation (`generate_enterprise_reports.py`)

### Report 1: 1M+ Row Correlation Analysis
- Focus: Cross-asset correlations
- Key insight: DIS-XLY correlation of 0.87
- Pages: 4

### Report 2: 7M+ Row Comprehensive Analysis
- Focus: Full data summary across all sources
- Key metrics: All KPIs and data inventory
- Pages: 5

### Report 3: Enterprise Scale Certificate
- Focus: Processing proof and data authenticity
- Includes: Complete file manifest with row counts
- Pages: 3

---

## Execution Commands

```bash
# Step 1: Pull initial data
python3 pull_disney_real_data.py

# Step 2: Expand data to 2.5M+ rows
python3 expand_disney_data.py

# Step 3: Generate PDF reports
python3 generate_enterprise_reports.py
```

---

## Processing Times

| Phase | Duration | Rows/Second |
|-------|----------|-------------|
| Initial Pull | 36 seconds | 24,989 |
| Expansion | 114 seconds | 59,932 |
| Report Generation | 15 seconds | - |
| **Total** | **165 seconds** | **46,860** |

---

## Data Authenticity Statement

All data in this project is **100% authentic**:

1. **Market Data:** Sourced directly from Yahoo Finance API (real OHLCV prices)
2. **SEC Data:** Official filings from SEC EDGAR (government source)
3. **Economic Data:** Federal Reserve Economic Data (government source)
4. **Options Data:** Real-time options chains from Yahoo Finance

**No synthetic, simulated, or generated data was used.**

All computed metrics (correlations, technical indicators) are mathematically derived from the authentic source data.

---

## Dependencies

```
pandas>=2.0
numpy>=1.24
yfinance>=0.2
requests>=2.28
weasyprint>=60.0
jinja2>=3.1
```

---

## File Structure

```
Enterprise_Scale/
├── data/                                    # Raw data files (618MB)
│   ├── stock_data.csv
│   ├── correlations_expanded.csv
│   ├── technical_indicators.csv
│   └── ... (15 files total)
├── pull_disney_real_data.py                 # Phase 1 data pull
├── expand_disney_data.py                    # Phase 2 expansion
├── generate_enterprise_reports.py           # PDF report generator
├── CMS_Disney_1M_Correlation_Analysis.pdf   # Output report 1
├── CMS_Disney_7M_Comprehensive_Analysis.pdf # Output report 2
├── CMS_Disney_Enterprise_Scale_Certificate.pdf # Output report 3
└── DATA_PROCESSING_METHODOLOGY.md           # This file
```

---

*Document created: January 25, 2026*
*Author: Mboya Jeffers, Data Engineer & Analyst*
