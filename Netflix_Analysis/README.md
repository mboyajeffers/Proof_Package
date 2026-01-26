# Netflix Streaming Wars Market Analysis

**A portfolio project demonstrating enterprise-scale data engineering with 23.6M+ rows of authentic market data.**

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://python.org)
[![Data Scale](https://img.shields.io/badge/Scale-23.6M%20rows-red.svg)](#data-scale)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Overview

This project analyzes the **"Streaming Wars"** competitive landscape by building a comprehensive correlation analysis pipeline. By examining how Netflix (NFLX) correlates with streaming competitors, tech platforms, content creators, and telecom companies, we can understand market dynamics and investor sentiment.

**Key Numbers:**
| Metric | Value |
|--------|-------|
| Total Rows | 23,622,884 |
| Data Volume | 1.5 GB |
| Securities Analyzed | 39 tickers |
| Correlation Pairs | 741 unique combinations |
| API Sources | 4 |

---

## The Streaming Wars Universe

### Securities Analyzed

```
Netflix Primary:       NFLX

Streaming Competitors: DIS (Disney+), WBD (Max), CMCSA (Peacock),
                       AMZN (Prime), AAPL (Apple TV+), ROKU, FUBO

Tech Platforms:        GOOGL, META, MSFT, CRM, ADBE, SPOT

Content/Media:         SONY, FOXA, NWSA, AMC, CNK, IMAX, LYV

Telecom:               T, VZ, TMUS, CHTR, CABO

Market ETFs:           SPY, QQQ, IWM, VTI, VOO

Streaming ETFs:        VGT, XLC, VOX, ARKK, SOCL, GAMR
```

---

## Data Architecture

### Output Files

| File | Rows | Size | Description |
|------|------|------|-------------|
| `correlations_expanded.csv` | 21,307,845 | 1.17 GB | Full correlation matrix |
| `correlations.csv` | 1,290,016 | 72 MB | NFLX-focused correlations |
| `technical_indicators.csv` | 231,558 | 148 MB | Daily technicals |
| `stock_data.csv` | 231,558 | 28 MB | Primary OHLCV |
| `technical_expanded.csv` | 172,607 | 69 MB | Hourly technicals |
| `hourly_data_expanded.csv` | 172,607 | 20 MB | Hourly OHLCV |
| `fred_expanded.csv` | 79,314 | 3 MB | Economic indicators |
| `options_expanded.csv` | 36,964 | 6 MB | Options chains |
| `minute_data_expanded.csv` | 27,052 | 3 MB | Minute-level data |
| `sec_company_facts.csv` | 23,206 | 4 MB | SEC XBRL data |

---

## Key Insights

### Correlation Findings

| Pair | Correlation | Insight |
|------|-------------|---------|
| NFLX - QQQ | 0.80-0.90 | Netflix trades as a tech stock |
| NFLX - ROKU | 0.75-0.85 | Strong streaming sentiment linkage |
| NFLX - DIS | 0.65-0.75 | Streaming competitors move together |
| NFLX - AMZN | 0.70-0.80 | Tech/streaming hybrid correlation |
| NFLX - T | 0.25-0.35 | Telecom offers diversification |

### Business Applications

1. **Portfolio Construction**: Build diversified media/tech portfolios understanding correlation structure
2. **Risk Management**: High NFLX-QQQ correlation means tech sector hedges work for Netflix
3. **Pairs Trading**: Correlated streaming stocks move together on industry news
4. **Sector Rotation**: Low telecom correlation offers defensive positioning

---

## Quick Start

### Prerequisites
- Python 3.10+
- ~4 GB available RAM
- Internet connection

### Installation

```bash
git clone https://github.com/YOUR_USERNAME/netflix-streaming-analysis.git
cd netflix-streaming-analysis
pip install -r requirements.txt
```

### Run the Pipeline

```bash
# Step 1: Pull initial data (~1.8M rows)
python pull_netflix_data.py

# Step 2: Expand to 23M+ rows
python expand_netflix_data.py

# Step 3: Generate PDF reports
python generate_netflix_reports.py
```

---

## Technical Implementation

### Memory-Efficient Processing

```python
# Chunked correlation computation
for i, ticker1 in enumerate(tickers):
    for ticker2 in tickers[i+1:]:
        for window in [10, 20, 30, 60, 90, 120, 180, 252]:
            rolling_corr = returns[ticker1].rolling(window).corr(returns[ticker2])
            # Write incrementally to avoid memory overflow
```

### Rolling Windows

Correlations computed over 8 time horizons:
- Short-term: 10, 20, 30 days
- Medium-term: 60, 90, 120 days
- Long-term: 180, 252 days (trading year)

---

## Requirements

```
pandas>=2.0
numpy>=1.24
yfinance>=0.2
requests>=2.28
weasyprint>=60.0
jinja2>=3.1
```

---

## Project Structure

```
netflix-streaming-analysis/
├── README.md
├── requirements.txt
│
├── pull_netflix_data.py           # Phase 1: Initial data pull
├── expand_netflix_data.py         # Phase 2: Scale expansion
├── generate_netflix_reports.py    # Phase 3: PDF generation
│
├── data/                          # Output data files (1.5 GB)
│   ├── correlations_expanded.csv  # 21.3M rows
│   ├── stock_data.csv
│   └── ... (14 files)
│
└── reports/
    ├── Netflix_Correlation_Analysis.pdf
    ├── Netflix_Comprehensive_Analysis.pdf
    └── Netflix_Processing_Summary.pdf
```

---

## Skills Demonstrated

| Category | Skills |
|----------|--------|
| **Data Engineering** | Multi-source API integration, ETL pipelines |
| **Scale** | 23.6M row processing, memory-efficient operations |
| **Python** | pandas, numpy, requests, yfinance |
| **Financial Domain** | Correlation analysis, streaming industry knowledge |
| **Automation** | End-to-end pipeline orchestration |

---

## About

**Author:** Mboya Jeffers
**Portfolio:** [cleanmetricsstudios.com](https://cleanmetricsstudios.com)
**Email:** MboyaJeffers9@gmail.com
**LinkedIn:** [linkedin.com/in/mboya-jeffers-6377ba325](https://linkedin.com/in/mboya-jeffers-6377ba325)

I'm looking for Data Engineer or Analytics Engineer roles where I can apply these patterns to real business problems.

---

## License

MIT License
