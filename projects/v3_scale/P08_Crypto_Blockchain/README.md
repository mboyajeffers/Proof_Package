# P08: Crypto & Blockchain Analytics Pipeline
## Enterprise-Scale Cryptocurrency Market Platform

**Author:** Mboya Jeffers
**Target Scale:** 5M+ records
**Status:** Pipeline Ready

---

## Overview

Production-ready data pipeline for cryptocurrency market analytics using CoinGecko API.

| Component | Description |
|-----------|-------------|
| **Extract** | CoinGecko API (market data, OHLCV, exchanges) |
| **Transform** | Kimball star schema dimensional model |
| **Analyze** | Market metrics, price performance, volatility |
| **Evidence** | Quality gates, checksums, audit logs |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    P08 CRYPTO ANALYTICS PIPELINE                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐        │
│  │   EXTRACT    │   │  TRANSFORM   │   │   ANALYZE    │        │
│  │              │   │              │   │              │        │
│  │ • CoinGecko  │──▶│ • dim_asset  │──▶│ • Market cap │        │
│  │ • Markets    │   │ • dim_exch   │   │ • Dominance  │        │
│  │ • OHLCV      │   │ • fact_price │   │ • Volatility │        │
│  │ • Exchanges  │   │ • fact_ohlcv │   │ • Categories │        │
│  └──────────────┘   └──────────────┘   └──────────────┘        │
│         │                  │                  │                 │
│         ▼                  ▼                  ▼                 │
│  ┌─────────────────────────────────────────────────────┐       │
│  │                    EVIDENCE LAYER                    │       │
│  │  • Extraction logs  • Row counts  • Checksums       │       │
│  └─────────────────────────────────────────────────────┘       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Data Sources

| Source | Endpoint | Data |
|--------|----------|------|
| CoinGecko Markets | /coins/markets | Price, volume, market cap |
| CoinGecko OHLC | /coins/{id}/ohlc | Historical OHLCV |
| CoinGecko Global | /global | Total market metrics |
| CoinGecko Exchanges | /exchanges | Exchange rankings |

**Free API:** No authentication required (rate limited to 10-30 calls/min)

---

## Star Schema

### Dimensions
- `dim_asset` - Cryptocurrency master data (SCD Type 2)
- `dim_exchange` - Exchange information
- `dim_chain` - Blockchain platforms
- `dim_date` - Date dimension

### Facts
- `fact_prices` - Daily price and market snapshot
- `fact_ohlcv` - Historical OHLCV time series
- `fact_global_metrics` - Global market aggregates

---

## KPIs Calculated

| Category | Metrics |
|----------|---------|
| **Market Overview** | Total market cap, 24h volume, active assets |
| **Dominance** | BTC dominance, ETH dominance, top 10 share |
| **Price Performance** | Top gainers, losers, volatility leaders |
| **Categories** | Stablecoins, DeFi, L1 platforms, tokens |
| **Market Cap Tiers** | Large/mid/small/micro cap distribution |
| **Exchange Analysis** | Volume distribution, trust scores |

---

## Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run test mode (sample data)
python src/main.py --mode test --limit 50

# Run full extraction (5M+ rows, several hours)
python src/main.py --mode full
```

---

## Project Structure

```
P08_Crypto_Blockchain/
├── README.md
├── requirements.txt
├── src/
│   ├── main.py          # Pipeline orchestrator
│   ├── extract.py       # CoinGecko API extraction
│   ├── transform.py     # Star schema transformation
│   └── analytics.py     # KPI calculations
├── sql/
│   └── schema.sql       # PostgreSQL DDL
├── data/                # Output data (gitignored)
└── evidence/
    └── P08_evidence.json
```

---

## Engineering Patterns

- **Rate Limiting**: Respects CoinGecko free tier (6s between calls)
- **Pagination**: Handles multi-page market data results
- **Asset Classification**: Automatic categorization (stablecoin, DeFi, etc.)
- **Tier Bucketing**: Large/mid/small/micro cap classification
- **Surrogate Keys**: MD5-based keys for dimension tables
- **Evidence Trail**: Checksums and logs for auditability

---

## Sample Analytics Output

### Market Overview
```
Metric              Value
Total Market Cap    $2.1T
24h Volume          $85B
BTC Dominance       48.5%
ETH Dominance       16.2%
Active Assets       15,000+
```

### Top Gainers (24h)
```
Symbol  Name       Price      Change    Market Cap
PEPE    Pepe       $0.00001   +45.2%    $4.2B
ARB     Arbitrum   $1.25      +22.1%    $3.1B
```

### Market Cap Tiers
```
Tier              Assets    Total Cap    Avg Change
Large (>$10B)     25        $1.8T        +2.1%
Mid ($1B-$10B)    85        $250B        +3.5%
Small ($100M-$1B) 350       $120B        +5.2%
```

---

## Data Verification

All data sources are publicly accessible:
- CoinGecko API: Free tier, no authentication
- Real-time and historical data available
- 15,000+ cryptocurrencies tracked

---

**Author:** Mboya Jeffers | MboyaJeffers9@gmail.com
