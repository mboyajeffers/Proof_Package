# Crypto & Blockchain Analytics Pipeline Architecture
## P08: Enterprise Data Engineering on CoinGecko Market Data

**Author:** Mboya Jeffers
**Version:** 1.0.0
**Pipeline:** `projects/v3_scale/P08_Crypto_Blockchain/src/` (5M rows)

---

## High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                CRYPTO & BLOCKCHAIN ANALYTICS PIPELINE (P08)                  │
│           CoinGecko API → Star Schema → Market Structure KPIs               │
└─────────────────────────────────────────────────────────────────────────────┘

     ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
     │  CoinGecko   │      │   15,000+    │      │    5M+       │
     │  API (Free)  │─────▶│   Assets     │─────▶│    Records   │
     │              │      │   Tracked    │      │    Produced  │
     └──────────────┘      └──────────────┘      └──────────────┘
            │                                           │
            ▼                                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  EXTRACT                                                                    │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ CoinGeckoExtractor                                                    ││
│  │  • /coins/markets?vs_currency=usd    → Price, volume, market cap      ││
│  │  • /coins/{id}/ohlc?days=365          → Historical OHLCV              ││
│  │  • /global                            → Total market metrics          ││
│  │  • /exchanges                         → Exchange rankings             ││
│  │  • Rate limiting: 6s between calls (10-30 calls/min free tier)        ││
│  │  • No authentication required                                          ││
│  │  • Pagination: 250 coins per page                                      ││
│  └────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  TRANSFORM                                                                  │
│  ┌──────────────────────┐   ┌──────────────────────────────────────────────┐│
│  │  DataCleaner         │   │  DataModeler                                 ││
│  │  • Price normalization│  │  • Kimball star schema                       ││
│  │  • Volume validation │──▶│  • Asset dimension (SCD Type 2)              ││
│  │  • Market cap tiers  │   │  • Exchange dimension                        ││
│  │  • Category tagging  │   │  • Chain dimension                           ││
│  │  • Stablecoin filter │   │  • Price/OHLCV/global fact tables            ││
│  │  • Null handling     │   │  • Market cap tier bucketing                 ││
│  └──────────────────────┘   └──────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  VALIDATE                                                                   │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ QualityGateRunner (4 Gates)                                            ││
│  │  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐             ││
│  │  │  Price    │ │  Volume   │ │ Market Cap│ │  Exchange │             ││
│  │  │ Validity  │ │Consistency│ │  Tiers    │ │ Coverage  │             ││
│  │  │   (25%)   │ │   (25%)   │ │   (25%)   │ │   (25%)   │             ││
│  │  └───────────┘ └───────────┘ └───────────┘ └───────────┘             ││
│  └────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  LOAD                                                                       │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ KPICalculator (30 KPIs - Largest in Portfolio)                         ││
│  │  • Market overview (total cap, volume, active assets)                  ││
│  │  • Dominance metrics (BTC/ETH dominance, top 10 share)                ││
│  │  • Price performance (gainers, losers, volatility leaders)             ││
│  │  • Category analysis (stablecoins, DeFi, L1, tokens)                  ││
│  │  • Market cap tiers (large/mid/small/micro distribution)               ││
│  │  • Exchange analysis (volume distribution, trust scores)               ││
│  └────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │ Outputs:                                                                ││
│  │  • dim_asset.parquet           → Cryptocurrency master data            ││
│  │  • dim_exchange.parquet        → Exchange information                  ││
│  │  • dim_chain.parquet           → Blockchain platforms                  ││
│  │  • dim_date.parquet            → Date dimension                        ││
│  │  • fact_prices.parquet         → Daily price snapshots                 ││
│  │  • fact_ohlcv.parquet          → Historical OHLCV time series          ││
│  │  • fact_global_metrics.parquet → Global market aggregates              ││
│  │  • kpis.json                   → 30 calculated KPIs                    ││
│  │  • pipeline_metrics.json       → Execution telemetry                   ││
│  └─────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Star Schema Data Model

```
┌───────────────────────────────────────────────────────────────────────────┐
│                         DIMENSIONAL MODEL                                  │
└───────────────────────────────────────────────────────────────────────────┘

┌───────────────────────┐   ┌───────────────────────┐   ┌──────────────────┐
│     dim_asset         │   │   dim_exchange         │   │   dim_chain      │
├───────────────────────┤   ├───────────────────────┤   ├──────────────────┤
│ asset_id (PK)         │   │ exchange_id (PK)       │   │ chain_id (PK)    │
│ coingecko_id          │   │ exchange_name          │   │ chain_name       │
│ symbol                │   │ country                │   │ consensus        │
│ name                  │   │ year_established       │   │ launch_year      │
│ category              │   │ trust_score            │   │ native_token     │
│ market_cap_tier       │   │ centralized            │   │ tps_estimate     │
│ is_stablecoin         │   │ trade_volume_24h_btc   │   └──────────────────┘
│ platform_chain        │   └───────────────────────┘
│ launch_date           │
│ all_time_high         │
│ ath_date              │
└───────────────────────┘
          │
          │ 1:N
          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          fact_prices                                      │
├─────────────────────────────────────────────────────────────────────────┤
│ price_id (PK)          │ Generated MD5 hash                             │
│ asset_id (FK)          │ → dim_asset                                   │
│ date_id (FK)           │ → dim_date                                    │
│ current_price          │ USD price                                      │
│ market_cap             │ Total market capitalization                     │
│ fully_diluted_valuation│ Max supply × price                             │
│ total_volume           │ 24h trading volume                              │
│ high_24h               │ 24h high                                        │
│ low_24h                │ 24h low                                         │
│ price_change_24h       │ Absolute price change                           │
│ price_change_pct_24h   │ Percentage price change                         │
│ circulating_supply     │ Coins in circulation                            │
│ total_supply           │ Total coins created                             │
│ max_supply             │ Maximum possible supply                         │
│ ath_change_pct         │ % from all-time high                            │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                          fact_ohlcv                                       │
├─────────────────────────────────────────────────────────────────────────┤
│ ohlcv_id (PK)          │ Generated MD5 hash                             │
│ asset_id (FK)          │ → dim_asset                                   │
│ date_id (FK)           │ → dim_date                                    │
│ open                   │ Period open price                               │
│ high                   │ Period high                                     │
│ low                    │ Period low                                      │
│ close                  │ Period close price                              │
│ volume                 │ Period volume                                   │
│ daily_return           │ (close - open) / open                           │
│ volatility             │ (high - low) / open                             │
│ is_positive_day        │ Boolean: close > open                           │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Market Cap Tier Classification

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MARKET CAP TIER BUCKETING                                 │
└─────────────────────────────────────────────────────────────────────────────┘

              ┌──────────────────┐
              │   market_cap     │
              └────────┬─────────┘
                       │
         ┌─────────────┼──────────────┬──────────────┐
         ▼             ▼              ▼              ▼
    > $10B        $1B-$10B      $100M-$1B        < $100M
         │             │              │              │
         ▼             ▼              ▼              ▼
     LARGE CAP     MID CAP      SMALL CAP     MICRO CAP
     (~25 assets)  (~85 assets) (~350 assets) (~14,500+)

    Examples:
    • Large: BTC ($1.3T), ETH ($300B), BNB ($50B)
    • Mid: AVAX ($8B), LINK ($7B), DOT ($6B)
    • Small: GMX ($500M), LIDO ($400M)
    • Micro: Long tail of 14,500+ tokens
```

---

## KPI Calculations

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                  CRYPTO KPI FRAMEWORK (30 KPIs - Largest)                    │
└─────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 1. MARKET OVERVIEW (6 KPIs)                                                │
├────────────────────────────────────────────────────────────────────────────┤
│   total_market_cap, total_24h_volume, active_assets,                       │
│   total_exchanges, market_cap_change_24h, volume_change_24h                │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 2. DOMINANCE METRICS (5 KPIs)                                              │
├────────────────────────────────────────────────────────────────────────────┤
│   ┌───────────────────────────────────────────────────────────────────┐    │
│   │ btc_dominance    = btc_market_cap / total_market_cap              │    │
│   │ eth_dominance    = eth_market_cap / total_market_cap              │    │
│   │ top_10_share     = sum(top_10_caps) / total_market_cap            │    │
│   │ stablecoin_share = sum(stablecoin_caps) / total_market_cap        │    │
│   │ defi_share       = sum(defi_caps) / total_market_cap              │    │
│   └───────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 3. PRICE PERFORMANCE (6 KPIs)                                              │
├────────────────────────────────────────────────────────────────────────────┤
│   top_gainers_24h, top_losers_24h, avg_change_24h,                         │
│   volatility_leaders, highest_volume, ath_proximity                        │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 4. CATEGORY ANALYSIS (5 KPIs)                                              │
├────────────────────────────────────────────────────────────────────────────┤
│   stablecoin_volume, defi_tvl_estimate, l1_platform_count,                 │
│   meme_coin_cap, token_vs_coin_ratio                                       │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 5. MARKET CAP TIERS (4 KPIs)                                               │
├────────────────────────────────────────────────────────────────────────────┤
│   large_cap_total, mid_cap_total, small_cap_total, micro_cap_count         │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 6. EXCHANGE ANALYSIS (4 KPIs)                                              │
├────────────────────────────────────────────────────────────────────────────┤
│   top_exchange_volume, exchange_concentration (HHI),                        │
│   centralized_vs_dex_ratio, avg_trust_score                                │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## API Integration Pattern

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      COINGECKO API INTEGRATION                              │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Endpoint 1: Markets                                                        │
│  GET https://api.coingecko.com/api/v3/coins/markets                         │
│  Parameters: vs_currency=usd, order=market_cap_desc, per_page=250, page=N   │
│  Returns: Price, market cap, volume, supply for top coins                   │
│  Pages: ~60 pages for all 15,000+ assets                                    │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Endpoint 2: OHLC History                                                   │
│  GET https://api.coingecko.com/api/v3/coins/{id}/ohlc?days=365              │
│  Returns: Open, High, Low, Close for past year                              │
│  Fetched for: Top 100 coins by market cap                                   │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Rate Limiting Strategy                                                     │
│  ─────────────────────────────────────────────────────────────────────────  │
│  • Free tier: 10-30 calls/minute                                            │
│  • Pipeline delay: 6 seconds between calls                                  │
│  • Exponential backoff on 429 responses                                     │
│  • Checkpoint every 10 pages for resume capability                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| **Market cap tier bucketing** | Standard classification used by institutional investors |
| **Stablecoin flagging** | Stablecoins distort market metrics; filter-able for analysis |
| **OHLCV for top 100 only** | Rate limits make full history for 15K+ assets impractical |
| **Daily return calculation** | Standard volatility metric for financial analysis |
| **Exchange HHI** | Herfindahl-Hirschman Index measures exchange concentration |
| **6-second delay** | Conservative rate limiting to avoid CoinGecko bans |
| **Category auto-tagging** | Classify assets by name/description patterns (DeFi, L1, meme) |

---

## Execution

```bash
# Run test mode (top 50 coins, fast)
python src/main.py --mode test --limit 50

# Run full extraction (5M+ rows, several hours)
python src/main.py --mode full
```

---

*Architecture Document v1.0.0 | Mboya Jeffers | Feb 2026*
