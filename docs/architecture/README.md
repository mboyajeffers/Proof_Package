# Pipeline Architecture Documentation

**Author:** Mboya Jeffers
**Portfolio:** Data Engineering Portfolio

---

## Overview

This folder contains visual architecture documentation for all production pipelines in the portfolio. These documents are designed to demonstrate enterprise-grade data engineering capabilities to technical interviewers and hiring managers.

---

## Documents

| Document | Pipeline | Key Highlights |
|----------|----------|----------------|
| [SEC_FINANCIAL_ARCHITECTURE.md](SEC_FINANCIAL_ARCHITECTURE.md) | P01 | 889-line pipeline, SEC EDGAR XBRL, 40+ companies, cohort benchmarking, financial ratios |
| [FEDERAL_AWARDS_ARCHITECTURE.md](FEDERAL_AWARDS_ARCHITECTURE.md) | P02 | 931-line pipeline, USAspending.gov API, star schema, 6 quality gates, HHI market concentration |
| [HEALTHCARE_QUALITY_ARCHITECTURE.md](HEALTHCARE_QUALITY_ARCHITECTURE.md) | P03 | 693-line pipeline, Data.Medicare.gov, 4 data sources, hospital benchmarking, ownership analysis |
| [ENERGY_GRID_ARCHITECTURE.md](ENERGY_GRID_ARCHITECTURE.md) | P04 | EIA-930 API, time-series ingestion, generation mix analysis, demand forecasting |
| [GAMING_ANALYTICS_ARCHITECTURE.md](GAMING_ANALYTICS_ARCHITECTURE.md) | P05 | Steam + SteamSpy APIs, 8M rows, player engagement metrics, Kimball star schema |
| [BETTING_SPORTS_ARCHITECTURE.md](BETTING_SPORTS_ARCHITECTURE.md) | P06 | ESPN API, multi-league (NFL/NBA/MLB/NHL), betting analytics, team performance |
| [MEDIA_STREAMING_ARCHITECTURE.md](MEDIA_STREAMING_ARCHITECTURE.md) | P07 | IMDB/TMDb datasets, 10M rows, Bayesian ratings, genre bridge tables |
| [CRYPTO_BLOCKCHAIN_ARCHITECTURE.md](CRYPTO_BLOCKCHAIN_ARCHITECTURE.md) | P08 | CoinGecko API, market structure analysis, tier bucketing, 30 KPIs |

---

## Architecture by Version

### v2 Foundation (P01-P04)

| Pipeline | Industry | Scale | API Pattern |
|----------|----------|-------|-------------|
| P01 SEC Financial | Finance | 1M facts | Multi-endpoint, XBRL parsing |
| P02 Federal Awards | Government | 1M awards | Cursor pagination, large datasets |
| P03 Healthcare Quality | Healthcare | 5M records | Multi-source merge, privacy rules |
| P04 Energy Grid | Energy | 500K readings | Time-series, hourly intervals |

### v3 Scale (P05-P08)

| Pipeline | Industry | Scale | API Pattern |
|----------|----------|-------|-------------|
| P05 Gaming Analytics | Gaming | 8M rows | Dual API (Steam + SteamSpy), rate limiting |
| P06 Betting Sports | Betting | 8M rows | Multi-league, seasonal data |
| P07 Media Streaming | Media | 10M rows | Bulk dataset + API hybrid |
| P08 Crypto Blockchain | Crypto | 5M rows | Market data, real-time pricing |

---

## Common Architecture Patterns

All pipelines share enterprise-grade patterns:

```
┌────────────────────────────────────────────────────────────┐
│                    SHARED PATTERNS                          │
├────────────────────────────────────────────────────────────┤
│ 1. Modular Architecture                                    │
│    DataClient → DataIngestion → DataCleaner →              │
│    DataModeler → QualityGateRunner → KPICalculator         │
├────────────────────────────────────────────────────────────┤
│ 2. Star Schema Modeling                                    │
│    Dimension tables + Central fact table                   │
├────────────────────────────────────────────────────────────┤
│ 3. Quality Gate Framework                                  │
│    Weighted scoring, pass thresholds, issue logging        │
├────────────────────────────────────────────────────────────┤
│ 4. Pipeline Telemetry                                      │
│    Duration, API calls, errors, quality scores             │
├────────────────────────────────────────────────────────────┤
│ 5. Idempotent Outputs                                      │
│    Deterministic file outputs, hash-based deduplication    │
└────────────────────────────────────────────────────────────┘
```

---

## Skills Demonstrated

| Category | Skills |
|----------|--------|
| **Data Engineering** | ETL pipeline design, API integration, star schema modeling |
| **Python** | Dataclasses, type hints, pandas, numpy, concurrent.futures |
| **Quality** | Data validation, null handling, duplicate detection, referential integrity |
| **APIs** | REST API consumption, rate limiting, pagination, error handling |
| **Domain Knowledge** | Finance (XBRL), Government (NAICS), Healthcare (HCAHPS), Energy (EIA), Gaming (Steam), Betting (ESPN), Media (IMDB), Crypto (CoinGecko) |
| **Best Practices** | Logging, metrics collection, modular design, documentation |

---

## How to Use These Documents

**For Interviewers:** These diagrams demonstrate system design thinking and production-grade architecture. The ASCII diagrams work in any environment (terminals, GitHub, plain text editors).

**For Code Review:** Cross-reference with actual pipeline code in each project's `src/` folder.

**For Portfolio Presentation:** Use as talking points during technical discussions about data engineering approaches.

---

*Architecture Documentation v2.0.0 | Mboya Jeffers*
