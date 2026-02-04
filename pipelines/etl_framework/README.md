# Enterprise ETL Framework

Scalable data extraction and transformation framework using Kimball dimensional modeling.

**Author:** Mboya Jeffers  
**Email:** MboyaJeffers9@gmail.com

## Overview

This framework provides enterprise-grade ETL capabilities for extracting data from public APIs and transforming it into optimized star schemas for analytics.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         ETL ORCHESTRATOR                            │
│    Job Scheduling │ Batch Execution │ Status Tracking │ Logging    │
└─────────────────────────────────────────────────────────────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        ▼                       ▼                       ▼
┌───────────────┐       ┌───────────────┐       ┌───────────────┐
│   EXTRACTOR   │       │  TRANSFORMER  │       │    WRITER     │
├───────────────┤       ├───────────────┤       ├───────────────┤
│ Rate Limiting │       │ Star Schema   │       │ Parquet       │
│ Caching       │       │ SCD Type 1/2  │       │ Compression   │
│ Retry Logic   │       │ Surrogate Keys│       │ Metadata      │
│ Pagination    │       │ Bridge Tables │       │ Checksums     │
└───────────────┘       └───────────────┘       └───────────────┘
```

## Features

### Core Infrastructure
- **BaseExtractor**: Abstract class with rate limiting, caching, retry logic
- **BaseTransformer**: Star schema transformation with SCD support
- **SurrogateKeys**: MD5-based deterministic key generation
- **ParquetWriter**: Columnar storage with Snappy compression
- **ETLRegistry**: Pipeline discovery and factory methods
- **ETLOrchestrator**: Job coordination and batch execution

### Industry Verticals

| Vertical | Data Source | Metrics |
|----------|-------------|---------|
| **Gaming** | Steam, SteamSpy | Players, ownership, reviews, playtime |
| **Crypto** | CoinGecko | Price, market cap, volume, sentiment |
| **Betting** | ESPN | Teams, standings, scores (NFL/NBA/MLB/NHL) |
| **Media** | TMDb | Movies, TV shows, ratings, genres |

## Directory Structure

```
etl_framework/
├── core/
│   ├── base_extractor.py      # Abstract extractor with rate limiting
│   ├── base_transformer.py    # Star schema transformation
│   ├── surrogate_keys.py      # MD5 key generation
│   ├── parquet_writer.py      # Parquet output with metadata
│   ├── etl_registry.py        # Pipeline discovery
│   └── etl_orchestrator.py    # Job coordination
├── extractors/
│   ├── steam_extractor.py     # Steam/SteamSpy API
│   ├── coingecko_extractor.py # CoinGecko API
│   ├── espn_extractor.py      # ESPN API
│   └── media_extractor.py     # TMDb API
├── schemas/
│   ├── gaming_schema.py       # Gaming star schema
│   ├── crypto_schema.py       # Crypto star schema
│   ├── betting_schema.py      # Betting star schema
│   └── media_schema.py        # Media star schema
├── transformers/
│   ├── gaming/transform.py    # Gaming transformer
│   ├── crypto/transform.py    # Crypto transformer
│   ├── betting/transform.py   # Betting transformer
│   └── media/transform.py     # Media transformer
├── config.py                  # API endpoints and rate limits
└── README.md
```

## Usage

### Run Single Pipeline
```python
from etl_framework.core import get_orchestrator

orchestrator = get_orchestrator()
result = orchestrator.run_pipeline('gaming', extractor_params={'limit': 100})

print(f"Extracted: {result.records_extracted}")
print(f"Tables: {result.tables_created}")
print(f"Rows: {result.total_rows}")
```

### Run All Pipelines
```python
from etl_framework.core import get_orchestrator

orchestrator = get_orchestrator()
results = orchestrator.run_all_pipelines()

for r in results:
    print(f"{r.pipeline}: {r.total_rows:,} rows in {r.total_duration_sec:.1f}s")
```

### Direct Extraction
```python
from etl_framework.extractors import SteamExtractor

extractor = SteamExtractor()
result = extractor.extract_top_games(count=100, sort_by='owners')

for game in result.data[:5]:
    print(f"{game['name']}: {game['owners_estimate']:,} owners")
```

## Star Schema Design

Each vertical produces a Kimball-style star schema:

### Dimensions
- Slowly Changing Dimension (SCD) Type 1 and Type 2 support
- Surrogate keys via MD5 hashing for deterministic joins
- Audit columns (_loaded_at, _source)

### Facts
- Grain documented per table
- Additive and semi-additive measures
- Degenerate dimensions for operational data

### Bridges
- Many-to-many relationships (e.g., game-genre, title-genre)
- Facilitates complex analytics queries

## Output Format

All tables are written as Parquet files with:
- Snappy compression
- Metadata sidecar files (.meta.json)
- Row counts and checksums
- Schema information

## Requirements

- Python 3.8+
- pandas
- pyarrow
- requests

## License

MIT License
