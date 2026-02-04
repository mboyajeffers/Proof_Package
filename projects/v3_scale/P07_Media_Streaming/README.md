# P07: Media & Streaming Analytics Pipeline
## Enterprise-Scale Entertainment Data Platform

**Author:** Mboya Jeffers
**Target Scale:** 10M+ records
**Status:** Pipeline Ready

---

## Overview

Production-ready data pipeline for media and entertainment analytics using IMDB public datasets.

| Component | Description |
|-----------|-------------|
| **Extract** | IMDB datasets (titles, ratings, cast/crew) |
| **Transform** | Kimball star schema dimensional model |
| **Analyze** | Content performance, genre trends, talent analysis |
| **Evidence** | Quality gates, checksums, audit logs |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    P07 MEDIA ANALYTICS PIPELINE                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐        │
│  │   EXTRACT    │   │  TRANSFORM   │   │   ANALYZE    │        │
│  │              │   │              │   │              │        │
│  │ • IMDB Data  │──▶│ • dim_title  │──▶│ • Ratings    │        │
│  │ • Ratings    │   │ • dim_person │   │ • Genres     │        │
│  │ • Cast/Crew  │   │ • fact_*     │   │ • Talent     │        │
│  │ • 10M+ rows  │   │ • bridge     │   │ • Trends     │        │
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

| Source | URL | Data |
|--------|-----|------|
| title.basics.tsv.gz | datasets.imdbws.com | 10M+ titles |
| title.ratings.tsv.gz | datasets.imdbws.com | 1.3M+ rated titles |
| name.basics.tsv.gz | datasets.imdbws.com | 12M+ people |
| title.principals.tsv.gz | datasets.imdbws.com | 50M+ cast/crew credits |

---

## Star Schema

### Dimensions
- `dim_title` - Movie/TV show master data (SCD Type 2)
- `dim_person` - Actor/director/writer information
- `dim_genre` - Genre categories
- `dim_date` - Date dimension (by year)

### Facts
- `fact_ratings` - Rating aggregates (avg rating, votes, weighted)
- `fact_cast_crew` - Title-person relationships with roles

### Bridge Tables
- `title_genre_bridge` - Many-to-many title-genre relationships

---

## KPIs Calculated

| Category | Metrics |
|----------|---------|
| **Content Performance** | Average rating, vote count, weighted rating |
| **Genre Analysis** | Genre popularity, rating by genre, distribution |
| **Talent Analysis** | Prolific actors/directors, career statistics |
| **Time Trends** | Content by decade, production volume trends |
| **Rating Distribution** | Score buckets, top rated by type |

---

## Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run test mode (sample data)
python src/main.py --mode test --limit 1000

# Run full extraction (10M+ rows, several hours)
python src/main.py --mode full
```

---

## Project Structure

```
P07_Media_Streaming/
├── README.md
├── requirements.txt
├── src/
│   ├── main.py          # Pipeline orchestrator
│   ├── extract.py       # IMDB dataset extraction
│   ├── transform.py     # Star schema transformation
│   └── analytics.py     # KPI calculations
├── sql/
│   └── schema.sql       # PostgreSQL DDL
├── data/                # Output data (gitignored)
└── evidence/
    └── P07_evidence.json
```

---

## Engineering Patterns

- **Streaming Decompression**: Process gzip files without full memory load
- **Chunked Processing**: Handle large TSV files efficiently
- **Bayesian Weighted Rating**: Fair ranking accounting for vote count
- **Bridge Tables**: Properly model many-to-many (title-genre)
- **Surrogate Keys**: MD5-based keys for dimension tables
- **Evidence Trail**: Checksums and logs for auditability

---

## Sample Analytics Output

### Top Rated Movies (Weighted)
```
Title                    Year   Rating   Votes      Weighted
The Shawshank Redemption 1994   9.3      2,700,000  9.28
The Godfather            1972   9.2      1,900,000  9.17
The Dark Knight          2008   9.0      2,700,000  8.98
```

### Genre Performance
```
Genre       Titles    Avg Rating   Total Votes
Documentary 150,000   7.2          50M
Drama       300,000   6.8          200M
Comedy      250,000   6.3          180M
```

---

## Data Verification

All data sources are publicly accessible:
- IMDB Datasets: Free download at datasets.imdbws.com
- No authentication required
- Updated daily by IMDB

---

**Author:** Mboya Jeffers | MboyaJeffers9@gmail.com
