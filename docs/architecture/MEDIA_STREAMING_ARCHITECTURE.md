# Media & Streaming Analytics Pipeline Architecture
## P07: Enterprise Data Engineering on IMDB/TMDb Entertainment Data

**Author:** Mboya Jeffers
**Version:** 1.0.0
**Pipeline:** `projects/v3_scale/P07_Media_Streaming/src/` (10M rows)

---

## High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                  MEDIA & STREAMING ANALYTICS PIPELINE (P07)                  │
│           IMDB Bulk Datasets + TMDb API → Star Schema → Media KPIs          │
└─────────────────────────────────────────────────────────────────────────────┘

     ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
     │  IMDB Bulk   │      │   10M+       │      │   10M+       │
     │  TSV.GZ +    │─────▶│   Titles     │─────▶│   Records    │
     │  TMDb API    │      │   Processed  │      │   Produced   │
     └──────────────┘      └──────────────┘      └──────────────┘
            │                                           │
            ▼                                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  EXTRACT                                                                    │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ MediaExtractor                                                        ││
│  │  • datasets.imdbws.com/title.basics.tsv.gz    → 10M+ titles           ││
│  │  • datasets.imdbws.com/title.ratings.tsv.gz   → 1.3M+ ratings         ││
│  │  • datasets.imdbws.com/name.basics.tsv.gz     → 12M+ people           ││
│  │  • datasets.imdbws.com/title.principals.tsv.gz→ 50M+ credits          ││
│  │  • api.themoviedb.org/3/movie/{id}             → Supplemental data    ││
│  │  • Streaming decompression (no full memory load)                       ││
│  │  • No authentication for IMDB bulk (TMDb needs free API key)           ││
│  └────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  TRANSFORM                                                                  │
│  ┌──────────────────────┐   ┌──────────────────────────────────────────────┐│
│  │  DataCleaner         │   │  DataModeler                                 ││
│  │  • \N → NULL handling│   │  • Kimball star schema                       ││
│  │  • Genre splitting   │──▶│  • Title dimension (SCD Type 2)              ││
│  │  • Year parsing      │   │  • Person dimension                          ││
│  │  • Type filtering    │   │  • Genre dimension                           ││
│  │  • Runtime cleaning  │   │  • title_genre_bridge (many-to-many)         ││
│  │  • Duplicate removal │   │  • Rating + cast/crew fact tables             ││
│  └──────────────────────┘   └──────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  VALIDATE                                                                   │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ QualityGateRunner (5 Gates)                                            ││
│  │  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌──────────┐││
│  │  │  Rating   │ │  Genre    │ │  Title    │ │  Person   │ │ Bridge   │││
│  │  │  Range    │ │  Coverage │ │Completness│ │  Linking  │ │Integrity │││
│  │  │  (20%)    │ │   (20%)   │ │   (20%)   │ │   (20%)   │ │  (20%)   │││
│  │  └───────────┘ └───────────┘ └───────────┘ └───────────┘ └──────────┘││
│  └────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  LOAD                                                                       │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ KPICalculator (14 KPIs)                                                ││
│  │  • Content metrics (Bayesian weighted ratings, vote analysis)          ││
│  │  • Genre analysis (popularity, rating by genre, distribution)          ││
│  │  • Talent analysis (prolific actors/directors, career stats)           ││
│  │  • Temporal trends (content by decade, production volume)              ││
│  └────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │ Outputs:                                                                ││
│  │  • dim_title.parquet           → Movie/TV master data                  ││
│  │  • dim_person.parquet          → Actor/director/writer data            ││
│  │  • dim_genre.parquet           → Genre categories                      ││
│  │  • dim_date.parquet            → Date dimension (by year)              ││
│  │  • fact_ratings.parquet        → Rating aggregates                     ││
│  │  • fact_cast_crew.parquet      → Title-person roles                    ││
│  │  • title_genre_bridge.parquet  → Many-to-many title-genre              ││
│  │  • kpis.json                   → Calculated KPIs                       ││
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

┌───────────────────────┐                         ┌───────────────────────┐
│     dim_title         │                         │     dim_person        │
├───────────────────────┤                         ├───────────────────────┤
│ title_id (PK)         │                         │ person_id (PK)        │
│ tconst (IMDB ID)      │                         │ nconst (IMDB ID)      │
│ primary_title         │                         │ primary_name          │
│ original_title        │                         │ birth_year            │
│ title_type            │                         │ death_year            │
│ start_year            │                         │ primary_profession    │
│ end_year              │                         │ known_for_titles      │
│ runtime_minutes       │                         └───────────────────────┘
│ is_adult              │                                    │
└───────────────────────┘                                    │
          │                                                  │
          │          ┌─────────────────────┐                 │
          │          │ title_genre_bridge  │                 │
          │          ├─────────────────────┤                 │
          ├─────────▶│ title_id (FK)       │                 │
          │          │ genre_id (FK)       │                 │
          │          └─────────────────────┘                 │
          │                                                  │
          │ 1:N                                              │ 1:N
          ▼                                                  ▼
┌─────────────────────────────────┐    ┌──────────────────────────────────┐
│        fact_ratings             │    │       fact_cast_crew             │
├─────────────────────────────────┤    ├──────────────────────────────────┤
│ rating_id (PK)                  │    │ credit_id (PK)                   │
│ title_id (FK)                   │    │ title_id (FK)                    │
│ average_rating                  │    │ person_id (FK)                   │
│ num_votes                       │    │ ordering                         │
│ bayesian_weighted_rating        │    │ category (actor, director, etc.) │
│ vote_tier (low/med/high)        │    │ job                              │
│ rating_bucket (1-2, 3-4, ...)   │    │ characters                       │
└─────────────────────────────────┘    └──────────────────────────────────┘

┌───────────────────────┐
│    dim_genre           │
├───────────────────────┤
│ genre_id (PK)         │
│ genre_name            │
│ is_fiction             │
│ genre_group           │
└───────────────────────┘
```

---

## Bayesian Weighted Rating

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    BAYESIAN WEIGHTED RATING FORMULA                          │
│               (Fair ranking that accounts for vote count)                    │
└─────────────────────────────────────────────────────────────────────────────┘

    weighted_rating = (v / (v + m)) * R + (m / (v + m)) * C

    Where:
    ┌──────────────────────────────────────────────────────────────────┐
    │  R = average rating for this title                               │
    │  v = number of votes for this title                              │
    │  m = minimum votes required (25th percentile of vote counts)     │
    │  C = global mean rating across all titles                        │
    └──────────────────────────────────────────────────────────────────┘

    Effect:
    • Titles with few votes → pulled toward global average
    • Titles with many votes → weighted rating ≈ actual rating
    • Prevents low-vote titles from dominating rankings

    Example:
    • Movie A: Rating 9.5, 10 votes     → Weighted: ~7.2 (pulled to mean)
    • Movie B: Rating 8.5, 100K votes    → Weighted: ~8.5 (mostly unchanged)
```

---

## Bulk Data Extraction Pattern

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    IMDB BULK DATASET EXTRACTION                              │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  File 1: title.basics.tsv.gz (~750 MB uncompressed)                         │
│  ─────────────────────────────────────────────────────────────────────────  │
│  Fields: tconst, titleType, primaryTitle, originalTitle, isAdult,           │
│          startYear, endYear, runtimeMinutes, genres (comma-separated)       │
│  Rows: 10M+ titles (movies, TV series, shorts, episodes)                    │
│  Strategy: Stream gzip → chunked pandas (100K rows per chunk)               │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  File 2: title.ratings.tsv.gz (~20 MB uncompressed)                         │
│  ─────────────────────────────────────────────────────────────────────────  │
│  Fields: tconst, averageRating, numVotes                                    │
│  Rows: 1.3M+ rated titles                                                   │
│  Strategy: Full load (small enough for memory)                              │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  File 3: name.basics.tsv.gz (~700 MB uncompressed)                          │
│  ─────────────────────────────────────────────────────────────────────────  │
│  Fields: nconst, primaryName, birthYear, deathYear, primaryProfession,      │
│          knownForTitles                                                     │
│  Rows: 12M+ people                                                          │
│  Strategy: Stream gzip → chunked pandas (100K rows per chunk)               │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  File 4: title.principals.tsv.gz (~2 GB uncompressed)                       │
│  ─────────────────────────────────────────────────────────────────────────  │
│  Fields: tconst, ordering, nconst, category, job, characters                │
│  Rows: 50M+ cast/crew credits                                               │
│  Strategy: Stream gzip → chunked pandas → join with rated titles only       │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| **Bulk download over API** | IMDB bulk datasets are faster than per-title API calls for 10M+ titles |
| **Streaming decompression** | .tsv.gz files too large for memory; stream and chunk-process |
| **Bayesian weighted rating** | Fair comparison between titles with different vote counts |
| **Bridge table for genres** | Titles have multiple genres; bridge avoids denormalization |
| **Filter to rated titles** | Only 1.3M of 10M titles have ratings; focus on rated content |
| **TMDb supplemental** | IMDB bulk lacks images/revenue; TMDb fills gaps for top titles |
| **Title type filtering** | Focus on movies + TV series; exclude episodes, shorts |

---

## Execution

```bash
# Run test mode (1,000 titles, fast)
python src/main.py --mode test --limit 1000

# Run full extraction (10M+ rows, several hours)
python src/main.py --mode full
```

---

*Architecture Document v1.0.0 | Mboya Jeffers | Feb 2026*
