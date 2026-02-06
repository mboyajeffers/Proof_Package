# Gaming Analytics Pipeline Architecture
## P05: Enterprise Data Engineering on Steam/PC Gaming Data

**Author:** Mboya Jeffers
**Version:** 1.0.0
**Pipeline:** `projects/v3_scale/P05_Gaming_Analytics/src/` (8M rows)

---

## High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    GAMING ANALYTICS PIPELINE (P05)                           │
│              Steam API + SteamSpy → Star Schema → Player KPIs               │
└─────────────────────────────────────────────────────────────────────────────┘

     ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
     │   Steam API  │      │   100K+      │      │    8M+       │
     │  + SteamSpy  │─────▶│   Games      │─────▶│    Records   │
     │   (Public)   │      │  Catalogued  │      │    Produced  │
     └──────────────┘      └──────────────┘      └──────────────┘
            │                                           │
            ▼                                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  EXTRACT                                                                    │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ SteamExtractor                                                        ││
│  │  • Steam Web API (api.steampowered.com)  → App list, details          ││
│  │  • SteamSpy API (steamspy.com/api.php)   → Owners, playtime           ││
│  │  • Store API (store.steampowered.com)    → Pricing, reviews           ││
│  │  • Rate limiting: SteamSpy 1 req/sec, Steam 100K/day                  ││
│  │  • No authentication required                                          ││
│  └────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  TRANSFORM                                                                  │
│  ┌──────────────────────┐   ┌──────────────────────────────────────────────┐│
│  │  DataCleaner         │   │  DataModeler                                 ││
│  │  • Price parsing     │   │  • Kimball star schema                       ││
│  │  • Date normalization│──▶│  • Game dimension (SCD Type 2)               ││
│  │  • Genre extraction  │   │  • Developer/Genre dimensions                ││
│  │  • Review scoring    │   │  • Game metrics fact table                   ││
│  │  • F2P flagging      │   │  • Bridge tables (genre, developer)          ││
│  └──────────────────────┘   └──────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  VALIDATE                                                                   │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ QualityGateRunner (4 Gates)                                            ││
│  │  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐             ││
│  │  │ Ownership │ │  Review   │ │ Genre     │ │ Pricing   │             ││
│  │  │ Validity  │ │Consistency│ │ Coverage  │ │ Logic     │             ││
│  │  │   (25%)   │ │   (25%)   │ │   (25%)   │ │   (25%)   │             ││
│  │  └───────────┘ └───────────┘ └───────────┘ └───────────┘             ││
│  └────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  LOAD                                                                       │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ KPICalculator (16 KPIs)                                                ││
│  │  • Player engagement (DAU/MAU, concurrent, session time)               ││
│  │  • Financial metrics (ARPU, revenue estimates, F2P ratio)              ││
│  │  • Review analytics (sentiment, positive rate, distribution)           ││
│  │  • Genre performance (top genres by owners, revenue)                   ││
│  └────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │ Outputs:                                                                ││
│  │  • dim_game.parquet              → Game master data                    ││
│  │  • dim_developer.parquet         → Developer entities                  ││
│  │  • dim_genre.parquet             → Genre categories                    ││
│  │  • dim_platform.parquet          → Platform support                    ││
│  │  • dim_date.parquet              → Date dimension                      ││
│  │  • fact_game_metrics.parquet     → Player/financial/review metrics     ││
│  │  • game_genre_bridge.parquet     → Many-to-many relationships          ││
│  │  • game_developer_bridge.parquet → Many-to-many relationships          ││
│  │  • kpis.json                     → Calculated KPIs                     ││
│  │  • pipeline_metrics.json         → Execution telemetry                 ││
│  └─────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Star Schema Data Model

```
┌───────────────────────────────────────────────────────────────────────────┐
│                         DIMENSIONAL MODEL                                  │
└───────────────────────────────────────────────────────────────────────────┘

┌───────────────────────┐    ┌────────────────────┐    ┌───────────────────┐
│     dim_game          │    │  dim_developer     │    │   dim_genre       │
├───────────────────────┤    ├────────────────────┤    ├───────────────────┤
│ game_id (PK)          │    │ developer_id (PK)  │    │ genre_id (PK)     │
│ steam_appid           │    │ developer_name     │    │ genre_name        │
│ title                 │    │ publisher_name     │    │ genre_category    │
│ release_date          │    │ total_games        │    │ is_indie          │
│ is_free               │    └────────────────────┘    └───────────────────┘
│ price_usd             │              │                        │
│ metacritic_score      │              │                        │
│ platforms             │    ┌─────────┴────────┐    ┌─────────┴──────────┐
│ supported_languages   │    │game_dev_bridge   │    │game_genre_bridge   │
└───────────────────────┘    ├──────────────────┤    ├────────────────────┤
          │                  │ game_id (FK)     │    │ game_id (FK)       │
          │ 1:N              │ developer_id (FK)│    │ genre_id (FK)      │
          ▼                  └──────────────────┘    └────────────────────┘
┌─────────────────────────────────────────────────────────────────────────┐
│                       fact_game_metrics                                   │
├─────────────────────────────────────────────────────────────────────────┤
│ fact_id (PK)           │ Generated MD5 hash                             │
│ game_id (FK)           │ → dim_game                                    │
│ date_id (FK)           │ → dim_date                                    │
│ owners_estimate        │ SteamSpy ownership estimate                    │
│ players_2weeks         │ Active players (2-week window)                 │
│ average_playtime       │ Average playtime (minutes)                     │
│ median_playtime        │ Median playtime (minutes)                      │
│ positive_reviews       │ Positive review count                          │
│ negative_reviews       │ Negative review count                          │
│ review_score           │ Positive / (positive + negative)               │
│ ccu_estimate           │ Concurrent users estimate                      │
│ revenue_estimate       │ Owners × price estimate                        │
│ peak_ccu              │ Peak concurrent users                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## KPI Calculations

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       GAMING KPI FRAMEWORK (16 KPIs)                        │
└─────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 1. PLAYER ENGAGEMENT                                                       │
├────────────────────────────────────────────────────────────────────────────┤
│   ┌───────────────────────────────────────────────────────────────────┐    │
│   │ active_player_rate = players_2weeks / owners_estimate             │    │
│   │ session_length     = average_playtime (minutes)                   │    │
│   │ retention_proxy    = median_playtime / average_playtime            │    │
│   │ ccu_ratio          = peak_ccu / owners_estimate                   │    │
│   └───────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 2. FINANCIAL METRICS                                                       │
├────────────────────────────────────────────────────────────────────────────┤
│   ┌───────────────────────────────────────────────────────────────────┐    │
│   │ revenue_estimate = owners_estimate * price_usd * 0.7 (Steam cut) │    │
│   │ arpu             = revenue_estimate / owners_estimate             │    │
│   │ f2p_ratio        = count(is_free=True) / total_games              │    │
│   └───────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 3. REVIEW ANALYTICS                                                        │
├────────────────────────────────────────────────────────────────────────────┤
│   ┌───────────────────────────────────────────────────────────────────┐    │
│   │ positive_rate    = positive / (positive + negative)               │    │
│   │ review_volume    = positive + negative                            │    │
│   │ sentiment_score  = (positive_rate - 0.5) * 2  (scaled -1 to 1)   │    │
│   └───────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 4. GENRE PERFORMANCE                                                       │
├────────────────────────────────────────────────────────────────────────────┤
│   • Top genres by total owners                                             │
│   • Top genres by average revenue                                          │
│   • Genre engagement (avg playtime by genre)                               │
│   • Genre review scores (avg sentiment by genre)                           │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## API Integration Pattern

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     STEAM + STEAMSPY API INTEGRATION                        │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Source 1: Steam Web API                                                    │
│  ─────────────────────────────────────────────────────────────────────────  │
│  GET https://api.steampowered.com/ISteamApps/GetAppList/v2/                 │
│  GET https://store.steampowered.com/api/appdetails?appids={id}              │
│                                                                             │
│  Returns: App list (100K+), game details, pricing, platforms                │
│  Rate limit: 100K calls/day                                                 │
│  Authentication: None required                                              │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Source 2: SteamSpy API                                                     │
│  ─────────────────────────────────────────────────────────────────────────  │
│  GET https://steamspy.com/api.php?request=appdetails&appid={id}             │
│  GET https://steamspy.com/api.php?request=top100in2weeks                    │
│                                                                             │
│  Returns: Ownership estimates, playtime, CCU, review counts                 │
│  Rate limit: 1 request/second (strict)                                      │
│  Authentication: None required                                              │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Extraction Strategy                                                        │
│  ─────────────────────────────────────────────────────────────────────────  │
│  1. Fetch full app list from Steam (100K+ games)                            │
│  2. Filter to games only (exclude DLC, tools, demos)                        │
│  3. Fetch details from SteamSpy (1/sec rate limit = ~3,600/hour)            │
│  4. Enrich with Store API (pricing, metacritic)                             │
│  5. Checkpoint every 1,000 games for resume capability                      │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| **Dual API extraction** | Steam for catalog, SteamSpy for player metrics - neither has both |
| **Bridge tables for genres** | Games have 1-5 genres; bridge avoids denormalization |
| **Revenue estimation** | owners × price × 0.7 (Steam 30% cut) - industry standard estimate |
| **SCD Type 2 for dim_game** | Games change price/title over time; track historical state |
| **MD5 surrogate keys** | Deterministic, reproducible keys for dimension tables |
| **Checkpoint resume** | Long extraction (hours); checkpointing prevents re-fetching |

---

## Execution

```bash
# Run test mode (sample data, fast)
python src/main.py --mode test

# Run full extraction (8M+ rows, several hours)
python src/main.py --mode full
```

---

*Architecture Document v1.0.0 | Mboya Jeffers | Feb 2026*
