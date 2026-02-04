# P06: Betting & Sports Analytics Pipeline
## Enterprise-Scale Sports Data Platform

**Author:** Mboya Jeffers
**Target Scale:** 8M+ records
**Status:** Pipeline Ready

---

## Overview

Production-ready data pipeline for sports betting analytics using ESPN and public sports data APIs.

| Component | Description |
|-----------|-------------|
| **Extract** | ESPN API, historical game data |
| **Transform** | Kimball star schema dimensional model |
| **Analyze** | Team performance, betting trends, home advantage |
| **Evidence** | Quality gates, checksums, audit logs |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    P06 BETTING ANALYTICS PIPELINE               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐        │
│  │   EXTRACT    │   │  TRANSFORM   │   │   ANALYZE    │        │
│  │              │   │              │   │              │        │
│  │ • ESPN API   │──▶│ • dim_team   │──▶│ • Win rates  │        │
│  │ • Scores     │   │ • dim_player │   │ • ATS trends │        │
│  │ • Box scores │   │ • fact_games │   │ • O/U trends │        │
│  │ • Schedules  │   │ • fact_odds  │   │ • Home adv.  │        │
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
| ESPN API | site.api.espn.com | Scores, teams, schedules |
| ESPN Boxscore | site.api.espn.com/summary | Player stats, box scores |

**Leagues Supported:** NFL, NBA, MLB, NHL, NCAA Football, NCAA Basketball

---

## Star Schema

### Dimensions
- `dim_team` - Team master data (SCD Type 2)
- `dim_player` - Player information
- `dim_league` - League/sport data
- `dim_venue` - Stadium/arena data
- `dim_season` - Season information
- `dim_date` - Date dimension

### Facts
- `fact_games` - Game results (scores, attendance, margins)
- `fact_odds` - Betting lines (spread, moneyline, over/under)
- `fact_player_stats` - Player box scores

---

## KPIs Calculated

| Category | Metrics |
|----------|---------|
| **Team Performance** | Win/loss records, point differentials, home/away splits |
| **Betting Trends** | ATS records, cover percentages, spread accuracy |
| **Over/Under** | Total trends, over percentage, average lines |
| **Home Advantage** | Home win rate, average margin, by league |
| **Scoring** | Average totals, distribution, close game percentage |

---

## Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run test mode (sample data)
python src/main.py --mode test

# Run full extraction (8M+ rows, several hours)
python src/main.py --mode full
```

---

## Project Structure

```
P06_Betting_Sports/
├── README.md
├── requirements.txt
├── src/
│   ├── main.py          # Pipeline orchestrator
│   ├── extract.py       # ESPN API extraction
│   ├── transform.py     # Star schema transformation
│   └── analytics.py     # KPI calculations
├── sql/
│   └── schema.sql       # PostgreSQL DDL
├── data/                # Output data (gitignored)
└── evidence/
    └── P06_evidence.json
```

---

## Engineering Patterns

- **Rate Limiting**: Exponential backoff for API throttling
- **Checkpointing**: Resume capability for long extractions
- **Multi-League**: Unified schema supporting 6+ leagues
- **Data Quality**: Validation gates at each pipeline stage
- **Surrogate Keys**: MD5-based keys for dimension tables
- **Evidence Trail**: Checksums and logs for auditability

---

## Sample Analytics Output

### Team Performance
```
Team          W    L    Win%   Home    Away    PtsDiff
Lakers       52   30   63.4%  28-13   24-17    +4.2
Celtics      57   25   69.5%  32-9    25-16    +7.1
```

### Betting Trends
```
Metric              Value
Home Cover %        51.8%
Over Hit %          49.2%
Avg Spread          -4.5
Avg Total Line      218.5
```

---

## Data Verification

All data sources are publicly accessible:
- ESPN API: No authentication required for scores/schedules
- Historical data: 5+ years available

---

**Author:** Mboya Jeffers | MboyaJeffers9@gmail.com
