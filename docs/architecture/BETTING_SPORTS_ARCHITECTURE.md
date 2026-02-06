# Betting & Sports Analytics Pipeline Architecture
## P06: Enterprise Data Engineering on ESPN Multi-League Data

**Author:** Mboya Jeffers
**Version:** 1.0.0
**Pipeline:** `projects/v3_scale/P06_Betting_Sports/src/` (8M rows)

---

## High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                  BETTING & SPORTS ANALYTICS PIPELINE (P06)                   │
│             ESPN API → Multi-League Schema → Betting KPIs                    │
└─────────────────────────────────────────────────────────────────────────────┘

     ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
     │   ESPN API   │      │   6 Leagues  │      │    8M+       │
     │  (Public)    │─────▶│  NFL NBA MLB │─────▶│    Records   │
     │              │      │  NHL NCAAF/B │      │    Produced  │
     └──────────────┘      └──────────────┘      └──────────────┘
            │                                           │
            ▼                                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  EXTRACT                                                                    │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ ESPNExtractor                                                         ││
│  │  • /sports/{sport}/leagues/{league}/teams     → Team rosters          ││
│  │  • /sports/{sport}/leagues/{league}/scoreboard → Live + historical    ││
│  │  • /events/{id}/summary                       → Box scores           ││
│  │  • Rate limiting: ~100 requests/minute                                ││
│  │  • No authentication required                                          ││
│  │  • Supports: NFL, NBA, MLB, NHL, NCAAF, NCAAB                         ││
│  └────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  TRANSFORM                                                                  │
│  ┌──────────────────────┐   ┌──────────────────────────────────────────────┐│
│  │  DataCleaner         │   │  DataModeler                                 ││
│  │  • Score parsing     │   │  • Kimball star schema                       ││
│  │  • Date normalization│──▶│  • Team/Player/League dimensions             ││
│  │  • League unification│   │  • Game results fact table                   ││
│  │  • Venue matching    │   │  • Odds/betting fact table                   ││
│  │  • Season tagging    │   │  • Player stats fact table                   ││
│  └──────────────────────┘   └──────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  VALIDATE                                                                   │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ QualityGateRunner (4 Gates)                                            ││
│  │  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐             ││
│  │  │  Score    │ │  Season   │ │  Team     │ │  Venue    │             ││
│  │  │Consistency│ │  Logic    │ │ Coverage  │ │ Matching  │             ││
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
│  │  • Team performance (win/loss, point differential, home/away splits)   ││
│  │  • Betting trends (ATS records, cover %, spread accuracy)              ││
│  │  • Over/Under analysis (total trends, over %, average lines)           ││
│  │  • Home advantage (home win rate, margin by league)                    ││
│  │  • Scoring distribution (avg totals, close games %)                    ││
│  └────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │ Outputs:                                                                ││
│  │  • dim_team.parquet          → Team master data (SCD Type 2)           ││
│  │  • dim_player.parquet        → Player information                      ││
│  │  • dim_league.parquet        → League/sport data                       ││
│  │  • dim_venue.parquet         → Stadium/arena data                      ││
│  │  • dim_season.parquet        → Season metadata                         ││
│  │  • dim_date.parquet          → Date dimension                          ││
│  │  • fact_games.parquet        → Game results                            ││
│  │  • fact_odds.parquet         → Betting lines                           ││
│  │  • fact_player_stats.parquet → Box scores                              ││
│  │  • kpis.json                 → Calculated KPIs                         ││
│  └─────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Star Schema Data Model

```
┌───────────────────────────────────────────────────────────────────────────┐
│                         DIMENSIONAL MODEL                                  │
└───────────────────────────────────────────────────────────────────────────┘

┌───────────────────┐  ┌───────────────────┐  ┌───────────────────┐
│   dim_team        │  │   dim_league      │  │   dim_venue       │
├───────────────────┤  ├───────────────────┤  ├───────────────────┤
│ team_id (PK)      │  │ league_id (PK)    │  │ venue_id (PK)     │
│ team_name         │  │ league_name       │  │ venue_name        │
│ team_abbr         │  │ sport             │  │ city              │
│ league_id (FK)    │  │ conference_count  │  │ state             │
│ conference        │  │ teams_per_conf    │  │ capacity          │
│ division          │  │ season_start      │  │ surface_type      │
│ city              │  │ season_end        │  └───────────────────┘
│ colors            │  └───────────────────┘
└───────────────────┘
          │
          │ 1:N
          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                            fact_games                                     │
├─────────────────────────────────────────────────────────────────────────┤
│ game_id (PK)           │ Generated MD5 hash                             │
│ home_team_id (FK)      │ → dim_team                                    │
│ away_team_id (FK)      │ → dim_team                                    │
│ venue_id (FK)          │ → dim_venue                                   │
│ date_id (FK)           │ → dim_date                                    │
│ season_id (FK)         │ → dim_season                                  │
│ home_score             │ Final home team score                          │
│ away_score             │ Final away team score                          │
│ spread                 │ Closing spread (home perspective)              │
│ over_under             │ Closing over/under total                       │
│ attendance             │ Reported attendance                            │
│ home_covered           │ Boolean: home team covered spread              │
│ total_over             │ Boolean: total went over                       │
│ margin                 │ home_score - away_score                        │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## KPI Calculations

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    BETTING & SPORTS KPI FRAMEWORK (16 KPIs)                  │
└─────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 1. TEAM PERFORMANCE                                                        │
├────────────────────────────────────────────────────────────────────────────┤
│   ┌───────────────────────────────────────────────────────────────────┐    │
│   │ win_pct         = wins / (wins + losses)                          │    │
│   │ point_diff      = avg(home_score - away_score) [as home team]     │    │
│   │ home_win_pct    = home_wins / home_games                          │    │
│   │ away_win_pct    = away_wins / away_games                          │    │
│   └───────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 2. BETTING TRENDS                                                          │
├────────────────────────────────────────────────────────────────────────────┤
│   ┌───────────────────────────────────────────────────────────────────┐    │
│   │ ats_record      = covers / total_games_with_spread                │    │
│   │ cover_pct       = games_covered / games_with_spread               │    │
│   │ avg_margin_vs_spread = avg(actual_margin - spread)                │    │
│   │ upset_rate      = underdogs_winning / total_games                  │    │
│   └───────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 3. OVER/UNDER ANALYSIS                                                     │
├────────────────────────────────────────────────────────────────────────────┤
│   ┌───────────────────────────────────────────────────────────────────┐    │
│   │ over_pct        = overs / total_games_with_total                  │    │
│   │ avg_total       = avg(home_score + away_score)                    │    │
│   │ avg_total_diff  = avg(actual_total - posted_total)                │    │
│   └───────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 4. HOME ADVANTAGE                                                          │
├────────────────────────────────────────────────────────────────────────────┤
│ By league:                                                                 │
│   • NFL: ~57% home win rate                                                │
│   • NBA: ~60% home win rate                                                │
│   • MLB: ~54% home win rate                                                │
│   • NHL: ~55% home win rate                                                │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## Multi-League Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    UNIFIED MULTI-LEAGUE SCHEMA                              │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────┬──────────┬───────┬────────────┬──────────────────────────────┐
│ League      │ Teams    │ Games │ Seasons    │ ESPN API Path                │
├─────────────┼──────────┼───────┼────────────┼──────────────────────────────┤
│ NFL         │ 32       │ ~270  │ Sep-Feb    │ /football/nfl                │
│ NBA         │ 30       │ ~1230 │ Oct-Jun    │ /basketball/nba              │
│ MLB         │ 30       │ ~2430 │ Mar-Oct    │ /baseball/mlb                │
│ NHL         │ 32       │ ~1312 │ Oct-Jun    │ /hockey/nhl                  │
│ NCAAF       │ 130+     │ ~800  │ Aug-Jan    │ /football/college-football   │
│ NCAAB       │ 350+     │ ~5000 │ Nov-Apr    │ /basketball/mens-college     │
└─────────────┴──────────┴───────┴────────────┴──────────────────────────────┘

Key Design: Single unified schema handles all leagues via dim_league.
Sport-specific metrics stored in generic columns with league context.
```

---

## Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| **Unified schema for 6 leagues** | Single fact table with league FK avoids code duplication |
| **Separate odds fact table** | Not all games have betting data; avoids NULLs in main fact |
| **Home perspective for spreads** | Industry standard; negative spread = home favored |
| **SCD Type 2 for teams** | Teams relocate, rebrand; track historical identity |
| **Season dimension** | Season dates vary by sport; dedicated dimension handles this |
| **ESPN as primary source** | Free, reliable, covers all major US leagues |

---

## Execution

```bash
# Run test mode (sample data)
python src/main.py --mode test

# Run full extraction (8M+ rows, several hours)
python src/main.py --mode full
```

---

*Architecture Document v1.0.0 | Mboya Jeffers | Feb 2026*
