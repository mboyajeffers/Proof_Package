# Energy Grid Pipeline Architecture
## P04: Enterprise Data Engineering on EIA-930 Time-Series Data

**Author:** Mboya Jeffers
**Version:** 1.0.0
**Pipeline:** `projects/v2_foundation/P04_Energy_Grid/src/` (500K+ rows)

---

## High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      ENERGY GRID PIPELINE (P04)                             │
│                 EIA-930 API → Hourly Readings → Grid KPIs                   │
└─────────────────────────────────────────────────────────────────────────────┘

     ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
     │   EIA-930    │      │  60+ Balancng│      │   500K+      │
     │  Grid API    │─────▶│  Authorities │─────▶│   Hourly     │
     │   (Public)   │      │  All U.S.    │      │   Readings   │
     └──────────────┘      └──────────────┘      └──────────────┘
            │                                           │
            ▼                                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  EXTRACT                                                                    │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ EIAGridClient                                                         ││
│  │  • /v2/electricity/rto/region-data/data    → Demand + generation      ││
│  │  • /v2/electricity/rto/interchange-data    → Net interchange flows    ││
│  │  • Rate limiting: 1 request/second                                    ││
│  │  • API key required (free registration)                               ││
│  │  • Hourly granularity: 2015-present                                   ││
│  └────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  TRANSFORM                                                                  │
│  ┌──────────────────────┐   ┌──────────────────────────────────────────────┐│
│  │  DataCleaner         │   │  DataModeler                                 ││
│  │  • UTC normalization │   │  • Star schema creation                      ││
│  │  • MW unit validation│──▶│  • Balancing authority dimension             ││
│  │  • Null handling     │   │  • Fuel type dimension                       ││
│  │  • Outlier flagging  │   │  • Datetime dimension                        ││
│  │  • Timezone mapping  │   │  • Grid operations fact table                ││
│  └──────────────────────┘   └──────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  VALIDATE                                                                   │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ QualityGateRunner (4 Gates)                                            ││
│  │  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐             ││
│  │  │ Temporal  │ │  Range    │ │ Coverage  │ │ Continuity│             ││
│  │  │Consistency│ │Validation │ │           │ │           │             ││
│  │  │   (25%)   │ │   (25%)   │ │   (25%)   │ │   (25%)   │             ││
│  │  └───────────┘ └───────────┘ └───────────┘ └───────────┘             ││
│  └────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  LOAD                                                                       │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ KPICalculator                                                          ││
│  │  • Demand analytics (peak, average, forecast accuracy)                 ││
│  │  • Generation mix (renewable share, fuel distribution)                 ││
│  │  • Grid operations (interchange, load balancing)                       ││
│  │  • Regional comparisons (BA-level benchmarks)                          ││
│  └────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │ Outputs:                                                                ││
│  │  • dim_balancing_authority.parquet → ~60 balancing authorities          ││
│  │  • dim_fuel_type.parquet          → ~15 fuel types                     ││
│  │  • dim_datetime.parquet           → 17K+ hourly periods                ││
│  │  • fact_grid_ops.parquet          → 500K+ readings                     ││
│  │  • demand_analysis.csv            → Regional demand patterns           ││
│  │  • generation_mix.csv             → Fuel breakdown                     ││
│  │  • pipeline_summary.txt           → Execution telemetry                ││
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
│ dim_balancing_authority│                         │   dim_fuel_type       │
├───────────────────────┤                         ├───────────────────────┤
│ ba_id (PK)            │                         │ fuel_id (PK)          │
│ ba_code               │                         │ fuel_code             │
│ ba_name               │                         │ fuel_name             │
│ region                │                         │ fuel_category         │
│ timezone              │                         │ is_renewable          │
│ interconnection       │                         │ is_clean              │
└───────────────────────┘                         └───────────────────────┘
          │                                                │
          │ 1:N                                            │ 1:N
          ▼                                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           fact_grid_ops                                   │
├─────────────────────────────────────────────────────────────────────────┤
│ fact_id (PK)           │ Generated MD5 hash                             │
│ ba_id (FK)             │ → dim_balancing_authority                      │
│ datetime_id (FK)       │ → dim_datetime                                │
│ fuel_id (FK)           │ → dim_fuel_type                               │
│ demand_mw              │ Total demand in megawatts                      │
│ generation_mw          │ Total generation in megawatts                  │
│ net_generation         │ Generation minus self-consumption              │
│ interchange_mw         │ Net import/export between regions              │
│ demand_forecast_mw     │ Day-ahead demand forecast                      │
│ forecast_error_pct     │ Absolute forecast error percentage             │
└─────────────────────────────────────────────────────────────────────────┘
          ▲
          │ N:1
┌───────────────────────┐
│    dim_datetime        │
├───────────────────────┤
│ datetime_id (PK)      │
│ timestamp             │
│ date                  │
│ hour                  │
│ day_of_week           │
│ month                 │
│ quarter               │
│ year                  │
│ is_peak_hour          │
│ is_weekend            │
│ season                │
└───────────────────────┘
```

---

## Quality Gate Framework

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ENERGY GRID QUALITY GATES                                │
│                    (Time-Series Specific Validation)                        │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┬────────────────────────────────────────────────┬──────────┐
│ Gate            │ Validation Logic                               │ Weight   │
├─────────────────┼────────────────────────────────────────────────┼──────────┤
│ Temporal        │ • Hourly intervals are consistent              │   25%    │
│ Consistency     │ • No duplicate timestamps per BA               │          │
│                 │ • Pass threshold: 95%                          │          │
├─────────────────┼────────────────────────────────────────────────┼──────────┤
│ Range           │ • MW values within physical bounds             │   25%    │
│ Validation      │ • Demand > 0, Generation >= 0                  │          │
│                 │ • Pass threshold: 98%                          │          │
├─────────────────┼────────────────────────────────────────────────┼──────────┤
│ Coverage        │ • % of expected hours with data                │   25%    │
│                 │ • All major BAs present                        │          │
│                 │ • Pass threshold: 90%                          │          │
├─────────────────┼────────────────────────────────────────────────┼──────────┤
│ Continuity      │ • No gaps > 4 hours in sequence                │   25%    │
│                 │ • Flag missing intervals                       │          │
│                 │ • Pass threshold: 90%                          │          │
└─────────────────┴────────────────────────────────────────────────┴──────────┘
```

---

## API Integration Pattern

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EIA-930 API INTEGRATION                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Endpoint 1: Regional Demand & Generation                                   │
│  ─────────────────────────────────────────────────────────────────────────  │
│  GET https://api.eia.gov/v2/electricity/rto/region-data/data                │
│                                                                             │
│  Parameters:                                                                │
│  • api_key: EIA API key (free registration)                                 │
│  • frequency: hourly                                                        │
│  • data: value (MW readings)                                                │
│  • facets: respondent (balancing authority code)                             │
│  • start/end: date range                                                    │
│  • sort: period asc                                                         │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Endpoint 2: Interchange Data                                               │
│  ─────────────────────────────────────────────────────────────────────────  │
│  GET https://api.eia.gov/v2/electricity/rto/interchange-data/data           │
│                                                                             │
│  Returns: Net interchange between balancing authorities (MW)                │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Rate Limiting & Pagination                                                 │
│  ─────────────────────────────────────────────────────────────────────────  │
│  • 1 request/second (EIA policy)                                            │
│  • Pagination: offset-based, 5,000 rows per page                           │
│  • Large queries auto-paginated                                             │
│  • API key required (free at eia.gov/opendata)                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## KPI Calculations

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       ENERGY GRID KPI FRAMEWORK                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 1. DEMAND ANALYTICS                                                        │
├────────────────────────────────────────────────────────────────────────────┤
│ Metrics:                                                                   │
│   • Peak demand by region (MW)                                             │
│   • Average hourly demand                                                  │
│   • Demand forecast accuracy (MAPE)                                        │
│   • Day-of-week load profile                                               │
│   • Peak vs off-peak ratio                                                 │
│                                                                            │
│ Formulas:                                                                  │
│   ┌───────────────────────────────────────────────────────────────────┐    │
│   │ forecast_accuracy = 1 - abs(actual - forecast) / actual           │    │
│   │ peak_ratio        = peak_demand / avg_demand                      │    │
│   │ load_factor       = avg_demand / peak_demand                      │    │
│   └───────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 2. GENERATION MIX                                                          │
├────────────────────────────────────────────────────────────────────────────┤
│ Metrics:                                                                   │
│   • Renewable share (%)                                                    │
│   • Clean energy percentage                                                │
│   • Fuel type distribution                                                 │
│   • Generation capacity utilization                                        │
│                                                                            │
│ Formulas:                                                                  │
│   ┌───────────────────────────────────────────────────────────────────┐    │
│   │ renewable_share = renewable_gen / total_gen                        │    │
│   │ clean_pct       = (renewable + nuclear) / total_gen                │    │
│   │ capacity_factor = actual_gen / (nameplate_capacity * hours)        │    │
│   └───────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 3. GRID OPERATIONS                                                         │
├────────────────────────────────────────────────────────────────────────────┤
│ Metrics:                                                                   │
│   • Net interchange (import/export balance)                                │
│   • Regional self-sufficiency ratio                                        │
│   • Peak shaving effectiveness                                             │
│   • Grid stability indicators                                              │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| **Hourly granularity** | EIA-930 provides hourly data; matches grid operations cadence |
| **UTC normalization** | BAs span multiple time zones; UTC provides consistent analysis |
| **Parquet storage** | Columnar format optimal for time-series aggregation queries |
| **Peak hour flag** | Pre-computed flag (7am-11pm local) enables fast peak analysis |
| **Renewable classification** | Fuel types tagged is_renewable/is_clean for energy mix analysis |
| **2-year window** | Balances scale (500K+ rows) with storage constraints |

---

## Files Produced

| File | Description | Typical Size |
|------|-------------|--------------|
| `dim_balancing_authority.parquet` | BA master data | < 1 MB |
| `dim_fuel_type.parquet` | Fuel classifications | < 1 MB |
| `dim_datetime.parquet` | Hourly time dimension | 2-5 MB |
| `fact_grid_ops.parquet` | Hourly grid readings | 50-100 MB |
| `demand_analysis.csv` | Regional demand patterns | 1-5 MB |
| `generation_mix.csv` | Fuel breakdown by region | 1-5 MB |
| `pipeline_summary.txt` | Execution telemetry | < 1 MB |

---

## Execution

```bash
# Run full pipeline (500K+ readings, ~2 years)
python src/main.py --mode full

# Test mode (100K readings, ~6 months)
python src/main.py --mode test

# Quick validation (50K readings, ~1 month)
python src/main.py --mode quick
```

---

*Architecture Document v1.0.0 | Mboya Jeffers | Feb 2026*
