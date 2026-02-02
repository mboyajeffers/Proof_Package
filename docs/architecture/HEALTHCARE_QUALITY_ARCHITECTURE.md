# Healthcare Quality Pipeline Architecture
## P3-MED: Hospital Quality Benchmarking on Data.Medicare.gov

**Author:** Mboya Jeffers
**Version:** 1.0.0
**Pipeline:** `/pipelines/healthcare_quality/pipeline.py` (693 lines)

---

## High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                   HEALTHCARE QUALITY PIPELINE (P3-MED)                      │
│                  Data.Medicare.gov → Star Schema → KPIs                     │
└─────────────────────────────────────────────────────────────────────────────┘

     ┌──────────────────────────────────────────────────────────────────────┐
     │                    CMS PROVIDER DATA API                             │
     │                    (Data.Medicare.gov)                               │
     └──────────────────────────────────────────────────────────────────────┘
                │                │                │                │
                ▼                ▼                ▼                ▼
     ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
     │   Hospital   │ │   Timely &   │ │   Patient    │ │ Readmissions │
     │    General   │ │  Effective   │ │  Experience  │ │   & Deaths   │
     │    Info      │ │    Care      │ │   (HCAHPS)   │ │              │
     │  (xubh-q36u) │ │  (yv7e-xc69) │ │  (dgck-syfz) │ │  (ynj2-r877) │
     └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘
                │                │                │                │
                └────────────────┴────────────────┴────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  EXTRACT                                                                    │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ MedicareDataClient                                                     ││
│  │  • CMS Provider Data API format                                        ││
│  │  • Params: size, offset (pagination)                                   ││
│  │  • Rate limiting: 0.5s delay                                           ││
│  │  • State filtering: CA, TX, FL, NY, PA, IL, OH, GA, NC, MI             ││
│  └────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  TRANSFORM                                                                  │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ DataCleaner                                                            ││
│  │  ┌────────────────────┐  ┌────────────────────┐  ┌──────────────────┐  ││
│  │  │ clean_hospital_info│  │clean_quality_measures│ │   clean_hcahps   │  ││
│  │  │ • Column normalize │  │ • Score to numeric  │  │ • Percent convert│  ││
│  │  │ • Rating to numeric│  │ • N/A handling      │  │ • Rate normalize │  ││
│  │  │ • facility_id clean│  │ • facility_id clean │  │                  │  ││
│  │  └────────────────────┘  └────────────────────┘  └──────────────────┘  ││
│  └────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ DataModeler                                                            ││
│  │  • build_hospital_dim() → Surrogate keys, dimension table              ││
│  │  • build_quality_fact() → MD5 fact IDs, measure type tagging           ││
│  └────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  VALIDATE                                                                   │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ QualityGateRunner (4 Gates)                                            ││
│  │                                                                        ││
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   ││
│  │  │ Completeness│  │  Uniqueness │  │    Range    │  │ Suppression │   ││
│  │  │    (25%)    │  │    (25%)    │  │    (15%)    │  │    (20%)    │   ││
│  │  │             │  │             │  │             │  │             │   ││
│  │  │ Key columns │  │ facility_id │  │ Rating 1-5  │  │ Score N/A   │   ││
│  │  │ null check  │  │ duplicates  │  │ validation  │  │ rate check  │   ││
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘   ││
│  └────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  LOAD                                                                       │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ KPICalculator                                                          ││
│  │  • Summary statistics (total hospitals, states, avg rating)            ││
│  │  • Ratings distribution (1-5 star breakdown)                           ││
│  │  • State comparison (avg rating by state)                              ││
│  │  • Ownership analysis (rating by ownership type)                       ││
│  │  • Top/bottom hospitals (quality leaders & laggards)                   ││
│  └────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │ Outputs:                                                                ││
│  │  • raw_hospitals.csv            → Raw API data                         ││
│  │  • raw_timely_care.csv          → Raw quality measures                 ││
│  │  • cleaned_hospitals.csv        → Standardized hospital data           ││
│  │  • cleaned_timely_care.csv      → Standardized measures                ││
│  │  • hospital_dim.csv             → Hospital dimension                   ││
│  │  • quality_fact.csv             → Quality measures fact table          ││
│  │  • kpis.json                    → Healthcare quality KPIs              ││
│  │  • pipeline_metrics.json        → Execution telemetry                  ││
│  └─────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Star Schema Data Model

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DIMENSIONAL MODEL                                 │
└─────────────────────────────────────────────────────────────────────────────┘


                    ┌─────────────────────────────────────┐
                    │           hospital_dim              │
                    ├─────────────────────────────────────┤
                    │ hospital_key (PK)     SURROGATE     │
                    │ facility_id           CMS ID        │
                    │ facility_name                       │
                    │ address                             │
                    │ citytown                            │
                    │ state                               │
                    │ zip_code                            │
                    │ county_name                         │
                    │ phone_number                        │
                    │ hospital_type                       │
                    │ hospital_ownership                  │
                    │ emergency_services    (Y/N)         │
                    │ hospital_overall_rating (1-5)       │
                    └─────────────────────────────────────┘
                                     │
                                     │ 1:N
                                     ▼
                    ┌─────────────────────────────────────┐
                    │           quality_fact              │
                    ├─────────────────────────────────────┤
                    │ fact_id (PK)          MD5 hash      │
                    │ facility_id (FK)      → hospital_dim│
                    │ measure_id                          │
                    │ measure_type          (timely_care, │
                    │                        hcahps,      │
                    │                        readmissions)│
                    │ score                 Numeric       │
                    │ snapshot_date                       │
                    └─────────────────────────────────────┘


    ┌─────────────────────────────────────────────────────────────────────┐
    │                    HOSPITAL TYPE VALUES                             │
    ├─────────────────────────────────────────────────────────────────────┤
    │  • Acute Care Hospitals                                             │
    │  • Critical Access Hospitals                                        │
    │  • Childrens Hospitals                                              │
    │  • Psychiatric Hospitals                                            │
    └─────────────────────────────────────────────────────────────────────┘

    ┌─────────────────────────────────────────────────────────────────────┐
    │                    OWNERSHIP TYPE VALUES                            │
    ├─────────────────────────────────────────────────────────────────────┤
    │  • Government - Federal                                             │
    │  • Government - Hospital District or Authority                      │
    │  • Government - Local                                               │
    │  • Government - State                                               │
    │  • Proprietary                                                      │
    │  • Voluntary non-profit - Church                                    │
    │  • Voluntary non-profit - Other                                     │
    │  • Voluntary non-profit - Private                                   │
    └─────────────────────────────────────────────────────────────────────┘
```

---

## Data Sources (CMS Provider Data API)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DATA.MEDICARE.GOV DATASETS                               │
│                    (All Public, No API Key Required)                        │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────┬────────────┬──────────────────────────────────────────────┐
│ Dataset          │ ID         │ Description                                  │
├──────────────────┼────────────┼──────────────────────────────────────────────┤
│ Hospital General │ xubh-q36u  │ Facility info, location, type, ownership,    │
│ Information      │            │ emergency services, overall star rating      │
├──────────────────┼────────────┼──────────────────────────────────────────────┤
│ Timely and       │ yv7e-xc69  │ ED wait times, stroke care, surgery timing,  │
│ Effective Care   │            │ preventive care measures                     │
├──────────────────┼────────────┼──────────────────────────────────────────────┤
│ Patient Survey   │ dgck-syfz  │ HCAHPS patient experience scores:            │
│ (HCAHPS)         │            │ communication, responsiveness, cleanliness   │
├──────────────────┼────────────┼──────────────────────────────────────────────┤
│ Complications    │ ynj2-r877  │ Mortality rates, readmission rates,          │
│ and Deaths       │            │ hospital-acquired conditions                 │
└──────────────────┴────────────┴──────────────────────────────────────────────┘

API Endpoint Format:
┌─────────────────────────────────────────────────────────────────────────────┐
│ https://data.cms.gov/provider-data/api/1/datastore/query/{dataset_id}/0    │
│                                                                             │
│ Parameters:                                                                 │
│   • size   = Number of records (default: 5000)                              │
│   • offset = Pagination offset                                              │
│                                                                             │
│ Response Format:                                                            │
│   { "results": [...], "count": N, ... }                                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Quality Gate Framework

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    HEALTHCARE DATA QUALITY GATES                            │
│                    (Weighted Score: 0.0 - 1.0)                              │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┬──────────────────────────────────────────────┬────────────┐
│ Gate            │ Validation Logic                             │ Weight     │
├─────────────────┼──────────────────────────────────────────────┼────────────┤
│ Completeness    │ • Check key columns for nulls:               │   25%      │
│                 │   facility_id, facility_name, state,         │            │
│                 │   hospital_type                              │            │
│                 │ • Pass threshold: 90%                        │            │
├─────────────────┼──────────────────────────────────────────────┼────────────┤
│ Uniqueness      │ • facility_id must be unique                 │   25%      │
│                 │ • Detect duplicate hospital records          │            │
│                 │ • Pass threshold: 99%                        │            │
├─────────────────┼──────────────────────────────────────────────┼────────────┤
│ Range           │ • hospital_overall_rating between 1-5        │   15%      │
│                 │ • Flag out-of-bounds values                  │            │
│                 │ • Pass threshold: 95%                        │            │
├─────────────────┼──────────────────────────────────────────────┼────────────┤
│ Suppression     │ • Track "Not Available" / suppressed values  │   20%      │
│                 │ • CMS suppresses data for privacy            │            │
│                 │ • Pass threshold: 60% (informational)        │            │
└─────────────────┴──────────────────────────────────────────────┴────────────┘

Note: Healthcare data has higher suppression rates due to CMS privacy rules.
Small sample sizes (< 25 patients) are suppressed to prevent identification.
```

---

## API Integration Pattern

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CMS PROVIDER DATA API INTEGRATION                        │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Request Pattern                                                            │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                             │
│  GET /provider-data/api/1/datastore/query/{dataset_id}/0                    │
│      ?size=5000                                                             │
│      &offset=0                                                              │
│                                                                             │
│  Headers:                                                                   │
│    User-Agent: DataEngineering-Portfolio/1.0 (mboyajeffers9@gmail.com)      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Pagination Strategy                                                        │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                             │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐                                  │
│  │ Page 1  │───▶│ Page 2  │───▶│ Page 3  │───▶ ...                          │
│  │ size=   │    │ offset= │    │ offset= │                                  │
│  │ 5000    │    │ 5000    │    │ 10000   │                                  │
│  └─────────┘    └─────────┘    └─────────┘                                  │
│                                                                             │
│  Rate Limiting: 0.5s delay between requests                                 │
│  No API key required (public data)                                          │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  State Filtering                                                            │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                             │
│  Target States (Top 10 by population):                                      │
│  ┌────┬────┬────┬────┬────┬────┬────┬────┬────┬────┐                        │
│  │ CA │ TX │ FL │ NY │ PA │ IL │ OH │ GA │ NC │ MI │                        │
│  └────┴────┴────┴────┴────┴────┴────┴────┴────┴────┘                        │
│                                                                             │
│  Filter applied post-retrieval (API doesn't support state filtering)        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## KPI Calculations

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    HEALTHCARE QUALITY KPIs                                  │
└─────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────┐
│ 1. SUMMARY STATISTICS                  │
├────────────────────────────────────────┤
│ • total_hospitals: Count of facilities │
│ • states_covered: Unique states        │
│ • with_ratings: Hospitals with scores  │
│ • avg_rating: Mean star rating         │
│ • quality_measures: Measure count      │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│ 2. RATINGS DISTRIBUTION                │
├────────────────────────────────────────┤
│ Star breakdown:                        │
│   • 1_star: N hospitals                │
│   • 2_star: N hospitals                │
│   • 3_star: N hospitals                │
│   • 4_star: N hospitals                │
│   • 5_star: N hospitals                │
│                                        │
│ Use: Quality tier segmentation         │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│ 3. STATE COMPARISON                    │
├────────────────────────────────────────┤
│ Per state (top 10):                    │
│   • hospital_count                     │
│   • avg_rating                         │
│   • median_rating                      │
│                                        │
│ Use: Geographic quality benchmarking   │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│ 4. OWNERSHIP ANALYSIS                  │
├────────────────────────────────────────┤
│ Per ownership type:                    │
│   • count                              │
│   • avg_rating                         │
│                                        │
│ Compare: Non-profit vs Proprietary     │
│          vs Government                 │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│ 5. TOP HOSPITALS (n=10)                │
├────────────────────────────────────────┤
│ Highest rated facilities:              │
│   • facility_id, facility_name         │
│   • citytown, state                    │
│   • hospital_overall_rating            │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│ 6. BOTTOM HOSPITALS (n=10)             │
├────────────────────────────────────────┤
│ Lowest rated facilities:               │
│   • facility_id, facility_name         │
│   • citytown, state                    │
│   • hospital_overall_rating            │
│                                        │
│ Use: Quality improvement targeting     │
└────────────────────────────────────────┘
```

---

## Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| **4 data sources** | Comprehensive view: facility info + care quality + patient experience + outcomes |
| **Post-fetch state filtering** | CMS API doesn't support query filters; filter client-side |
| **Suppression gate** | Healthcare data has legitimate N/A values due to CMS privacy rules |
| **60% suppression threshold** | Lower than other pipelines because CMS suppresses small sample sizes |
| **MD5 fact IDs** | Deduplication across multiple measure types |
| **Ownership analysis KPI** | Key insight: non-profit vs proprietary quality differences |

---

## Files Produced

| File | Description | Typical Size |
|------|-------------|--------------|
| `raw_hospitals.csv` | Raw hospital info from API | 1-5 MB |
| `raw_timely_care.csv` | Raw quality measures | 5-20 MB |
| `cleaned_hospitals.csv` | Standardized hospital data | 1-3 MB |
| `cleaned_timely_care.csv` | Standardized measures | 3-15 MB |
| `hospital_dim.csv` | Hospital dimension table | 1-3 MB |
| `quality_fact.csv` | Quality measures fact table | 3-15 MB |
| `kpis.json` | Healthcare quality KPIs | < 1 MB |
| `pipeline_metrics.json` | Execution telemetry | < 1 MB |

---

## Execution

```bash
# Run the pipeline
python pipeline.py

# Output example:
# ============================================================
# P3-MED: Healthcare Quality Intelligence Pipeline
# ============================================================
# [1/6] Fetching data from Data.Medicare.gov...
# [2/6] Cleaning and normalizing data...
# [3/6] Building star schema...
# [4/6] Running quality gates...
# [5/6] Calculating KPIs...
# [6/6] Saving pipeline metrics...
# ============================================================
# Pipeline Complete!
#   Duration: 45.2 seconds
#   API Calls: 2
#   Hospitals: 942
#   Quality Score: 87.3%
# ============================================================
```

---

*Architecture Document v1.0.0 | Mboya Jeffers | Feb 2026*
