# v2 Foundation Projects
## 7.5M Rows | 4 Industries | Enterprise Patterns

---

## Overview

| Metric | Value |
|--------|-------|
| **Total Rows** | 7.5M |
| **Projects** | 4 |
| **Industries** | Finance, Government, Healthcare, Energy |
| **Status** | COMPLETE |

---

## Projects

### P01: SEC Financial Intelligence
- **Rows:** 1M+ financial facts
- **Source:** SEC EDGAR XBRL
- **Analytics:** 50+ KPIs (ROE, ROA, margins)

### P02: Federal Contract Awards
- **Rows:** 1M+ awards
- **Source:** USASpending.gov
- **Analytics:** Agency spending, contractor rankings

### P03: Medicare Prescriber Analysis
- **Rows:** 5M+ prescriptions
- **Source:** Medicare Part D Files
- **Analytics:** Opioid patterns, drug utilization

### P04: Energy Grid Analytics
- **Rows:** 500K+ readings
- **Source:** EIA-930 API
- **Analytics:** Demand patterns, generation mix

---

## Run Any Project

```bash
cd P01_SEC_Financial && python src/main.py --mode full
cd P02_Federal_Awards && python src/main.py --mode full
cd P03_Medicare_Prescriber && python src/main.py --mode full
cd P04_Energy_Grid && python src/main.py --mode full
```

---

*v2 Foundation - Demonstrates core data engineering patterns*
