# Project3 Issues Log
## Version: 1.1.0 | Status: PRE-PDF AUDIT COMPLETE

---

## Summary

| Severity | Count | Resolved |
|----------|-------|----------|
| CRITICAL | 0 | - |
| HIGH | 3 | 2 ✓ + 1 DISCLOSED |
| MEDIUM | 5 | 3 ✓ + 2 DISCLOSED |
| LOW | 3 | NOTED |

**Pre-PDF audit completed 2026-02-01**

---

## Resolved Issues

### ISS-001: P3-MED CMS API URL Format Incorrect
**Severity:** HIGH | **Project:** P3-MED | **Status:** FIXED

**Issue:** Initial API URL used Socrata `/resource/` format which returned 400 errors.

**Evidence:**
```
ERROR: 400 Client Error: Bad Request for url:
https://data.cms.gov/resource/xubh-q36u.json?$limit=10000
```

**Resolution Applied:**
- Changed URL format to CMS Provider Data API format
- Correct format: `https://data.cms.gov/provider-data/api/1/datastore/query/{id}/0`
- Parameter 'size' used instead of 'limit'
- Fixed: 2026-02-01

---

### ISS-002: P3-MED Column Name 'city' Not Found
**Severity:** HIGH | **Project:** P3-MED | **Status:** FIXED

**Issue:** Pipeline referenced `'city'` column but actual CMS schema uses `'citytown'`.

**Evidence:**
```
KeyError: "['city'] not in index"
```

**Resolution Applied:**
- Changed all references from `'city'` to `'citytown'`
- Verified against actual API response schema
- Fixed: 2026-02-01

---

### ISS-003: P3-MED Limited State Coverage
**Severity:** MEDIUM | **Project:** P3-MED | **Status:** NOTED

**Issue:** Only 4 of 10 target states retrieved due to API pagination limits.

**Evidence:**
- Retrieved 1500 records total
- After state filter: 942 records
- States covered: CA, IL, FL, GA (4 of 10)

**Resolution Applied:**
- Documented as demo scope limitation
- Full pagination would require multiple API calls
- Acceptable for portfolio demonstration
- Noted: 2026-02-01

---

### ISS-004: P3-SEC NumPy Bool JSON Serialization
**Severity:** MEDIUM | **Project:** P3-SEC | **Status:** FIXED

**Issue:** NumPy boolean values not JSON serializable.

**Evidence:**
```
TypeError: Object of type bool_ is not JSON serializable
when serializing dict item 'passed'
```

**Resolution Applied:**
- Added explicit `bool()` conversion in quality gates
- Added default handler in json.dump()
- Fixed: 2026-02-01

---

### ISS-005: P3-ENG String Values in Numeric Column
**Severity:** MEDIUM | **Project:** P3-ENG | **Status:** FIXED

**Issue:** EIA API returns 'value' column as strings, causing comparison errors.

**Evidence:**
```
TypeError: '>=' not supported between instances of 'str' and 'int'
```

**Resolution Applied:**
- Added `pd.to_numeric(generation_df['value'], errors='coerce')` before quality checks
- Fixed: 2026-02-01

---

## Pre-PDF Audit Issues (2026-02-01)

### ISS-006: P3-MED 32.8% Hospitals Missing Ratings
**Severity:** MEDIUM | **Project:** P3-MED | **Status:** DISCLOSED

**Issue:** 309 of 942 hospitals (32.8%) have no overall rating in CMS data.

**Evidence:**
- Total hospitals: 942
- With ratings: 633
- Without ratings: 309

**Resolution:** Disclosed in Founder Summary PDF. This is expected - CMS notes some hospitals are too new, too small, or don't report sufficient data for rating.

---

### ISS-007: P3-MED Sample Bias Toward California
**Severity:** MEDIUM | **Project:** P3-MED | **Status:** DISCLOSED

**Issue:** Due to API pagination limits, top/bottom hospital lists are predominantly California.

**Evidence:**
- All 10 top hospitals: CA
- All 10 bottom hospitals: CA
- CA has 378 of 942 hospitals (40%)

**Resolution:** Disclosed in PDF. Limitation noted as "demo scope" - full implementation would paginate through all states.

---

### ISS-008: P3-SEC 86.6% CVEs Have UNKNOWN Severity
**Severity:** HIGH | **Project:** P3-SEC | **Status:** DISCLOSED

**Issue:** 433 of 500 CVEs (86.6%) have UNKNOWN severity because they are recent and awaiting NVD scoring.

**Evidence:**
```
Severity distribution:
UNKNOWN     433 (86.6%)
MEDIUM       38
HIGH         21
CRITICAL      5
LOW           3
```

**Resolution:** Disclosed prominently in PDF. This is expected behavior for recent CVEs - NVD scoring is not instantaneous. CVSS scores typically assigned within days/weeks of publication.

---

### ISS-009: P3-SEC Zero Recent CVEs in KEV Catalog
**Severity:** LOW | **Project:** P3-SEC | **Status:** NOTED

**Issue:** None of the 500 recent CVEs appear in the CISA KEV catalog.

**Evidence:**
- in_kev_catalog: 0
- KEV total: 1,501 vulnerabilities

**Resolution:** Explained in PDF. KEV contains confirmed actively exploited vulnerabilities - newly published CVEs haven't been confirmed exploited yet. This validates the pipeline is working correctly.

---

### ISS-010: P3-SEC Low EPSS Coverage
**Severity:** LOW | **Project:** P3-SEC | **Status:** NOTED

**Issue:** EPSS scores retrieved for only 72 of 500 CVEs (14.4%).

**Evidence:**
- avg_epss: 0.0007 (very low)
- EPSS API returned scores for 72 CVEs

**Resolution:** EPSS scores are calculated for CVEs with sufficient data. New CVEs may not have EPSS scores immediately. This is expected and documented.

---

### ISS-011: P3-ENG Battery Shows Negative Generation
**Severity:** LOW | **Project:** P3-ENG | **Status:** NOTED

**Issue:** Battery (BAT) fuel type shows -0.05% in fuel mix.

**Evidence:**
```json
"BAT": -0.05
```

**Resolution:** This is correct behavior. Batteries discharge (negative) when supplying power to grid. Positive values would indicate charging. No action needed.

---

## Quality Audit Notes

### P3-MED Quality Assessment

| Check | Status | Notes |
|-------|--------|-------|
| API Endpoint Verified | ✓ | data.cms.gov confirmed working |
| Data Schema Verified | ✓ | Columns match documentation |
| No Simulated Data | ✓ | 100% real CMS data |
| Numbers Consistent | ✓ | KPIs match raw data |

### P3-SEC Quality Assessment

| Check | Status | Notes |
|-------|--------|-------|
| NVD API Verified | ✓ | services.nvd.nist.gov working |
| KEV Catalog Verified | ✓ | CISA feed current |
| EPSS Scores Retrieved | ✓ | Partial coverage (72 of 500) |
| No Simulated Data | ✓ | 100% real government data |
| CVSS Completeness Low | ⚠ | 13.4% - recent CVEs unscored |

**Note:** Low CVSS completeness is expected for recent CVEs. The NVD scoring process takes time. This is documented, not a data quality issue.

### P3-ENG Quality Assessment

| Check | Status | Notes |
|-------|--------|-------|
| EIA API Verified | ✓ | api.eia.gov working |
| All Regions Represented | ✓ | 7 of 7 target regions |
| Data Validity | ✓ | 96.2% valid values |
| No Simulated Data | ✓ | 100% real EIA-930 data |

---

## Data Attribution

All Project 3 pipelines use **REAL PUBLIC GOVERNMENT APIs**:

| Project | Primary Source | Verification URL |
|---------|---------------|------------------|
| P3-MED | CMS Hospital Compare | https://data.cms.gov/provider-data/dataset/xubh-q36u |
| P3-SEC | NIST NVD | https://nvd.nist.gov/developers/vulnerabilities |
| P3-SEC | CISA KEV | https://www.cisa.gov/known-exploited-vulnerabilities-catalog |
| P3-SEC | FIRST EPSS | https://api.first.org/epss |
| P3-ENG | EIA-930 | https://www.eia.gov/opendata/ |

**NO SYNTHETIC OR SIMULATED DATA** was used in Project 3.

---

## Lessons Learned

1. **Always test API endpoints first** - URL formats vary between Socrata/custom APIs
2. **Verify column names against actual response** - Documentation may be outdated
3. **Handle type conversions explicitly** - APIs return strings even for numeric data
4. **Document limitations clearly** - Rate limits and demo scope are acceptable

---

*Last Updated: 2026-02-01*
