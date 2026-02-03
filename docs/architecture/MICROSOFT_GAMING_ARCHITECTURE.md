# Methodology

## Overview

This document describes the methodology used for financial and stock performance analysis in the Microsoft Gaming Analytics Pipeline.

---

## Data Sources

### SEC EDGAR XBRL

**Source:** https://data.sec.gov/api/xbrl/companyfacts/

Financial metrics are extracted from official SEC filings using the XBRL (eXtensible Business Reporting Language) format.

**Key Points:**
- Only 10-K (annual) filings are used for financial metrics
- Multiple XBRL concept names map to the same canonical metric
- All data includes SEC accession numbers for audit trail

**Rate Limiting:** SEC requires ≤10 requests/second with User-Agent header.

### Yahoo Finance

**Source:** Yahoo Finance API

Historical stock prices are pulled at daily granularity.

**Key Points:**
- Adjusted close prices account for splits and dividends
- 10 years of daily data (~2,514 trading days)

---

## Risk Metrics

### Sharpe Ratio

**Formula:**
```
Sharpe = (Rp - Rf) / σp
```

Where:
- Rp = Annualized portfolio return
- Rf = Risk-free rate (default: 0%)
- σp = Annualized volatility

**Interpretation:**
| Value | Rating |
|-------|--------|
| < 1.0 | Suboptimal |
| 1.0 - 2.0 | Good |
| 2.0 - 3.0 | Very Good |
| > 3.0 | Excellent |

**Reference:** Sharpe, W.F. (1966). "Mutual Fund Performance"

---

### Sortino Ratio

**Formula:**
```
Sortino = (Rp - Rf) / σd
```

Where:
- σd = Downside deviation (std of negative returns only)

**Key Difference from Sharpe:** Only penalizes downside volatility. Upside volatility is not considered "risk."

**Reference:** Sortino, F.A. & van der Meer, R. (1991). "Downside Risk"

---

### Value at Risk (VaR)

**Method:** Historical simulation

**Formula:**
```
VaR(α) = Percentile(returns, 1-α)
```

**Example:**
- VaR(95%) = 5th percentile of daily returns
- Interpretation: "95% of days, losses will not exceed this amount"

**Levels Used:**
- 95% confidence (standard)
- 99% confidence (stress scenario)

**Reference:** Basel Committee on Banking Supervision

---

### Maximum Drawdown

**Formula:**
```
MDD = min((Pt - Pmax) / Pmax)
```

Where:
- Pt = Price at time t
- Pmax = Cumulative maximum price up to time t

**Interpretation:** Largest peak-to-trough decline. Measures worst-case scenario for an investor who bought at the peak.

---

### Volatility

**Formula:**
```
σ = std(daily returns) × √252
```

**Annualization:** Multiplying by √252 converts daily volatility to annual (252 trading days/year).

---

## Financial Metrics Extraction

### XBRL Concept Mapping

Financial metrics can appear under different XBRL concept names. The pipeline maps these to canonical names:

| Canonical | XBRL Concepts |
|-----------|---------------|
| Revenue | Revenues, RevenueFromContractWithCustomerExcludingAssessedTax, SalesRevenueNet |
| Net Income | NetIncomeLoss, ProfitLoss |
| Total Assets | Assets |
| Equity | StockholdersEquity, StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest |
| EPS | EarningsPerShareBasic, EarningsPerShareDiluted |

### Filing Selection

Only Form 10-K (annual reports) are used. The most recent filing by period end date is selected.

---

## Quality Assurance

### Data Validation

1. **Completeness:** All required fields present
2. **Reasonableness:** Values within expected ranges
3. **Consistency:** Cross-validation between sources

### Audit Trail

Every data point includes:
- Source URL
- SEC accession number (for financials)
- Extraction timestamp

---

## Limitations

1. **Historical Data:** Past performance does not predict future results
2. **XBRL Coverage:** Not all financial metrics are available in XBRL for all companies
3. **Adjusted Prices:** Yahoo Finance adjustments may differ from other sources
4. **Risk-Free Rate:** Default of 0% simplifies comparison but may not reflect actual rates

---

## References

1. Sharpe, W.F. (1966). "Mutual Fund Performance." Journal of Business.
2. Sortino, F.A. & van der Meer, R. (1991). "Downside Risk." Journal of Portfolio Management.
3. Basel Committee on Banking Supervision. "Minimum Capital Requirements for Market Risk."
4. SEC EDGAR. "Company Facts API Documentation."
