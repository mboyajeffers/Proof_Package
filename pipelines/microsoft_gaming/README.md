# Microsoft Gaming Analytics Pipeline

Financial and stock performance analysis of Microsoft's gaming division (Xbox, Activision Blizzard).

## Overview

This pipeline extracts financial data from SEC EDGAR and stock prices from Yahoo Finance, then calculates risk-adjusted performance metrics following CFA/Basel standards.

## Results Summary

| Metric | Value |
|--------|-------|
| Revenue (2025) | $281.7B |
| Net Income | $101.8B |
| Total Return (10Y) | +798.2% |
| Annualized Return | +24.6% |
| Sharpe Ratio | 0.92 |
| Sortino Ratio | 1.24 |
| Max Drawdown | -37.1% |

## Data Sources

| Source | Verification |
|--------|--------------|
| Financials | [SEC EDGAR XBRL](https://data.sec.gov/api/xbrl/companyfacts/CIK0000789019.json) |
| Stock Prices | Yahoo Finance (MSFT) |

## Files

```
microsoft_gaming/
├── __init__.py
├── pipeline.py          # Main orchestration
├── sec_client.py        # SEC EDGAR API client
├── yahoo_client.py      # Yahoo Finance client
├── risk_metrics.py      # Sharpe, Sortino, VaR calculations
├── config.yaml          # Configuration
├── requirements.txt     # Dependencies
└── tests/
    └── test_risk_metrics.py
```

## Usage

```bash
cd pipelines/microsoft_gaming
pip install -r requirements.txt
python -m pipeline --ticker MSFT --years 10
```

## Risk Metrics

- **Sharpe Ratio:** Risk-adjusted return (excess return / volatility)
- **Sortino Ratio:** Downside risk-adjusted (only penalizes negative volatility)
- **VaR (95%, 99%):** Value at Risk at confidence levels
- **Max Drawdown:** Largest peak-to-trough decline

See [MICROSOFT_GAMING_ARCHITECTURE.md](/docs/architecture/MICROSOFT_GAMING_ARCHITECTURE.md) for full methodology.

## Author

Mboya Jeffers — Data Engineer
