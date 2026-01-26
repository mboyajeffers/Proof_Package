# I Analyzed the Streaming Wars with 23.6 Million Rows of Market Data

**A Portfolio Project: Netflix Competitive Landscape Analysis**

---

The "Streaming Wars" is one of the biggest stories in media right now. Disney+ vs Netflix vs Max vs Prime Video vs Apple TV+ - every earnings call moves stocks.

I wanted to understand the correlation structure behind these movements. So I built a pipeline that processes **23.6 million rows of real market data** to answer: how do streaming stocks actually move together?

---

## The Question

When Netflix announces subscriber numbers, what else moves? When Disney+ bundles change, does it affect Netflix? How does the market actually think about streaming as a sector?

Correlation analysis answers these questions with data, not opinions.

---

## What I Built

**Data Sources:**
- Yahoo Finance: Stock prices, options chains (39 tickers)
- SEC EDGAR: Company filings, XBRL financial data
- FRED: Federal Reserve economic indicators

**Securities Universe:**
- Netflix (NFLX) as the primary focus
- 8 streaming competitors (DIS, WBD, CMCSA, AMZN, AAPL, ROKU, FUBO)
- 6 tech platforms (GOOGL, META, MSFT, CRM, ADBE, SPOT)
- 8 content/media companies (SONY, FOXA, AMC, CNK, etc.)
- 5 telecom providers (T, VZ, TMUS, CHTR, CABO)
- Market and sector ETFs

**Analysis:**
- Rolling correlations across 8 time windows (10 to 252 days)
- 741 unique ticker pairs analyzed
- 21.3 million correlation data points computed

---

## The Numbers

| Metric | Value |
|--------|-------|
| Total Rows Processed | 23,622,884 |
| Data Volume | 1.5 GB |
| Correlation Pairs | 741 |
| Processing Time | ~3 minutes |

The largest single file: **21.3 million rows** of rolling correlations.

---

## Key Findings

### Netflix trades as a tech stock, not a media stock

NFLX-QQQ correlation: **0.80-0.90**

This is higher than NFLX correlations with traditional media like Disney or Warner Bros Discovery. Investors view Netflix as part of the tech sector, not legacy entertainment.

### Streaming sentiment is real

NFLX-ROKU correlation: **0.75-0.85**

When streaming sentiment shifts, these stocks move together. Useful for pairs trading and understanding sector risk.

### Direct competitors are moderately correlated

NFLX-DIS correlation: **0.65-0.75**

Disney and Netflix compete for subscribers, but they're not perfectly correlated. Different investor bases, different business models (parks vs pure streaming).

### Telecom offers diversification

NFLX-T correlation: **0.25-0.35**
NFLX-VZ correlation: **0.20-0.30**

If you're long Netflix and want to hedge, telecom names provide meaningful diversification. They're in the same content delivery ecosystem but move independently.

---

## Why This Matters

This is the type of analysis that:
- **Portfolio managers** use to understand sector concentration
- **Risk teams** use to build correlation-aware hedges
- **Equity analysts** use to identify pairs trading opportunities
- **Corporate strategy teams** use to benchmark competitive dynamics

I built it from scratch using public APIs, demonstrating I can handle:
- Multi-source data integration
- Scale (23.6M rows, 1.5GB)
- Financial domain knowledge
- End-to-end automation

---

## Technical Approach

The challenge: computing rolling correlations for 741 ticker pairs across 8 time windows without running out of memory.

The solution: incremental computation with streaming writes.

```python
for ticker1 in tickers:
    for ticker2 in tickers[i+1:]:
        for window in windows:
            corr = returns[ticker1].rolling(window).corr(returns[ticker2])
            # Stream to file instead of holding in memory
```

This pattern let me process 1.5GB of data on a machine with limited RAM.

---

## The Code

Full source code on GitHub: [link to repo]

Three main scripts:
1. `pull_netflix_data.py` - Initial data collection (1.8M rows)
2. `expand_netflix_data.py` - Full correlation expansion (23.6M rows)
3. `generate_netflix_reports.py` - PDF report generation

---

## Looking for Work

I'm seeking Data Engineer or Analytics Engineer roles where I can apply these skills to real business problems.

What I bring:
- Can work with real APIs, not just Kaggle datasets
- Handle scale that matters (millions of rows)
- Understand financial/market data domains
- Build end-to-end pipelines, not just Jupyter notebooks

If your team needs someone who can build data infrastructure from scratch, let's talk.

---

**Mboya Jeffers**
Data Engineer & Analyst
[cleanmetricsstudios.com](https://cleanmetricsstudios.com)
MboyaJeffers9@gmail.com

---

*The PDF reports and full methodology are available in the GitHub repo.*

---

#dataengineering #python #streaming #netflix #portfolioproject #analytics #fintech #datascience #opentowork #jobsearch
