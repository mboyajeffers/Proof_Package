# Mboya Jeffers — Data Engineering Portfolio

Production-grade data pipelines, analytics frameworks, and engineering patterns.
Built for hiring managers and technical leads evaluating data engineering capabilities.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue)](https://linkedin.com/in/mboya-jeffers-6377ba325)
[![Email](https://img.shields.io/badge/Email-MboyaJeffers9%40gmail.com-green)](mailto:MboyaJeffers9@gmail.com)

---

## About Me

Data Engineer with full-stack pipeline ownership — from raw data ingestion through
star schema modeling to automated reporting. I build systems that process real data
from public APIs with quality gates, validation frameworks, and production discipline.

**Target Role:** Senior Data Engineer / Analytics Engineer ($125K-$200K+, Remote)

---

## Portfolio Overview

| Section | What It Demonstrates |
|---------|---------------------|
| [**pipelines/**](#pipelines) | End-to-end ETL with public APIs (SEC, NIST, CMS, EIA, USASpending) |
| [**demos/**](#demos) | Standalone code samples: financial metrics, FX conversion, validation |
| [**reports/**](#reports) | Sample outputs: executive summaries, methodology docs |

---

## Pipelines

Production pipelines using real government and financial APIs. Each pipeline includes:
- Data extraction with rate limiting
- Schema validation and cleaning
- Star schema dimensional modeling
- Quality gates (completeness, uniqueness)
- JSON metrics output

| Pipeline | Data Source | Scale | Verification |
|----------|-------------|-------|--------------|
| **Federal Awards** | USASpending.gov | 100K+ awards | [api.usaspending.gov](https://api.usaspending.gov) |
| **SEC Financial** | EDGAR XBRL | 36 companies, 1M+ facts | [data.sec.gov](https://www.sec.gov/cgi-bin/browse-edgar) |
| **Healthcare Quality** | CMS Hospital Compare | 942 hospitals | [data.cms.gov](https://data.cms.gov/provider-data/) |
| **Energy Grid** | EIA-930 | 7 balancing authorities | [api.eia.gov](https://www.eia.gov/opendata/) |
| **Vulnerability Scoring** | NIST NVD + CISA KEV | 2K CVEs | [nvd.nist.gov](https://nvd.nist.gov/developers) |

```bash
cd pipelines/federal_awards
python pipeline.py
```

---

## Demos

Standalone, interview-ready code demonstrating core data engineering skills.

### financial-metrics/
Risk and portfolio analytics calculations.

```python
from risk_metrics import calculate_all_metrics
metrics = calculate_all_metrics(price_series, benchmark_series)
print(f"Sharpe Ratio: {metrics['sharpe_ratio']:.2f}")
print(f"95% VaR: {metrics['var_95_parametric']:.2%}")
```

**Includes:** VaR (parametric/historical), Sharpe Ratio, Sortino Ratio, Maximum Drawdown, Beta

### etl-pipeline-template/
Configurable ETL framework with quality gates.

```python
from pipeline import ETLPipeline, PipelineConfig

config = PipelineConfig(
    name='my_etl',
    source_type='csv',
    source_path='data/input.csv',
    output_path='data/output.csv',
    schema={'id': 'int', 'amount': 'float'}
)

pipeline = ETLPipeline(config)
metrics = pipeline.run(key_columns=['id'])
```

### multi-currency-fx/
Currency conversion with ECB primary + fallback sources.

```python
from fx_converter import FXConverter

converter = FXConverter()
result = converter.convert(1000, 'USD', 'EUR')
# 1000 USD = 918.45 EUR (rate: 0.91845, source: ECB)
```

### data-validation/
Schema validation and quality scoring framework.

```python
from validator import DataValidator

validator = DataValidator(schema={'id': {'type': 'int', 'nullable': False}})
validator.add_range_check('age', min_val=0, max_val=120)
validator.add_pattern_check('email', r'^[\w\.-]+@[\w\.-]+\.\w+$')

report = validator.validate(df)
print(f"Quality Score: {report.quality_score:.1%}")
```

---

## Technical Skills Demonstrated

| Category | Skills |
|----------|--------|
| **Languages** | Python, SQL |
| **Data** | pandas, numpy, scipy |
| **APIs** | REST, rate limiting, retry logic, caching |
| **Databases** | PostgreSQL, star schema, dimensional modeling |
| **Infrastructure** | GCP, Linux, systemd, nginx |
| **Quality** | Data validation, quality gates, testing (pytest) |
| **DevOps** | Git, CI patterns |

---

## Engineering Practices

- **Schema Validation** — Contract-driven column validation with 150+ field alias mappings
- **Quality Gates** — Completeness, uniqueness, range validation on every pipeline
- **Rate Limiting** — Exponential backoff for API reliability under throttling
- **Star Schema** — Fact and dimension tables with surrogate keys
- **Idempotent Pipelines** — Safe to re-run without side effects
- **Observability** — Structured logging, pipeline metrics, audit trails

---

## Repository Structure

```
Data-Engineering-Portfolio/
├── README.md                     # You are here
├── pipelines/
│   ├── federal_awards/           # USASpending API
│   ├── sec_financial/            # SEC EDGAR XBRL
│   ├── healthcare_quality/       # CMS Hospital Compare
│   ├── energy_grid/              # EIA-930
│   └── vulnerability_scoring/    # NIST NVD + CISA KEV
├── demos/
│   ├── financial-metrics/        # VaR, Sharpe, Sortino, Beta
│   ├── etl-pipeline-template/    # Configurable ETL framework
│   ├── multi-currency-fx/        # Currency conversion
│   └── data-validation/          # Schema validation
├── reports/
│   ├── founder_summaries/        # One-page project summaries
│   └── executive_reports/        # Detailed analysis PDFs
└── _archive/                     # Previous versions
```

---

## Quick Start

```bash
# Clone
git clone https://github.com/mboyajeffers/Data-Engineering-Portfolio.git
cd Data-Engineering-Portfolio

# Install dependencies
pip install pandas numpy scipy requests pytest

# Run a pipeline
cd pipelines/federal_awards && python pipeline.py

# Run a demo
cd demos/financial-metrics && python risk_metrics.py

# Run tests
cd demos/financial-metrics && pytest test_risk_metrics.py -v
```

---

## Contact

**Mboya Jeffers** — Data Engineer

- **Email:** MboyaJeffers9@gmail.com
- **LinkedIn:** [linkedin.com/in/mboya-jeffers-6377ba325](https://linkedin.com/in/mboya-jeffers-6377ba325)
- **Location:** Remote (US-based)

---

## What I'm Looking For

**Target Roles:** Data Engineer, Analytics Engineer, Senior Data Engineer

**Ideal Environment:**
- Companies solving complex data problems at scale
- Teams that value clean architecture and production discipline
- Roles with end-to-end pipeline ownership
- Remote-first organizations

---

*All portfolio data is from public APIs and independently verifiable.*
