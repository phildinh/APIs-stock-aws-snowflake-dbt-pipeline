# Stock Market Data Pipeline

A production-grade data engineering portfolio project that ingests stock market data from multiple APIs, stores it in AWS S3 and Snowflake, transforms it with dbt, orchestrates it with Apache Airflow, and runs automated CI/CD with GitHub Actions.

---

## Architecture
```
yfinance API          Finnhub API
     │                     │
     ▼                     ▼
┌─────────────────────────────────┐
│     Python Ingestion Scripts    │
│  fetch_stock_prices.py          │
│  fetch_company_profiles.py      │
└────────────┬────────────────────┘
             │
     ┌───────┴───────┐
     ▼               ▼
 AWS S3           Snowflake
 (Raw JSON        RAW Schema
  Backup)         (Bronze)
                     │
                     ▼
              ┌─────────────┐
              │  dbt Core   │
              │  Staging    │ ← Silver Layer (views)
              │  Marts      │ ← Gold Layer (tables)
              └─────────────┘
                     │
                     ▼
             Apache Airflow
           (Daily Orchestration)
                     │
                     ▼
           GitHub Actions CI/CD
          (dbt build on every PR)
```

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python 3.11 | API calls, data extraction |
| Storage | AWS S3 | Raw JSON backup |
| Warehouse | Snowflake | Cloud data warehouse |
| Transformation | dbt Core 1.8 | Data modelling and testing |
| Orchestration | Apache Airflow 2.10 | Daily pipeline scheduling |
| Containerisation | Docker | Reproducible environments |
| CI/CD | GitHub Actions | Automated dbt testing on PRs |
| Version Control | Git + GitHub | Source control |

---

## Data Sources

| API | Data | Frequency |
|---|---|---|
| yfinance | Daily OHLCV stock prices | Daily |
| Finnhub | Company profiles | On change |

**10 tickers across 6 sectors:**
`AAPL` `MSFT` `NVDA` `GOOGL` `AMZN` `JPM` `JNJ` `XOM` `CAT` `KO`

---

## Data Model

### Medallion Architecture
```
Bronze (RAW)      → Raw data exactly as received from APIs
Silver (STAGING)  → Cleaned, typed, enriched with business metrics
Gold (MARTS)      → Business-ready tables for analytics
```

### Tables

**Bronze Layer**
- `RAW.RAW_STOCK_PRICES` — 5,010 rows, daily OHLCV prices
- `RAW.RAW_COMPANY_PROFILES` — 10 rows, one per ticker

**Silver Layer**
- `STAGING.STG_STOCK_PRICES` — cleaned prices + `daily_return_pct`
- `STAGING.STG_COMPANY_PROFILES` — cleaned profiles + `market_cap_tier`

**Gold Layer**
- `MARTS.FCT_STOCK_PRICES` — incremental fact table, enriched with sector and market cap tier
- `MARTS.DIM_COMPANY` — company dimension table for analytics joins

---

## Project Structure
```
APIs-stock-aws-snowflake-dbt-pipeline/
├── .github/
│   └── workflows/
│       └── dbt_ci.yml          ← CI/CD — runs dbt build on every PR
├── ingestion/
│   ├── __init__.py
│   ├── fetch_stock_prices.py   ← yfinance → S3 → Snowflake
│   ├── fetch_company_profiles.py ← Finnhub → S3 → Snowflake
│   ├── fetch_financials.py     ← disabled (Yahoo Finance API restrictions)
│   └── utils/
│       ├── __init__.py
│       ├── logger.py           ← centralised logging
│       ├── snowflake_client.py ← Snowflake connection + queries
│       ├── s3_client.py        ← AWS S3 upload/read
│       ├── yfinance_client.py  ← stock price fetcher
│       ├── finnhub_client.py   ← company profile fetcher
│       └── fmp_client.py       ← disabled (replaced by yfinance)
├── dbt/
│   └── finance_pipeline/
│       ├── dbt_project.yml
│       ├── models/
│       │   ├── staging/        ← Silver layer models
│       │   └── marts/          ← Gold layer models
│       ├── macros/
│       │   └── generate_schema_name.sql
│       └── tests/              ← Custom business logic tests
├── snowflake/
│   └── setup/                  ← Infrastructure as code SQL scripts
├── airflow/
│   ├── dags/
│   │   └── stock_pipeline.py   ← Daily DAG
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
├── .env                        ← Never committed
├── .gitignore
└── requirements.txt
```

---

## Key Engineering Decisions

**Idempotent ingestion** — Stock prices use DELETE + INSERT so re-running on the same day never creates duplicates. Company profiles use MERGE on ticker so updates are handled cleanly.

**Full load then incremental** — First pipeline run loads 2 years of stock price history. Subsequent runs only load new rows based on `MAX(trade_date)`, keeping compute costs low.

**Medallion architecture** — Three-layer data design (Bronze/Silver/Gold) ensures raw data is always preserved, transformations are auditable, and analysts always query clean, tested data.

**Role-based access control** — `LOADER` role handles ingestion (writes to RAW only). `TRANSFORMER` role handles dbt (reads RAW, writes STAGING and MARTS). No cross-contamination between layers.

**dbt testing** — 27 tests covering nullability, uniqueness, accepted values, and custom business logic (close price > 0, daily return within ±75%, volume > 0).

**CI/CD** — Every pull request to `develop` triggers a GitHub Actions workflow that runs `dbt build` against Snowflake, preventing broken code from ever being merged.

---

## dbt Tests

| Test Type | Count | Examples |
|---|---|---|
| not_null | 9 | ticker, trade_date, close_price |
| unique | 2 | ticker in dim_company |
| accepted_values | 4 | market_cap_tier, ticker universe |
| Custom / singular | 3 | close_price > 0, daily_return_pct within ±75%, volume > 0 |
| **Total** | **27** | All passing ✅ |

---

## Airflow DAG

Daily schedule: weekdays at 6am AEDT (UTC+11)
```
fetch_company_profiles
        ↓
fetch_stock_prices
        ↓
dbt build (--target prod)
        ↓
pipeline_complete
```

---

## Environment Setup

### Prerequisites
- Python 3.11
- Docker Desktop
- Snowflake account
- AWS account (S3)
- Finnhub API key

### Local Setup
```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/APIs-stock-aws-snowflake-dbt-pipeline.git
cd APIs-stock-aws-snowflake-dbt-pipeline

# Create virtual environment
python -m venv venv
venv\Scripts\Activate  # Windows
source venv/bin/activate  # Mac/Linux

# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp .env.example .env
# Fill in your credentials in .env
```

### Snowflake Setup

Run the SQL scripts in order:
```bash
snowflake/setup/01_create_warehouse.sql
snowflake/setup/02_create_databases.sql
snowflake/setup/03_create_schemas.sql
snowflake/setup/04_create_roles.sql
snowflake/setup/05_grant_permissions.sql
```

### dbt Setup
```bash
cd dbt/finance_pipeline
dbt debug      # verify connection
dbt build      # run all models and tests
```

### Run Ingestion
```bash
python -m ingestion.fetch_company_profiles
python -m ingestion.fetch_stock_prices
```

### Run Airflow (Docker)
```bash
docker-compose up --build
# Access Airflow UI at http://localhost:8080
```

---

## Environments

| Environment | Database | Used By |
|---|---|---|
| Dev | `STOCK_DB_DEV` | Local development |
| Prod | `STOCK_DB_PROD` | Airflow daily runs |

Switch environments with: `dbt build --target prod`

---

## Known Limitations

Yahoo Finance deprecated free access to fundamental data (income statements) in early 2026. `fetch_financials.py` is retained in the codebase for reference but disabled. A paid API such as FMP or Polygon.io would be required to re-enable this data source.

---

## Author

**Phil Dinh**
Sydney, Australia