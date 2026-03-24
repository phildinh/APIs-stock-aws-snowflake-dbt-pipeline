# Stock Market Data Pipeline
### End-to-end data engineering portfolio project — Python · AWS S3 · Snowflake · dbt · Airflow · Docker · GitHub Actions

---

## What This Project Does

Most companies have raw data sitting in APIs and databases that nobody can actually use — because it hasn't been cleaned, modelled, or made reliable enough for analysis.

This project solves that problem by building a **production-grade data pipeline** that:
- Pulls daily stock market data from external APIs automatically
- Stores raw data in AWS S3 (backup) and Snowflake (warehouse)
- Transforms raw data into clean, tested, business-ready tables using dbt
- Runs the entire pipeline on a daily schedule via Apache Airflow
- Tests every code change automatically via GitHub Actions CI/CD

The result: a fully automated pipeline that delivers clean, reliable stock market data every trading day — with zero manual intervention.

---

## Live Stats

| Metric | Value |
|---|---|
| Tickers tracked | 10 across 6 sectors |
| Raw rows loaded | 5,010 (2 years of daily prices) |
| dbt models | 4 (2 staging + 2 marts) |
| dbt tests passing | 27 / 27 ✅ |
| CI/CD | Automated on every PR |
| Environments | Dev + Prod (Snowflake) |

---

## Tech Stack

| Layer | Tool | Why I chose it |
|---|---|---|
| Language | Python 3.11 | Industry standard for data engineering |
| Data warehouse | Snowflake | Cloud-native, scales instantly, separation of storage and compute |
| Raw storage | AWS S3 | Cheap, durable backup of all raw API responses |
| Transformation | dbt Core 1.8 | SQL-based transformations with built-in testing and documentation |
| Orchestration | Apache Airflow 2.10 | Industry-standard scheduler, visual DAG monitoring |
| Containerisation | Docker | Reproducible environments — runs identically on any machine |
| CI/CD | GitHub Actions | Automated dbt testing on every pull request |
| Stock prices | yfinance | Free, reliable OHLCV data for major US equities |
| Company profiles | Finnhub API | Free tier provides company metadata including sector and market cap |

---

## Architecture Overview
```
┌─────────────────────────────────────────────────┐
│               DATA SOURCES                       │
│   yfinance (stock prices)  Finnhub (profiles)   │
└─────────────────┬───────────────────────────────┘
                  │ Python ingestion scripts
                  ▼
┌─────────────────────────────────────────────────┐
│              BRONZE LAYER                        │
│   AWS S3 (raw JSON backup)                       │
│   Snowflake RAW schema (raw tables)              │
└─────────────────┬───────────────────────────────┘
                  │ dbt Core
                  ▼
┌─────────────────────────────────────────────────┐
│              SILVER LAYER                        │
│   Snowflake STAGING schema                       │
│   stg_stock_prices   — cleaned + daily_return    │
│   stg_company_profiles — cleaned + market_cap_tier│
└─────────────────┬───────────────────────────────┘
                  │ dbt Core
                  ▼
┌─────────────────────────────────────────────────┐
│               GOLD LAYER                         │
│   Snowflake MARTS schema                         │
│   fct_stock_prices — incremental fact table      │
│   dim_company      — company dimension           │
└─────────────────────────────────────────────────┘
                  │
        Orchestrated daily by Airflow
        Tested on every PR by GitHub Actions
```

---

## Data Model

### Medallion Architecture (Bronze → Silver → Gold)

**Why three layers?** Raw data is preserved untouched in Bronze so nothing is ever lost. Silver cleans and standardises it so downstream models have a reliable foundation. Gold builds business-ready tables that answer real analytical questions.

| Layer | Schema | Tables | Purpose |
|---|---|---|---|
| Bronze | RAW | `raw_stock_prices`, `raw_company_profiles` | Raw data as received from APIs |
| Silver | STAGING | `stg_stock_prices`, `stg_company_profiles` | Cleaned, typed, enriched |
| Gold | MARTS | `fct_stock_prices`, `dim_company` | Business-ready for analytics |

### Business Metrics Added in dbt
- `daily_return_pct` — percentage price change from open to close each day
- `market_cap_tier` — classifies each company as Mega Cap / Large Cap / Mid Cap / Small Cap

### Fact and Dimension Design
- `fct_stock_prices` — **incremental** table, new rows appended daily, joined to dim_company for sector and market cap context
- `dim_company` — **full refresh** table, one row per ticker, source of truth for company attributes

---

## Engineering Decisions

### Idempotent Ingestion
Re-running the pipeline on the same day never creates duplicate data.
- Stock prices: `DELETE` rows for the date range → `INSERT` fresh rows
- Company profiles: `MERGE` on ticker — updates existing, inserts new

### Full Load Then Incremental
- First run loads 2 years of historical stock prices (730 days)
- Every subsequent run only loads new rows using `MAX(trade_date)` as the checkpoint
- Keeps Snowflake compute costs minimal on daily runs

### Role-Based Access Control
Two Snowflake roles with strict separation:
- `LOADER` — ingestion scripts only. Writes to RAW. Cannot touch STAGING or MARTS.
- `TRANSFORMER` — dbt only. Reads RAW, writes STAGING and MARTS. Cannot call APIs.

### dbt Testing Strategy
27 tests across four categories:

| Test Type | Count | What It Catches |
|---|---|---|
| `not_null` | 9 | Missing data from API failures |
| `unique` | 2 | Duplicate rows from idempotency bugs |
| `accepted_values` | 4 | Invalid tickers or market cap tiers |
| Custom singular | 3 | Business logic violations (negative prices, unrealistic returns) |

### Infrastructure as Code
All Snowflake objects (warehouses, databases, schemas, roles, permissions) are created via versioned SQL scripts in `snowflake/setup/`. Nothing is clicked manually in the UI.

---

## CI/CD Pipeline

Every pull request to `develop` or `main` triggers this workflow automatically:
```
PR opened
    ↓
GitHub Actions spins up Ubuntu runner
    ↓
Installs Python 3.11 + dbt-snowflake
    ↓
Creates profiles.yml from GitHub Secrets (no hardcoded credentials)
    ↓
Runs dbt build (all models + all 27 tests)
    ↓
✅ Pass → PR can be merged
❌ Fail → PR is blocked
```

This means broken SQL or failed tests can never reach the main branch.

---

## Airflow DAG

**Schedule:** Weekdays at 6:00am AEDT (Monday–Friday)
```
fetch_company_profiles    ← runs first, rarely changes
         ↓
fetch_stock_prices        ← daily OHLCV data
         ↓
dbt build --target prod   ← transforms + tests all layers
         ↓
pipeline_complete         ← confirmation log
```

---

## Challenges and How I Solved Them

**Yahoo Finance API restrictions (2026)**
Yahoo Finance significantly restricted free API access to fundamental data (income statements) in early 2026, returning 403 errors regardless of API key. Rather than blocking the project, I made a pragmatic decision to disable that data source and focus on the two working APIs. The code is retained in the repo for reference. A paid provider (FMP, Polygon.io) would re-enable this with a single config change.

**dbt schema naming conflict**
By default, dbt concatenates the target schema with the custom schema name — creating `RAW_STAGING` instead of `STAGING`. Fixed by writing a custom `generate_schema_name` macro that overrides dbt's default behaviour and uses the exact schema name defined in `dbt_project.yml`.

**Python import path resolution**
Running ingestion scripts from the project root caused `ModuleNotFoundError` for `utils` imports. Resolved by switching to fully qualified imports (`from ingestion.utils.logger import get_logger`) — making the import path explicit and unambiguous regardless of where the script is executed from.

**Docker dependency conflicts**
`dbt-snowflake==1.8.0` and `apache-airflow==2.10.5` have conflicting `dbt-adapters` and `dbt-common` dependency trees, causing pip's resolver to time out after 200,000 rounds. Resolved by pinning the full dbt dependency chain explicitly in `airflow/requirements.txt`.

---

## Project Structure
```
APIs-stock-aws-snowflake-dbt-pipeline/
├── .github/
│   └── workflows/
│       └── dbt_ci.yml              ← GitHub Actions CI/CD
├── ingestion/
│   ├── __init__.py
│   ├── fetch_stock_prices.py       ← yfinance → S3 → Snowflake
│   ├── fetch_company_profiles.py   ← Finnhub → S3 → Snowflake
│   ├── fetch_financials.py         ← disabled (API restrictions)
│   └── utils/
│       ├── logger.py               ← centralised logging (console + file)
│       ├── snowflake_client.py     ← connection, execute, fetch
│       ├── s3_client.py            ← upload, read, list
│       ├── yfinance_client.py      ← OHLCV price fetcher
│       └── finnhub_client.py       ← company profile fetcher
├── dbt/
│   └── finance_pipeline/
│       ├── dbt_project.yml
│       ├── models/
│       │   ├── staging/            ← Silver layer (views)
│       │   └── marts/              ← Gold layer (tables)
│       ├── macros/
│       │   └── generate_schema_name.sql
│       └── tests/                  ← Custom business logic tests
├── snowflake/
│   └── setup/                      ← Infrastructure as code
│       ├── 01_create_warehouse.sql
│       ├── 02_create_databases.sql
│       ├── 03_create_schemas.sql
│       ├── 04_create_roles.sql
│       └── 05_grant_permissions.sql
├── airflow/
│   ├── dags/
│   │   └── stock_pipeline.py       ← Daily DAG definition
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
├── .env                            ← Never committed
├── .gitignore
└── requirements.txt
```

---

## Local Setup

### Prerequisites
- Python 3.11+
- Docker Desktop
- Snowflake account (free trial works)
- AWS account with S3 bucket
- Finnhub API key (free tier)

### Steps
```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/APIs-stock-aws-snowflake-dbt-pipeline.git
cd APIs-stock-aws-snowflake-dbt-pipeline

# 2. Create and activate virtual environment
python -m venv venv
venv\Scripts\Activate       # Windows
source venv/bin/activate    # Mac/Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure credentials
# Copy .env.example to .env and fill in your values
# Never commit .env to Git

# 5. Set up Snowflake (run in order)
# snowflake/setup/01_create_warehouse.sql
# snowflake/setup/02_create_databases.sql
# snowflake/setup/03_create_schemas.sql
# snowflake/setup/04_create_roles.sql
# snowflake/setup/05_grant_permissions.sql

# 6. Verify dbt connection
cd dbt/finance_pipeline
dbt debug

# 7. Run ingestion
python -m ingestion.fetch_company_profiles
python -m ingestion.fetch_stock_prices

# 8. Run dbt
dbt build

# 9. Start Airflow (Docker)
docker-compose up --build
# UI available at http://localhost:8080
```

---

## Tickers

| Sector | Tickers |
|---|---|
| Technology | AAPL, MSFT, NVDA |
| Communications | GOOGL |
| Consumer | AMZN |
| Financials | JPM |
| Healthcare | JNJ |
| Energy | XOM |
| Industrials | CAT |
| Consumer Staples | KO |

---

## Author

**Phil Dinh**
