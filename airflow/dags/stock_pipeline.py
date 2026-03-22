# ============================================================
# stock_pipeline.py
# Purpose: Daily stock data pipeline DAG
# Schedule: Every day at 6am Sydney time (UTC+11 = 7pm UTC)
# ============================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner':            'phil',
    'retries':          1,
    'retry_delay':      timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id          = 'stock_pipeline',
    description     = 'Daily stock data ingestion and transformation',
    schedule_interval = '0 19 * * 1-5',  # 7pm UTC = 6am AEDT, weekdays only
    start_date      = datetime(2026, 1, 1),
    catchup         = False,
    default_args    = default_args,
    tags            = ['stock', 'ingestion', 'dbt'],
) as dag:

    # Task 1 — Fetch company profiles (runs first, rarely changes)
    fetch_company_profiles = BashOperator(
        task_id = 'fetch_company_profiles',
        bash_command = 'cd /opt/airflow && python -m ingestion.fetch_company_profiles',
    )

    # Task 2 — Fetch stock prices
    fetch_stock_prices = BashOperator(
        task_id = 'fetch_stock_prices',
        bash_command = 'cd /opt/airflow && python -m ingestion.fetch_stock_prices',
    )

    # Task 3 — Run dbt build (models + tests)
    dbt_build = BashOperator(
        task_id = 'dbt_build',
        bash_command = 'cd /opt/airflow/dbt/finance_pipeline && dbt build --target prod',
    )

    # Task 4 — Final confirmation log
    pipeline_complete = BashOperator(
        task_id = 'pipeline_complete',
        bash_command = 'echo "Stock pipeline completed successfully at $(date)"',
    )

    # Define execution order
    fetch_company_profiles >> fetch_stock_prices >> dbt_build >> pipeline_complete