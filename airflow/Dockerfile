# ============================================================
# Dockerfile
# Purpose: Custom Airflow image with our pipeline dependencies
# ============================================================

FROM apache/airflow:2.10.5-python3.11

# Switch to root to install system dependencies
USER root

RUN apt-get update && apt-get install -y \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user for pip installs
USER airflow

# Copy and install our requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt