-- ============================================================
-- 03_create_schemas.sql
-- Purpose: Create Bronze/Silver/Gold schemas in both databases
-- Run as: ACCOUNTADMIN
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- Dev schemas
CREATE SCHEMA IF NOT EXISTS STOCK_DB_DEV.RAW
    COMMENT = 'Bronze layer — raw data from APIs via ingestion scripts';

CREATE SCHEMA IF NOT EXISTS STOCK_DB_DEV.STAGING
    COMMENT = 'Silver layer — cleaned and typed data from dbt';

CREATE SCHEMA IF NOT EXISTS STOCK_DB_DEV.MARTS
    COMMENT = 'Gold layer — business-ready models from dbt';

-- Prod schemas
CREATE SCHEMA IF NOT EXISTS STOCK_DB_PROD.RAW
    COMMENT = 'Bronze layer — raw data from APIs via ingestion scripts';

CREATE SCHEMA IF NOT EXISTS STOCK_DB_PROD.STAGING
    COMMENT = 'Silver layer — cleaned and typed data from dbt';

CREATE SCHEMA IF NOT EXISTS STOCK_DB_PROD.MARTS
    COMMENT = 'Gold layer — business-ready models from dbt';