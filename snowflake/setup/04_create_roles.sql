-- ============================================================
-- 04_create_roles.sql
-- Purpose: Create roles for ingestion and dbt
-- Run as: ACCOUNTADMIN
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- LOADER: used by ingestion scripts to write raw data
CREATE ROLE IF NOT EXISTS LOADER
    COMMENT = 'Used by ingestion scripts — writes to RAW schema only';

-- TRANSFORMER: used by dbt to read RAW and write STAGING + MARTS
CREATE ROLE IF NOT EXISTS TRANSFORMER
    COMMENT = 'Used by dbt — reads RAW, writes STAGING and MARTS';

-- Grant roles to your user so you can switch into them
GRANT ROLE LOADER      TO USER DINHTHANHTRUNG;
GRANT ROLE TRANSFORMER TO USER DINHTHANHTRUNG;