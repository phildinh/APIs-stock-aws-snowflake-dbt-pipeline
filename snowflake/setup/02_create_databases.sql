-- ============================================================
-- 02_create_databases.sql
-- Purpose: Create dev and prod databases
-- Run as: ACCOUNTADMIN
-- ============================================================

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS STOCK_DB_DEV
    COMMENT = 'Development environment for stock pipeline';

CREATE DATABASE IF NOT EXISTS STOCK_DB_PROD
    COMMENT = 'Production environment for stock pipeline';