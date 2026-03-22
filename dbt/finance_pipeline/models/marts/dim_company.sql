-- ============================================================
-- dim_company.sql
-- Layer:   Gold / Marts
-- Source:  STAGING.stg_company_profiles
-- Purpose: Company dimension table for joins in analytics
-- ============================================================

with staging as (

    select * from {{ ref('stg_company_profiles') }}

)

select
    -- primary key
    ticker,

    -- descriptive attributes
    company_name,
    sector,
    exchange,
    country,
    currency,
    ipo_date,

    -- metrics
    market_cap_millions,
    market_cap_tier,

    -- metadata
    source_system,
    extracted_at,
    loaded_at

from staging