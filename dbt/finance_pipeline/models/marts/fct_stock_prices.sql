-- ============================================================
-- fct_stock_prices.sql
-- Layer:   Gold / Marts
-- Source:  STAGING.stg_stock_prices + STAGING.stg_company_profiles
-- Purpose: Core fact table for stock price analysis
-- Materialization: Incremental — new rows appended daily
-- ============================================================

{{
    config(
        materialized = 'incremental',
        unique_key   = ['ticker', 'trade_date'],
        on_schema_change = 'sync_all_columns'
    )
}}

with stock_prices as (

    select * from {{ ref('stg_stock_prices') }}

    {% if is_incremental() %}
        -- On incremental runs, only process rows newer than what we already have
        where trade_date > (select max(trade_date) from {{ this }})
    {% endif %}

),

company as (

    select
        ticker,
        sector,
        market_cap_tier
    from {{ ref('stg_company_profiles') }}

),

joined as (

    select
        -- identifiers
        sp.ticker,
        sp.trade_date,

        -- company context
        c.sector,
        c.market_cap_tier,

        -- prices
        sp.open_price,
        sp.high_price,
        sp.low_price,
        sp.close_price,

        -- volume
        sp.volume,

        -- metrics
        sp.daily_return_pct,

        -- metadata
        sp.source_system,
        sp.extracted_at,
        sp.loaded_at

    from stock_prices sp
    left join company c on sp.ticker = c.ticker

)

select * from joined