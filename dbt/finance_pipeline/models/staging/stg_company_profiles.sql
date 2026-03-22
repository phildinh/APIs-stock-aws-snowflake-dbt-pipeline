-- ============================================================
-- stg_company_profiles.sql
-- Layer:   Silver / Staging
-- Source:  RAW.RAW_COMPANY_PROFILES
-- Purpose: Clean company profiles and add market cap tier
-- ============================================================

with source as (

    select * from {{ source('raw', 'raw_company_profiles') }}

),

cleaned as (

    select
        -- identifiers
        ticker                                  as ticker,

        -- company info
        company_name                            as company_name,
        sector                                  as sector,
        exchange                                as exchange,
        country                                 as country,
        currency                                as currency,
        ipo_date                                as ipo_date,

        -- market cap in millions USD
        round(market_cap, 2)                    as market_cap_millions,

        -- business metric — classify company size
        -- used in marts for filtering and grouping
        case
            when market_cap >= 200000  then 'Mega Cap'
            when market_cap >= 10000   then 'Large Cap'
            when market_cap >= 2000    then 'Mid Cap'
            when market_cap >= 300     then 'Small Cap'
            else                            'Micro Cap'
        end                                     as market_cap_tier,

        -- metadata
        _source                                 as source_system,
        _extracted_at                           as extracted_at,
        _loaded_at                              as loaded_at

    from source
    where ticker is not null

)

select * from cleaned