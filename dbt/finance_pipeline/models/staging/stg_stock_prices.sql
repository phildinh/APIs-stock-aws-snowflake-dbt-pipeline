-- ============================================================
-- stg_stock_prices.sql
-- Layer:   Silver / Staging
-- Source:  RAW.RAW_STOCK_PRICES
-- Purpose: Clean raw stock prices and add daily return metric
-- ============================================================

with source as (

    select * from {{ source('raw', 'raw_stock_prices') }}

),

cleaned as (

    select
        -- identifiers
        ticker                              as ticker,
        trade_date                          as trade_date,

        -- prices — round to 2 decimal places for consistency
        round(open_price,  2)               as open_price,
        round(high_price,  2)               as high_price,
        round(low_price,   2)               as low_price,
        round(close_price, 2)               as close_price,

        -- volume
        volume                              as volume,

        -- business metric — daily return percentage
        -- how much did the stock move from open to close that day?
        round(
            (close_price - open_price) / nullif(open_price, 0) * 100,
            4
        )                                   as daily_return_pct,

        -- metadata
        _source                             as source_system,
        _extracted_at                       as extracted_at,
        _loaded_at                          as loaded_at

    from source
    where close_price > 0  -- filter out any bad data

)

select * from cleaned