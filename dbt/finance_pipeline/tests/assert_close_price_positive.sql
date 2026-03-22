-- ============================================================
-- assert_close_price_positive.sql
-- Purpose: Ensure no negative or zero close prices exist
-- A test PASSES when this query returns 0 rows
-- ============================================================

select
    ticker,
    trade_date,
    close_price
from {{ ref('fct_stock_prices') }}
where close_price <= 0