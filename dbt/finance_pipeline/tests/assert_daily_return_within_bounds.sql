-- ============================================================
-- assert_daily_return_within_bounds.sql
-- Purpose: Ensure daily returns are within realistic bounds
-- No stock should move more than 75% in a single day
-- A test PASSES when this query returns 0 rows
-- ============================================================

select
    ticker,
    trade_date,
    daily_return_pct
from {{ ref('fct_stock_prices') }}
where daily_return_pct > 75
   or daily_return_pct < -75