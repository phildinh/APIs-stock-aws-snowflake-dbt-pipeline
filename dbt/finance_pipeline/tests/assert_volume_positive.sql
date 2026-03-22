-- ============================================================
-- assert_volume_positive.sql
-- Purpose: Ensure all trading days have positive volume
-- Zero volume means no trades occurred — bad data
-- A test PASSES when this query returns 0 rows
-- ============================================================

select
    ticker,
    trade_date,
    volume
from {{ ref('fct_stock_prices') }}
where volume <= 0