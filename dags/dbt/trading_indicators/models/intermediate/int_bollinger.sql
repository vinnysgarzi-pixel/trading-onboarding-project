-- 20-day Bollinger statistics. bollinger_z is how many standard deviations
-- the close sits from its 20-day mean (the classic bands are at +/- 2).
with stats as (
    select
        symbol,
        price_date,
        close,
        avg(close) over (
            partition by symbol order by price_date
            rows between 19 preceding and current row
        ) as mid,
        stddev(close) over (
            partition by symbol order by price_date
            rows between 19 preceding and current row
        ) as sd,
        count(*) over (
            partition by symbol order by price_date
            rows between 19 preceding and current row
        ) as observed_periods
    from {{ ref('stg_stock_prices') }}
)

select
    symbol,
    price_date,
    case when observed_periods >= 20 then round(mid, 4) end as bollinger_mid,
    case
        when observed_periods >= 20 and sd > 0
        then round((close - mid) / sd, 4)
    end as bollinger_z
from stats
