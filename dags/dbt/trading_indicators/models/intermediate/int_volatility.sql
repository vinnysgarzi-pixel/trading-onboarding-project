-- 20-day realized volatility, annualized (sqrt(252)) and expressed in
-- percent. This is the explicit risk factor in the composite score; the
-- Bollinger model uses stddev only to normalize price position.
with returns as (
    select
        symbol,
        price_date,
        close / nullif(lag(close) over (partition by symbol order by price_date), 0) - 1
            as daily_return
    from {{ ref('stg_stock_prices') }}
),

stats as (
    select
        symbol,
        price_date,
        stddev(daily_return) over (
            partition by symbol order by price_date
            rows between 19 preceding and current row
        ) as return_sd,
        count(daily_return) over (
            partition by symbol order by price_date
            rows between 19 preceding and current row
        ) as observed_periods
    from returns
)

select
    symbol,
    price_date,
    case
        when observed_periods >= 20
        then round(return_sd * sqrt(252) * 100, 2)
    end as volatility_20
from stats
