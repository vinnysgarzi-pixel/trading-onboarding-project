with stats as (
    select
        symbol,
        price_date,
        volume,
        avg(volume) over (
            partition by symbol order by price_date
            rows between 19 preceding and current row
        ) as volume_avg_20,
        count(*) over (
            partition by symbol order by price_date
            rows between 19 preceding and current row
        ) as observed_periods
    from {{ ref('stg_stock_prices') }}
)

select
    symbol,
    price_date,
    case when observed_periods >= 20 then round(volume_avg_20, 0) end as volume_avg_20,
    case
        when observed_periods >= 20 and volume_avg_20 > 0
        then round(volume / volume_avg_20, 4)
    end as volume_ratio
from stats
