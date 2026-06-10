select
    symbol,
    price_date,
    case
        when count(*) over (
            partition by symbol order by price_date
            rows between 19 preceding and current row
        ) >= 20
        then avg(close) over (
            partition by symbol order by price_date
            rows between 19 preceding and current row
        )
    end as sma_20,
    case
        when count(*) over (
            partition by symbol order by price_date
            rows between 49 preceding and current row
        ) >= 50
        then avg(close) over (
            partition by symbol order by price_date
            rows between 49 preceding and current row
        )
    end as sma_50,
    case
        when count(*) over (
            partition by symbol order by price_date
            rows between 199 preceding and current row
        ) >= 200
        then avg(close) over (
            partition by symbol order by price_date
            rows between 199 preceding and current row
        )
    end as sma_200
from {{ ref('stg_stock_prices') }}
