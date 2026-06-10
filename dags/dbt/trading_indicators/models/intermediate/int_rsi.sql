-- 14-day RSI using simple moving averages of gains and losses (Cutler's RSI).
with deltas as (
    select
        symbol,
        price_date,
        close - lag(close) over (partition by symbol order by price_date) as price_change
    from {{ ref('stg_stock_prices') }}
),

averaged as (
    select
        symbol,
        price_date,
        avg(case when price_change > 0 then price_change else 0 end) over (
            partition by symbol order by price_date
            rows between 13 preceding and current row
        ) as avg_gain,
        avg(case when price_change < 0 then -price_change else 0 end) over (
            partition by symbol order by price_date
            rows between 13 preceding and current row
        ) as avg_loss,
        count(price_change) over (
            partition by symbol order by price_date
            rows between 13 preceding and current row
        ) as observed_periods
    from deltas
)

select
    symbol,
    price_date,
    case
        when observed_periods < 14 then null
        when avg_loss = 0 then 100
        else round(100 - 100 / (1 + avg_gain / avg_loss), 2)
    end as rsi_14
from averaged
