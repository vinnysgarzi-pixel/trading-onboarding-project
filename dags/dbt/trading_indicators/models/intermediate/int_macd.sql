-- MACD (12/26 EMA spread) with a 9-period signal line. EMAs are computed as
-- truncated, renormalized exponentially-weighted sums over a 60-row window,
-- which avoids recursive SQL and is accurate to well under a cent once the
-- window is warm. Warm-up rows are filtered out downstream in the mart
-- (it keeps only rows where SMA-200 exists, i.e. row 200+).
with ranked as (
    select symbol, price_date, close, rn
    from {{ ref('stg_stock_prices') }}
),

ema_base as (
    select
        a.symbol,
        a.price_date,
        a.rn,
        sum(b.close * power(1 - 2.0 / 13, a.rn - b.rn))
            / sum(power(1 - 2.0 / 13, a.rn - b.rn)) as ema_12,
        sum(b.close * power(1 - 2.0 / 27, a.rn - b.rn))
            / sum(power(1 - 2.0 / 27, a.rn - b.rn)) as ema_26
    from ranked a
    join ranked b
        on b.symbol = a.symbol
        and b.rn between a.rn - 59 and a.rn
    group by a.symbol, a.price_date, a.rn
),

macd_line as (
    select
        symbol,
        price_date,
        rn,
        ema_12 - ema_26 as macd
    from ema_base
)

select
    a.symbol,
    a.price_date,
    round(a.macd, 4) as macd,
    round(
        sum(b.macd * power(1 - 2.0 / 10, a.rn - b.rn))
            / sum(power(1 - 2.0 / 10, a.rn - b.rn)),
        4
    ) as macd_signal,
    round(
        a.macd
        - sum(b.macd * power(1 - 2.0 / 10, a.rn - b.rn))
            / sum(power(1 - 2.0 / 10, a.rn - b.rn)),
        4
    ) as macd_histogram
from macd_line a
join macd_line b
    on b.symbol = a.symbol
    and b.rn between a.rn - 29 and a.rn
group by a.symbol, a.price_date, a.rn, a.macd
