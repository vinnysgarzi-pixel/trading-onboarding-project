-- One row per symbol per trading day with every technical indicator the
-- trade advisor consumes. Only rows with a full SMA-200 window are kept,
-- so every indicator here is fully warmed up.
select
    base.symbol,
    base.price_date,
    base.close,
    base.volume,
    trend.sma_20,
    trend.sma_50,
    trend.sma_200,
    rsi.rsi_14,
    macd.macd,
    macd.macd_signal,
    macd.macd_histogram,
    boll.bollinger_mid,
    boll.bollinger_z,
    vol.volume_avg_20,
    vol.volume_ratio
from {{ ref('stg_stock_prices') }} as base
join {{ ref('int_sma_trend') }} as trend using (symbol, price_date)
join {{ ref('int_rsi') }} as rsi using (symbol, price_date)
join {{ ref('int_macd') }} as macd using (symbol, price_date)
join {{ ref('int_bollinger') }} as boll using (symbol, price_date)
join {{ ref('int_volume') }} as vol using (symbol, price_date)
where trend.sma_200 is not null
