-- One row per symbol per trading day with every technical indicator the
-- trade advisor consumes. Rows are kept once the 20-day window family
-- (RSI, Bollinger, volume) is warm; longer-window indicators (SMA-50,
-- SMA-200) stay NULL until their windows fill, and the advisor picks an
-- evaluation tier per ticker based on history_days.
select
    base.symbol,
    base.price_date,
    base.rn as history_days,
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
    vol.volume_ratio,
    vola.volatility_20
from {{ ref('stg_stock_prices') }} as base
join {{ ref('int_sma_trend') }} as trend using (symbol, price_date)
join {{ ref('int_rsi') }} as rsi using (symbol, price_date)
join {{ ref('int_macd') }} as macd using (symbol, price_date)
join {{ ref('int_bollinger') }} as boll using (symbol, price_date)
join {{ ref('int_volume') }} as vol using (symbol, price_date)
join {{ ref('int_volatility') }} as vola using (symbol, price_date)
where rsi.rsi_14 is not null
    and boll.bollinger_z is not null
    and vol.volume_ratio is not null
    and vola.volatility_20 is not null
