-- Prices are loaded by the market_data_ingest DAG with Alpaca's
-- adjustment=all, so close is already split/dividend adjusted.
select
    symbol,
    price_date,
    close,
    volume,
    row_number() over (partition by symbol order by price_date) as rn
from stock_prices
