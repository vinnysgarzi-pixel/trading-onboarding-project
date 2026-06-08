"""
# Stock SMA Signal Alerts

Fetches daily adjusted stock prices from Alpha Vantage, stores them in Snowflake,
calculates 50-day and 200-day simple moving averages, and emits alert messages
when a crossover signal appears.

This DAG is intentionally scheduled, not streaming: it runs every 3 hours after
market open on weekdays, and only alerts when a new BUY or SELL signal is
detected.

Required Airflow configuration:
- Snowflake connection: `stock_signal_snowflake`
- Alpha Vantage HTTP connection: `alpha_vantage_default`
  - Host: `https://www.alphavantage.co`
  - Password or Extra JSON `api_key`: your Alpha Vantage API key
- Optional Variable: `tracked_stock_tickers`, JSON array like `["AAPL", "MSFT"]`
- Optional Variable: `stock_alert_webhook_url`, webhook URL for alert delivery
"""

from __future__ import annotations

import json
from datetime import timedelta

from airflow.sdk import BaseHook, Variable, dag, task
from pendulum import datetime

SNOWFLAKE_CONN_ID = "stock_signal_snowflake"
MARKET_DATA_CONN_ID = "alpha_vantage_default"
DEFAULT_TRACKED_TICKERS = ["AAPL", "MSFT", "NVDA"]


@dag(
    dag_id="stock_sma_signals",
    start_date=datetime(2025, 1, 1, tz="America/New_York"),
    schedule="30 9,12,15 * * 1-5",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Astro", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["stocks", "signals", "onboarding"],
    doc_md=__doc__,
)
def stock_sma_signals():
    @task
    def get_ticker_config() -> list[str]:
        configured_tickers = Variable.get("tracked_stock_tickers", default=None)
        if configured_tickers:
            return json.loads(configured_tickers)

        return DEFAULT_TRACKED_TICKERS

    @task
    def initialize_snowflake_tables() -> None:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(
            [
                """
                CREATE TABLE IF NOT EXISTS stock_prices (
                    symbol STRING NOT NULL,
                    price_date DATE NOT NULL,
                    open FLOAT NOT NULL,
                    high FLOAT NOT NULL,
                    low FLOAT NOT NULL,
                    close FLOAT NOT NULL,
                    adjusted_close FLOAT,
                    volume NUMBER NOT NULL,
                    dividend_amount FLOAT,
                    split_coefficient FLOAT,
                    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS stock_signals (
                    symbol STRING NOT NULL,
                    signal_date DATE NOT NULL,
                    signal STRING NOT NULL,
                    close FLOAT NOT NULL,
                    sma_50 FLOAT NOT NULL,
                    sma_200 FLOAT NOT NULL,
                    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
                """,
            ]
        )

    @task
    def fetch_and_store_price_history(symbol: str) -> dict[str, int | str]:
        import requests
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        market_data_conn = BaseHook.get_connection(MARKET_DATA_CONN_ID)
        api_key = market_data_conn.extra_dejson.get("api_key") or market_data_conn.password
        if not api_key:
            raise ValueError(
                f"{MARKET_DATA_CONN_ID} must provide an Alpha Vantage API key in "
                "the password field or Extra JSON as api_key"
            )

        base_url = market_data_conn.host or "https://www.alphavantage.co"
        response = requests.get(
            f"{base_url.rstrip('/')}/query",
            params={
                "function": "TIME_SERIES_DAILY_ADJUSTED",
                "symbol": symbol,
                "outputsize": "full",
                "apikey": api_key,
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()

        if "Error Message" in payload:
            raise ValueError(f"Alpha Vantage rejected symbol {symbol}: {payload['Error Message']}")
        if "Note" in payload:
            raise ValueError(f"Alpha Vantage rate limit reached for {symbol}: {payload['Note']}")

        time_series = payload.get("Time Series (Daily)")
        if not time_series:
            raise ValueError(f"Alpha Vantage response for {symbol} did not include daily prices")

        rows = []
        for price_date, values in sorted(time_series.items())[-260:]:
            rows.append(
                (
                    symbol,
                    price_date,
                    float(values["1. open"]),
                    float(values["2. high"]),
                    float(values["3. low"]),
                    float(values["4. close"]),
                    float(values["5. adjusted close"]),
                    int(values["6. volume"]),
                    float(values["7. dividend amount"]),
                    float(values["8. split coefficient"]),
                )
            )

        if len(rows) < 200:
            raise ValueError(f"{symbol} returned {len(rows)} rows; at least 200 are required")

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    MERGE INTO stock_prices target
                    USING (
                        SELECT
                            %s AS symbol,
                            TO_DATE(%s) AS price_date,
                            %s AS open,
                            %s AS high,
                            %s AS low,
                            %s AS close,
                            %s AS adjusted_close,
                            %s AS volume,
                            %s AS dividend_amount,
                            %s AS split_coefficient
                    ) source
                    ON target.symbol = source.symbol
                    AND target.price_date = source.price_date
                    WHEN MATCHED THEN UPDATE SET
                        open = source.open,
                        high = source.high,
                        low = source.low,
                        close = source.close,
                        adjusted_close = source.adjusted_close,
                        volume = source.volume,
                        dividend_amount = source.dividend_amount,
                        split_coefficient = source.split_coefficient,
                        loaded_at = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (
                        symbol,
                        price_date,
                        open,
                        high,
                        low,
                        close,
                        adjusted_close,
                        volume,
                        dividend_amount,
                        split_coefficient
                    ) VALUES (
                        source.symbol,
                        source.price_date,
                        source.open,
                        source.high,
                        source.low,
                        source.close,
                        source.adjusted_close,
                        source.volume,
                        source.dividend_amount,
                        source.split_coefficient
                    )
                    """,
                    rows,
                )

        print(f"Upserted {len(rows)} Alpha Vantage daily price rows for {symbol}")
        return {"symbol": symbol, "rows_loaded": len(rows)}

    @task
    def calculate_sma_signals(tickers: list[str], load_results: list[dict]) -> list[dict]:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        loaded_symbols = {result["symbol"] for result in load_results if result["rows_loaded"] >= 200}
        signals = []
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for symbol in tickers:
                    if symbol not in loaded_symbols:
                        print(f"Skipping {symbol}; price load did not complete with enough history")
                        continue

                    cursor.execute(
                        """
                        WITH sma_values AS (
                            SELECT
                                symbol,
                                price_date,
                                adjusted_close AS close,
                                AVG(adjusted_close) OVER (
                                    PARTITION BY symbol
                                    ORDER BY price_date
                                    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                                ) AS sma_50,
                                AVG(adjusted_close) OVER (
                                    PARTITION BY symbol
                                    ORDER BY price_date
                                    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                                ) AS sma_200,
                                COUNT(*) OVER (
                                    PARTITION BY symbol
                                    ORDER BY price_date
                                    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                                ) AS sma_200_window
                            FROM stock_prices
                            WHERE symbol = %s
                        )
                        SELECT symbol, price_date, close, sma_50, sma_200
                        FROM sma_values
                        WHERE sma_200_window = 200
                        ORDER BY price_date DESC
                        LIMIT 2
                        """,
                        (symbol,),
                    )
                    rows = cursor.fetchall()

                    if len(rows) < 2:
                        print(f"Not enough SMA history for {symbol}")
                        continue

                    current = rows[0]
                    previous = rows[1]
                    current_spread = current[3] - current[4]
                    previous_spread = previous[3] - previous[4]

                    signal = "HOLD"
                    if previous_spread <= 0 < current_spread:
                        signal = "BUY"
                    elif previous_spread >= 0 > current_spread:
                        signal = "SELL"

                    signals.append(
                        {
                            "symbol": symbol,
                            "signal_date": str(current[1]),
                            "signal": signal,
                            "close": round(current[2], 2),
                            "sma_50": round(current[3], 2),
                            "sma_200": round(current[4], 2),
                        }
                    )

        print(f"Calculated SMA signals for {len(signals)} tickers")
        return signals

    @task
    def send_alerts(signals: list[dict]) -> None:
        import requests
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        webhook_url = Variable.get("stock_alert_webhook_url", default=None)
        actionable_signals = [signal for signal in signals if signal["signal"] in {"BUY", "SELL"}]

        if not actionable_signals:
            print("No new BUY or SELL crossover signals detected")
            return

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for signal in actionable_signals:
                    cursor.execute(
                        """
                        SELECT COUNT(*)
                        FROM stock_signals
                        WHERE symbol = %s AND signal_date = TO_DATE(%s) AND signal = %s
                        """,
                        (signal["symbol"], signal["signal_date"], signal["signal"]),
                    )
                    already_sent = cursor.fetchone()[0]

                    if already_sent:
                        print(
                            f"Skipping duplicate {signal['signal']} signal for "
                            f"{signal['symbol']} on {signal['signal_date']}"
                        )
                        continue

                    message = (
                        f"{signal['signal']} signal for {signal['symbol']} on {signal['signal_date']}: "
                        f"close={signal['close']}, SMA50={signal['sma_50']}, "
                        f"SMA200={signal['sma_200']}"
                    )

                    if webhook_url:
                        response = requests.post(
                            webhook_url,
                            data=json.dumps({"text": message}),
                            headers={"Content-Type": "application/json"},
                            timeout=30,
                        )
                        response.raise_for_status()

                    print(message)
                    cursor.execute(
                        """
                        INSERT INTO stock_signals (symbol, signal_date, signal, close, sma_50, sma_200)
                        VALUES (%s, TO_DATE(%s), %s, %s, %s, %s)
                        """,
                        (
                            signal["symbol"],
                            signal["signal_date"],
                            signal["signal"],
                            signal["close"],
                            signal["sma_50"],
                            signal["sma_200"],
                        ),
                    )

    tickers = get_ticker_config()
    initialize_snowflake_tables() >> tickers
    load_results = fetch_and_store_price_history.expand(symbol=tickers)
    signals = calculate_sma_signals(tickers, load_results)
    send_alerts(signals)


stock_sma_signals()
