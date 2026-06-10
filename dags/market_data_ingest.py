"""
# Market Data Ingest

Fetches daily adjusted stock bars and recent news headlines from Alpaca,
stores both in Snowflake, and emits `stock_prices` and `stock_news` Assets
so downstream DAGs (indicator computation, trade advisor) run data-aware.

Runs weekdays at 9:30 AM, 12:30 PM, and 3:30 PM America/New_York.

Required Airflow configuration:
- Snowflake connection: `stock_signal_snowflake`
- Alpaca HTTP connection: `alpaca_default`
  - Host: `https://data.alpaca.markets` (default if unset)
  - Login: Alpaca API key ID
  - Password: Alpaca API secret key
- Optional Variable: `tracked_stock_tickers`, JSON array like `["AAPL", "MSFT"]`
"""

from __future__ import annotations

import json
from datetime import timedelta

import pendulum
from airflow.sdk import Asset, BaseHook, Variable, dag, task

SNOWFLAKE_CONN_ID = "stock_signal_snowflake"
ALPACA_CONN_ID = "alpaca_default"
DEFAULT_TRACKED_TICKERS = ["AAPL", "MSFT", "NVDA"]
PRICE_HISTORY_CALENDAR_DAYS = 500  # ~340 trading days; SMA-200 needs 200
NEWS_LOOKBACK_DAYS = 5
MIN_REQUIRED_BARS = 220

STOCK_PRICES_ASSET = Asset(name="stock_prices")
STOCK_NEWS_ASSET = Asset(name="stock_news")


def _alpaca_request_config() -> tuple[str, dict[str, str]]:
    conn = BaseHook.get_connection(ALPACA_CONN_ID)
    if not conn.login or not conn.password:
        raise ValueError(
            f"{ALPACA_CONN_ID} must provide the Alpaca API key ID in the login "
            "field and the API secret key in the password field"
        )

    base_url = conn.host or "https://data.alpaca.markets"
    if not base_url.startswith("http"):
        base_url = f"https://{base_url}"

    headers = {
        "APCA-API-KEY-ID": conn.login,
        "APCA-API-SECRET-KEY": conn.password,
    }
    return base_url.rstrip("/"), headers


@dag(
    dag_id="market_data_ingest",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/New_York"),
    schedule="30 9,12,15 * * 1-5",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Astro", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["trade-advisor", "ingest", "onboarding"],
    doc_md=__doc__,
)
def market_data_ingest():
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
                    volume NUMBER NOT NULL,
                    trade_count NUMBER,
                    vwap FLOAT,
                    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS stock_news (
                    news_id STRING NOT NULL,
                    symbol STRING NOT NULL,
                    published_at TIMESTAMP_TZ,
                    headline STRING,
                    summary STRING,
                    source STRING,
                    url STRING,
                    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS ingest_issues (
                    run_id STRING NOT NULL,
                    symbol STRING NOT NULL,
                    stage STRING NOT NULL,
                    error STRING,
                    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
                """,
            ]
        )

    @task
    def fetch_and_store_prices(symbol: str) -> dict:
        import requests
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        base_url, headers = _alpaca_request_config()
        start_date = (
            pendulum.now("UTC").subtract(days=PRICE_HISTORY_CALENDAR_DAYS).to_date_string()
        )

        def symbol_failure(error: str) -> dict:
            print(f"Graceful failure for {symbol}: {error}")
            return {"symbol": symbol, "rows_loaded": 0, "status": "failed", "error": error}

        # Daily bars with all corporate-action adjustments applied, so SMA and
        # other indicator math downstream is split/dividend safe.
        bars: list[dict] = []
        params: dict[str, str | int] = {
            "timeframe": "1Day",
            "start": start_date,
            "adjustment": "all",
            "feed": "iex",
            "limit": 10000,
        }
        while True:
            response = requests.get(
                f"{base_url}/v2/stocks/{symbol}/bars",
                headers=headers,
                params=params,
                timeout=30,
            )
            # Symbol-shaped problems fail gracefully so one bad ticker can't
            # block the run; auth/server/rate-limit errors still raise & retry.
            if response.status_code in (400, 404, 422):
                return symbol_failure(
                    f"Alpaca rejected the symbol "
                    f"(HTTP {response.status_code}): {response.text[:200]}"
                )
            response.raise_for_status()
            payload = response.json()
            bars.extend(payload.get("bars") or [])
            next_page_token = payload.get("next_page_token")
            if not next_page_token:
                break
            params["page_token"] = next_page_token

        if len(bars) < MIN_REQUIRED_BARS:
            return symbol_failure(
                f"only {len(bars)} daily bars returned; {MIN_REQUIRED_BARS} needed "
                "for SMA-200 (unknown, delisted, or thinly traded symbol?)"
            )

        rows = [
            (
                symbol,
                bar["t"][:10],
                float(bar["o"]),
                float(bar["h"]),
                float(bar["l"]),
                float(bar["c"]),
                int(bar["v"]),
                int(bar.get("n") or 0),
                float(bar.get("vw") or 0.0),
            )
            for bar in bars
        ]

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
                            %s AS volume,
                            %s AS trade_count,
                            %s AS vwap
                    ) source
                    ON target.symbol = source.symbol
                    AND target.price_date = source.price_date
                    WHEN MATCHED THEN UPDATE SET
                        open = source.open,
                        high = source.high,
                        low = source.low,
                        close = source.close,
                        volume = source.volume,
                        trade_count = source.trade_count,
                        vwap = source.vwap,
                        loaded_at = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (
                        symbol, price_date, open, high, low, close,
                        volume, trade_count, vwap
                    ) VALUES (
                        source.symbol, source.price_date, source.open, source.high,
                        source.low, source.close, source.volume, source.trade_count,
                        source.vwap
                    )
                    """,
                    rows,
                )

        print(f"Upserted {len(rows)} Alpaca daily bars for {symbol}")
        return {"symbol": symbol, "rows_loaded": len(rows), "status": "ok", "error": None}

    @task
    def fetch_and_store_news(symbol: str) -> dict:
        import requests
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        base_url, headers = _alpaca_request_config()
        start = pendulum.now("UTC").subtract(days=NEWS_LOOKBACK_DAYS).to_iso8601_string()

        response = requests.get(
            f"{base_url}/v1beta1/news",
            headers=headers,
            params={"symbols": symbol, "start": start, "limit": 50},
            timeout=30,
        )
        if response.status_code in (400, 404, 422):
            error = (
                f"Alpaca news rejected the symbol "
                f"(HTTP {response.status_code}): {response.text[:200]}"
            )
            print(f"Graceful failure for {symbol}: {error}")
            return {"symbol": symbol, "articles_loaded": 0, "status": "failed", "error": error}
        response.raise_for_status()
        articles = response.json().get("news") or []

        rows = [
            (
                str(article["id"]),
                symbol,
                article.get("created_at"),
                (article.get("headline") or "")[:1000],
                (article.get("summary") or "")[:4000],
                article.get("source"),
                article.get("url"),
            )
            for article in articles
        ]

        if rows:
            hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
            with hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(
                        """
                        MERGE INTO stock_news target
                        USING (
                            SELECT
                                %s AS news_id,
                                %s AS symbol,
                                TO_TIMESTAMP_TZ(%s) AS published_at,
                                %s AS headline,
                                %s AS summary,
                                %s AS source,
                                %s AS url
                        ) source
                        ON target.news_id = source.news_id
                        AND target.symbol = source.symbol
                        WHEN NOT MATCHED THEN INSERT (
                            news_id, symbol, published_at, headline, summary, source, url
                        ) VALUES (
                            source.news_id, source.symbol, source.published_at,
                            source.headline, source.summary, source.source, source.url
                        )
                        """,
                        rows,
                    )

        print(f"Stored {len(rows)} news articles for {symbol}")
        return {"symbol": symbol, "articles_loaded": len(rows), "status": "ok", "error": None}

    @task(outlets=[STOCK_PRICES_ASSET, STOCK_NEWS_ASSET])
    def publish_market_data(
        price_results: list[dict], news_results: list[dict], **context
    ) -> None:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        failures = [
            (context["run_id"], result["symbol"], stage, result["error"])
            for results, stage in ((price_results, "prices"), (news_results, "news"))
            for result in results
            if result["status"] == "failed"
        ]
        if failures:
            hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
            with hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(
                        """
                        INSERT INTO ingest_issues (run_id, symbol, stage, error)
                        VALUES (%s, %s, %s, %s)
                        """,
                        failures,
                    )
            for _, symbol, stage, error in failures:
                print(f"ISSUE recorded for {symbol} ({stage}): {error}")

        healthy = [r for r in price_results if r["status"] == "ok"]
        if not healthy:
            raise ValueError(
                "No ticker loaded any price data; not emitting asset events. "
                "Check tracked_stock_tickers and the Alpaca connection."
            )

        total_bars = sum(result["rows_loaded"] for result in healthy)
        total_articles = sum(
            result["articles_loaded"] for result in news_results if result["status"] == "ok"
        )
        print(
            f"Market data refresh complete: {total_bars} price rows and "
            f"{total_articles} news articles across {len(healthy)}/{len(price_results)} "
            f"tickers ({len(failures)} issue(s) recorded)"
        )

    tickers = get_ticker_config()
    initialize_snowflake_tables() >> tickers
    price_results = fetch_and_store_prices.expand(symbol=tickers)
    news_results = fetch_and_store_news.expand(symbol=tickers)
    publish_market_data(price_results, news_results)


market_data_ingest()
