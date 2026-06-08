# Trading Onboarding Project

An Astro/Airflow onboarding project that tracks selected stock prices, calculates 50-day and 200-day simple moving average signals, and emits alerts when a BUY or SELL crossover appears.

The project is designed for deployment to Astro. It uses Snowflake as durable storage and Alpha Vantage as the market data API.

## What The DAG Does

`stock_sma_signals` runs on weekdays at:

- `9:30 AM America/New_York`
- `12:30 PM America/New_York`
- `3:30 PM America/New_York`

The workflow:

1. Reads the configured ticker list.
2. Creates Snowflake tables if needed.
3. Fetches daily adjusted prices from Alpha Vantage for each ticker.
4. Upserts price history into Snowflake.
5. Calculates 50-day and 200-day simple moving averages.
6. Detects BUY and SELL crossovers.
7. Sends an optional webhook alert and records emitted signals in Snowflake to avoid duplicates.

Default tickers are `AAPL`, `MSFT`, and `NVDA`.

## Architecture

```text
stock_sma_signals
    ├── get_ticker_config
    ├── initialize_snowflake_tables
    ├── fetch_and_store_price_history mapped per ticker
    ├── calculate_sma_signals
    └── send_alerts
```

Snowflake tables created by the DAG:

- `stock_prices`
- `stock_signals`

## Required Airflow Connections

### `stock_signal_snowflake`

Type: Snowflake

Configure this connection with your Snowflake account details:

- Login: Snowflake username
- Password: Snowflake password or passphrase, depending on auth method
- Schema: target schema
- Extra JSON: account, database, warehouse, role, and any other required Snowflake connection fields

Example Extra JSON shape:

```json
{
  "account": "your_account",
  "database": "your_database",
  "warehouse": "your_warehouse",
  "role": "your_role"
}
```

### `alpha_vantage_default`

Type: HTTP

Configure this connection with:

- Host: `https://www.alphavantage.co`
- Password: your Alpha Vantage API key

Alternatively, store the API key in Extra JSON:

```json
{
  "api_key": "your_api_key"
}
```

## Optional Airflow Variables

### `tracked_stock_tickers`

JSON array of ticker symbols to track.

Example:

```json
["AAPL", "MSFT", "NVDA"]
```

If this variable is not set, the DAG uses the default tickers in source code.

### `stock_alert_webhook_url`

Webhook URL for alert delivery. If this variable is not set, alerts are written to task logs only.

## Local Development

Start Airflow locally:

```bash
astro dev start
```

Parse DAGs without starting Airflow:

```bash
astro dev parse
```

## Deploying To Astro

Deploy this project to an Astro Deployment:

```bash
astro deploy <deployment-id>
```

Before triggering the DAG on Astro, configure the required Airflow connections and optional variables in the target deployment.

## Notes

- This project is a scheduled analytics workflow, not a real-time trading system.
- The schedule does not account for US market holidays.
- Alpha Vantage free-tier limits may affect frequent runs or larger ticker lists.
- The DAG records emitted signals in Snowflake so reruns do not resend the same BUY or SELL signal.
