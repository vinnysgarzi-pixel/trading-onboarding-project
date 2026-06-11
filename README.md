# Trading Onboarding Project — Trade Advisor

An Astro/Airflow demo pipeline that ingests market data and news from Alpaca,
computes technical indicators with dbt (via Astronomer Cosmos), blends them
with Claude-scored news sentiment into a 0-100 conviction score per ticker,
and delivers a ranked BUY/HOLD/SELL leaderboard with AI-written analyst
rationales.

> This is a signal pipeline demo for showcasing Astro orchestration patterns —
> **not investment advice**. The strategy is intentionally swappable.

GitHub repository: https://github.com/vinnysgarzi-pixel/trading-onboarding-project

## Current Astro Deployment

- Deployment name: `trading-onboarding-project`
- Deployment ID: `cmq5ovwx88pi801nvr18n2ysc`
- Runtime: Astro Runtime `3.2-5` / Airflow `3.2.2`
- Cloud/region: AWS `us-east-1`
- Airflow dashboard: `https://cosmicenergy.astronomer.run/d18n2ysc`
- Deployment dashboard: `https://cloud.astronomer.io/cmpx17yw51evb01n7rumg4h2p/deployments/cmq5ovwx88pi801nvr18n2ysc`

Connection and variable setup is still pending (see below).

## Architecture

Four DAGs wired together with **Airflow Assets** (data-aware scheduling), so
the lineage graph in the Astro UI shows the full pipeline:

```text
market_data_ingest  (cron: 9:30 / 12:30 / 3:30 ET weekdays)
  ├── fetch_and_store_prices   (dynamic task mapping per ticker → Alpaca bars)
  ├── fetch_and_store_news     (dynamic task mapping per ticker → Alpaca news)
  └── emits Assets: snowflake://stock_prices, snowflake://stock_news
          │
          ▼ (asset-triggered)
compute_indicators  (Cosmos DbtTaskGroup)
  ├── dbt: stg_stock_prices
  ├── dbt: int_sma_trend / int_rsi / int_macd / int_bollinger / int_volume
  ├── dbt: stock_indicators mart (+ dbt tests)
  └── emits Asset: snowflake://stock_indicators
          │
          ▼ (asset-triggered: indicators AND news)
trade_advisor
  ├── score_news_sentiment     (Claude reads headlines → -1..1 per ticker)
  ├── compose_recommendations  (weighted composite score → BUY/HOLD/SELL,
  │                             Claude writes per-ticker analyst rationale)
  └── emits Asset: snowflake://stock_recommendations
          │
          ▼ (asset-triggered)
deliver_advice
  └── send_leaderboard         (ranked message → webhook + logs)
```

### Composite score — tiered by available history

Each ticker gets a 0-100 conviction score from seven signals: trend vs moving
averages, MACD, RSI-14, Bollinger z-score, volume vs 20-day average, 20-day
annualized realized volatility, and Claude-scored news sentiment. Because
newly listed stocks can't support long-window indicators, the advisor picks
an evaluation tier per ticker from its trading history:

| Tier | History | trend | MACD | RSI | Boll | volume | volatility | sentiment | BUY / SELL |
|---|---|---|---|---|---|---|---|---|---|
| Full | ≥ 200 days | 25% (SMA-50/200) | 15% | 15% | 10% | 5% | 10% | 20% | ≥65 / ≤40 |
| Developing | 60–199 days | 15% (SMA-20/50) | 15% | 15% | 10% | 5% | 10% | 30% | ≥65 / ≤40 |
| New listing | 20–59 days | — | — | 15% | 15% | 10% | 15% | 45% | ≥70 / ≤35 |

Tickers with under ~20 trading days are skipped with a note in the delivery
(they join automatically once they have enough history). Non-full-tier
tickers are badged in the email (🆕 NEW LISTING / ⏳ limited history). A
ranking is produced on every run, so the demo always has fresh output.

### Snowflake tables

| Table | Written by |
|---|---|
| `stock_prices` | `market_data_ingest` |
| `stock_news` | `market_data_ingest` |
| `stock_indicators` (+ staging/intermediate views) | dbt via `compute_indicators` |
| `stock_recommendations` | `trade_advisor` |

### Graceful degradation

If the `anthropic_default` connection is missing, the advisor still runs:
sentiment defaults to neutral and rationales fall back to a template. The
webhook variable is also optional — the leaderboard always lands in task logs.

## Required Airflow Connections

Configure these in the Astro **Environment Manager** (or `airflow_settings.yaml`
for local dev — never commit secrets).

### `stock_signal_snowflake` (Snowflake)

- Login / Password: Snowflake user and password
- Schema: target schema
- Extra JSON: `{"account": "...", "database": "...", "warehouse": "...", "role": "..."}`

Cosmos builds the dbt profile from this same connection at runtime — no
`profiles.yml` is needed.

### `alpaca_default` (HTTP)

- Host: `https://data.alpaca.markets` (default if unset)
- Login: Alpaca API key ID
- Password: Alpaca API secret key

Free keys at https://alpaca.markets — market data and news API both work on
unfunded paper accounts.

### `anthropic_default` (Generic, optional)

Powers sentiment scoring and rationales (model `claude-opus-4-8`). Two shapes:

- Direct Anthropic: leave Host empty, Password = Anthropic API key
- Anthropic-compatible gateway (e.g. Astronomer's LLM gateway): Host = gateway
  base URL (`https://api.astronomer.io/v1alpha1/organizations/<org_id>/llm`),
  Password = Astro API token (sent as bearer auth)

### `smtp_default` (SMTP, optional)

Enables the HTML email report from `deliver_advice` (sent with the SMTP
provider's `EmailOperator`). Host/port/login/password of any SMTP server
(e.g. `smtp.gmail.com:587` with an app password), and set the sender in
Extra JSON: `{"from_email": "you@example.com"}`.

## Optional Airflow Variables

| Variable | Purpose | Default |
|---|---|---|
| `tracked_stock_tickers` | JSON array, e.g. `["AAPL", "MSFT", "NVDA"]` | `["AAPL", "MSFT", "NVDA"]` |
| `stock_alert_webhook_url` | Slack-compatible webhook for the leaderboard | logs only |
| `stock_alert_email_recipients` | Comma-separated email recipients | email skipped |

## Local Development

```bash
astro dev start      # run Airflow locally
astro dev parse      # parse-check all DAGs (includes the Cosmos dbt render)
astro dev pytest     # run the DAG integrity tests in tests/
```

The dbt project lives at `dags/dbt/trading_indicators/` (inside `dags/` so
DAG-only deploys ship dbt model changes too). The `Dockerfile` installs
`dbt-snowflake` into an isolated `dbt_venv` that Cosmos invokes at runtime.

## Deploying to Astro

```bash
astro deploy cmq5ovwx88pi801nvr18n2ysc          # full image deploy (needed when Dockerfile/requirements change)
astro deploy cmq5ovwx88pi801nvr18n2ysc --dags   # fast DAG-only deploy (DAGs + dbt models)
```

Create the connections with the Astro CLI when credentials are available:

```bash
astro deployment connection create \
  --deployment-id cmq5ovwx88pi801nvr18n2ysc \
  --conn-id stock_signal_snowflake \
  --conn-type snowflake \
  --login <snowflake_user> \
  --password <snowflake_password> \
  --schema <snowflake_schema> \
  --extra '{"account":"<account>","database":"<database>","warehouse":"<warehouse>","role":"<role>"}'

astro deployment connection create \
  --deployment-id cmq5ovwx88pi801nvr18n2ysc \
  --conn-id alpaca_default \
  --conn-type http \
  --host https://data.alpaca.markets \
  --login <alpaca_api_key_id> \
  --password <alpaca_api_secret_key>

astro deployment connection create \
  --deployment-id cmq5ovwx88pi801nvr18n2ysc \
  --conn-id anthropic_default \
  --conn-type generic \
  --password <anthropic_api_key>

astro deployment airflow-variable create \
  --deployment-id cmq5ovwx88pi801nvr18n2ysc \
  --key tracked_stock_tickers \
  --value '["AAPL", "MSFT", "NVDA"]'

astro deployment airflow-variable create \
  --deployment-id cmq5ovwx88pi801nvr18n2ysc \
  --key stock_alert_webhook_url \
  --value '<webhook_url>'
```

After deploying, only `market_data_ingest` needs unpausing on a schedule — the
other three DAGs are asset-triggered and fire automatically as data lands.

## Notes

- The schedule does not account for US market holidays.
- Alpaca's free tier uses the IEX feed (`feed=iex`); volumes are consolidated
  differently than SIP but are fine for indicator demos.
- Prices are fetched with `adjustment=all`, so stored closes are
  split/dividend adjusted and indicator math is corporate-action safe.
- The MACD model approximates EMAs with truncated exponentially-weighted
  window sums (no recursive SQL); warm-up rows never reach the mart because
  it keeps only rows with a full SMA-200 window.
