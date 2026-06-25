# Trading Onboarding Project — Trade Advisor

An Astro/Airflow demo pipeline that ingests market data and news from Alpaca,
computes technical indicators with dbt (via Astronomer Cosmos), and uses the
**Apache Airflow Common AI provider** (`@task.llm`, `@task.llm_branch`) to score
news sentiment, write analyst rationales, and route high-conviction calls
through a **human-in-the-loop** approval gate — delivering a ranked
BUY/HOLD/SELL leaderboard to the task logs (and an optional Slack-compatible
webhook). A separate LLM-powered `failure_analyst` DAG triages pipeline
failures, showcasing AI-driven observability on Astro.

The LLM operators run against **Astronomer's LLM gateway** through a
`pydanticai` connection — no model SDK or API key in the DAG code.

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

Connections and variables are configured on the deployment (see below).

## Architecture

Six DAGs wired together with **Airflow Assets** (data-aware scheduling), so
the lineage graph in the Astro UI shows the full pipeline:

```text
market_data_ingest  (cron: 9:30 / 12:30 / 3:30 ET weekdays)
  ├── fetch_and_store_prices   (dynamic task mapping per ticker → Alpaca bars)
  ├── fetch_and_store_news     (dynamic task mapping per ticker → Alpaca news)
  └── emits Assets: stock_prices, stock_news
          │
          ▼ (asset-triggered)
compute_indicators  (Cosmos DbtTaskGroup)
  ├── dbt: stg_stock_prices
  ├── dbt: int_sma_trend / int_rsi / int_macd / int_bollinger / int_volume / int_volatility
  ├── dbt: stock_indicators mart (+ dbt tests)
  └── emits Asset: stock_indicators
          │
          ▼ (asset-triggered: indicators AND news)
trade_advisor   (Common AI provider operators)
  ├── score_news_sentiment     @task.llm  → structured sentiment per ticker
  ├── compute_scorecards       tiered composite score → BUY/HOLD/SELL
  ├── generate_rationales      @task.llm  → analyst rationale per ticker
  ├── persist_recommendations  → emits Asset: stock_recommendations
  └── route_review             @task.llm_branch (risk officer) →
                                 flag_for_review | clear_for_delivery
          │                                    │
          ▼ (asset-triggered)                  ▼ (flags high-conviction rows PENDING_REVIEW)
deliver_advice                          trade_review   (on-demand, HITL)
  └── send_webhook (logs + optional       ├── fetch_pending_review
      Slack webhook)                       ├── analyst_decision  HITLBranchOperator (Approve/Reject)
     (+ code-native DeadlineAlert SLA)     └── mark_approved | mark_rejected

failure_analyst   (on-demand / Astro Dag-Failure alert via Dag Trigger)
  └── gather_incidents → diagnose @task.llm (severity + root cause) → log_diagnoses
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
tickers are badged in the leaderboard (🆕 NEW LISTING / ⏳ limited history). A
ranking is produced on every run, so the demo always has fresh output.

### Snowflake tables

| Table | Written by |
|---|---|
| `stock_prices` | `market_data_ingest` |
| `stock_news` | `market_data_ingest` |
| `stock_indicators` (+ staging/intermediate views) | dbt via `compute_indicators` |
| `stock_recommendations` | `trade_advisor` |

### LLM, review, and delivery

The `trade_advisor` LLM tasks require the `pydanticai_default` connection. The
`trade_review` (HITL) DAG is on-demand — an analyst triggers it to approve the
high-conviction signals the LLM branch flagged (`review_status = PENDING_REVIEW`),
recording the decision back to `stock_recommendations`. The leaderboard always
lands in task logs; set `stock_alert_webhook_url` to also push it to a
Slack-compatible webhook.

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

### `pydanticai_default` (Common AI provider)

Powers the `@task.llm` / `@task.llm_branch` operators (model
`anthropic:claude-haiku-4-5`).

- Conn type: `pydanticai`
- Host: LLM gateway base URL
  (`https://api.astronomer.io/v1alpha1/organizations/<org_id>/llm`)
- Password: Astro API token (the gateway accepts it as `x-api-key`)
- Extra JSON: `{"model": "anthropic:claude-haiku-4-5"}`

Model and endpoint live entirely on the connection — the DAGs never import a
model SDK. (The older `anthropic_default` connection is no longer used.)

## Optional Airflow Variables

| Variable | Purpose | Default |
|---|---|---|
| `tracked_stock_tickers` | JSON array, e.g. `["AAPL", "MSFT", "NVDA"]` | `["AAPL", "MSFT", "NVDA"]` |
| `stock_alert_webhook_url` | Slack-compatible webhook for the leaderboard | logs only |

Edit `tracked_stock_tickers` in the Astro UI (**Environment → Airflow Variables**)
to add/remove tickers — no redeploy needed; the next run picks it up. Keep the
list under ~15 (the LLM sentiment/rationale calls are single batched requests).

## Observability & SLAs

The pipeline ships two reliability features in code, plus three you wire up
once in the Astro console:

**In code (already deployed):**
- **`failure_analyst` DAG** — an LLM triages failures. Trigger it manually with
  a conf like `{"dagName": "...", "message": "..."}`, or wire it to real
  failures via the Astro alert below.
- **`DeadlineAlert` on `deliver_advice`** — Airflow 3's code-native SLA (replaces
  the removed `sla=`): if delivery doesn't finish within 2h of the run's logical
  date, an async triggerer callback fires.

**In the Astro console (one-time setup):**
1. **Failure → AI triage.** Astro UI → **Alerts → Create** → type **Dag Failure**,
   scope to `market_data_ingest`, `compute_indicators`, `trade_advisor`,
   `deliver_advice` (NOT `failure_analyst` — it would retrigger itself) →
   notification channel **Dag Trigger** → target this deployment + DAG
   `failure_analyst` (Astro passes `dagName` / `airflowDagRunId` / `message`
   into the run conf, which `failure_analyst` reads).
2. **Delivery SLA.** Astro UI → **Alerts → Create** → **Dag Timeliness** on
   `deliver_advice` with a verification time (e.g. advice delivered by 10:00 ET)
   — Astro's managed equivalent of an SLA, no DAG code.
3. **Astro Observe.** Observe → **Data Products → + Data Product** → select the
   `stock_recommendations` outputs → add a **Freshness/Timeliness SLA**. Lineage
   is auto-collected via OpenLineage (pre-installed on the runtime).

Deployment metrics (DAG/task runs, durations, worker CPU/mem) are in the
deployment's **Analytics** tab out of the box; export to Prometheus/Datadog via
**Environment → Metrics Export** if desired.

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
