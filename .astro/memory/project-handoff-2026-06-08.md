# Project Handoff 2026-06-08

### Source
user-interaction - End-of-day handoff after creating the project, repository, and Astro deployment.

## Memory
The trading onboarding project is an Astro-deployable Airflow project for scheduled stock SMA signal alerts. It uses Snowflake for durable state and Alpha Vantage for market data. Credential setup is intentionally paused and should resume by creating Airflow connections/variables on the new Astro deployment.

## Context
The user is a sales engineer at Astronomer building an onboarding demo. The goal is to deploy this to Astro, not only run locally. They asked to hold off on credentials because they were logging off for the day.

## Evidence
- GitHub repository created: https://github.com/vinnysgarzi-pixel/trading-onboarding-project
- New Astro deployment created: `trading-onboarding-project`
- Deployment ID: `cmq5ovwx88pi801nvr18n2ysc`
- Workspace: `Vinny Demo`
- Runtime: `3.2-5`, Airflow `3.2.2`
- Cloud/region: AWS `us-east-1`
- Deployment type: Standard development deployment
- DAG deploy enabled: true
- Airflow dashboard: `cosmicenergy.astronomer.run/d18n2ysc`
- Deployment dashboard: `cloud.astronomer.io/cmpx17yw51evb01n7rumg4h2p/deployments/cmq5ovwx88pi801nvr18n2ysc`
- DAG: `dags/stock_sma_signals.py`
- Schedule: `30 9,12,15 * * 1-5` with `America/New_York` start date/timezone
- Default tickers: `AAPL`, `MSFT`, `NVDA`
- Required remote Airflow connection still pending: `stock_signal_snowflake`
- Required remote Airflow connection still pending: `alpha_vantage_default`
- Optional variables still pending: `tracked_stock_tickers`, `stock_alert_webhook_url`
- User said to hold off on credentials for now and resume tomorrow.
