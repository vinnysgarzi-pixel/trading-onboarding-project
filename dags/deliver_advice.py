"""
# Deliver Advice

Asset-triggered DAG: runs whenever the trade advisor writes a new batch to
`stock_recommendations`. Formats the latest batch as a ranked leaderboard
message and delivers it to the configured webhook (Slack-compatible payload).
The message is always written to task logs, so the DAG works without a
webhook configured.

Required Airflow configuration:
- Snowflake connection: `stock_signal_snowflake`
- Optional Variable: `stock_alert_webhook_url`
"""

from __future__ import annotations

import json
from datetime import timedelta

import pendulum
from airflow.sdk import Asset, Variable, dag, task

SNOWFLAKE_CONN_ID = "stock_signal_snowflake"
RECOMMENDATIONS_ASSET = Asset(name="stock_recommendations")

SIGNAL_EMOJI = {"BUY": "🟢", "SELL": "🔴", "HOLD": "⚪"}


@dag(
    dag_id="deliver_advice",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/New_York"),
    schedule=[RECOMMENDATIONS_ASSET],
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Astro", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["trade-advisor", "alerts", "onboarding"],
    doc_md=__doc__,
)
def deliver_advice():
    @task
    def send_leaderboard() -> None:
        import requests
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        rows = hook.get_records(
            """
            WITH latest_batch AS (
                SELECT run_id
                FROM stock_recommendations
                ORDER BY created_at DESC
                LIMIT 1
            )
            SELECT
                symbol, signal, score, close, price_date,
                sentiment_summary, rationale
            FROM stock_recommendations
            WHERE run_id = (SELECT run_id FROM latest_batch)
            ORDER BY score DESC
            """
        )

        if not rows:
            print("No recommendations found; nothing to deliver")
            return

        as_of = rows[0][4]
        lines = [f"📈 *Trade Advisor* — signals as of {as_of}"]
        for rank, (symbol, signal, score, close, _, _, rationale) in enumerate(rows, 1):
            emoji = SIGNAL_EMOJI.get(signal, "")
            lines.append(
                f"{rank}. {emoji} *{symbol}* — {signal} ({score:.0f}/100) at ${close:.2f}\n"
                f"   {rationale}"
            )
        lines.append("_Demo signal pipeline on Astro — not investment advice._")
        message = "\n\n".join(lines)

        webhook_url = Variable.get("stock_alert_webhook_url", default=None)
        if webhook_url:
            response = requests.post(
                webhook_url,
                data=json.dumps({"text": message}),
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            response.raise_for_status()
            print("Leaderboard delivered to webhook")
        else:
            print("Variable stock_alert_webhook_url not set; logging only")

        print(message)

    send_leaderboard()


deliver_advice()
