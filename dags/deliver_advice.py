"""
# Deliver Advice

Asset-triggered DAG: runs whenever the trade advisor writes a new batch to
`stock_recommendations`. Delivers the ranked leaderboard to the task logs, and
optionally to a Slack-compatible webhook when one is configured:

- Slack-compatible webhook (Variable `stock_alert_webhook_url`)

The plain-text leaderboard is always written to task logs.

Required Airflow configuration:
- Snowflake connection: `stock_signal_snowflake`
- Optional Variable: `stock_alert_webhook_url`
"""

from __future__ import annotations

import json
from datetime import timedelta

import pendulum
from airflow.sdk import (
    Asset,
    AsyncCallback,
    DeadlineAlert,
    DeadlineReference,
    Variable,
    dag,
    task,
)

SNOWFLAKE_CONN_ID = "stock_signal_snowflake"
RECOMMENDATIONS_ASSET = Asset(name="stock_recommendations")

SIGNAL_EMOJI = {"BUY": "🟢", "SELL": "🔴", "HOLD": "⚪"}


def _format_text_leaderboard(rows: list[dict], issues: list[dict]) -> str:
    lines = [f"📈 *Trade Advisor* — signals as of {rows[0]['price_date']}"]
    for rank, row in enumerate(rows, 1):
        emoji = SIGNAL_EMOJI.get(row["signal"], "")
        tier = row.get("tier") or "full"
        tier_note = ""
        if tier == "new_ipo":
            tier_note = f" 🆕 new listing ({row['history_days']:.0f} trading days)"
        elif tier == "developing":
            tier_note = f" ⏳ limited history ({row['history_days']:.0f} days)"
        lines.append(
            f"{rank}. {emoji} *{row['symbol']}* — {row['signal']} "
            f"({row['score']:.0f}/100) at ${row['close']:.2f}{tier_note}\n"
            f"   {row['rationale']}"
        )
    if issues:
        issue_lines = "\n".join(
            f"   • {issue['symbol']} ({issue['stage']}): {issue['error']}"
            for issue in issues
        )
        lines.append(f"⚠️ *Data issues this run:*\n{issue_lines}")
    lines.append("_Demo signal pipeline on Astro — not investment advice._")
    return "\n\n".join(lines)


async def _sla_deadline_missed(*args, **kwargs) -> None:
    # Runs on the triggerer (async) if the delivery deadline passes before this
    # DAG finishes — Airflow 3's code-native SLA (Deadline Alerts) replacement
    # for the removed `sla=` callback.
    context = kwargs.get("context", {})
    print(f"SLA DEADLINE MISSED — trade advice not delivered in time: {context}")


@dag(
    dag_id="deliver_advice",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/New_York"),
    schedule=[RECOMMENDATIONS_ASSET],
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Astro", "retries": 2, "retry_delay": timedelta(minutes=2)},
    # Code-native SLA (Airflow 3 Deadline Alerts): if delivery hasn't completed
    # within 2h of the run's logical date, fire the callback on the triggerer.
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(hours=2),
        callback=AsyncCallback(_sla_deadline_missed),
    ),
    tags=["trade-advisor", "alerts", "onboarding"],
    doc_md=__doc__,
)
def deliver_advice():
    @task
    def fetch_latest_batch() -> dict:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        records = hook.get_records(
            """
            WITH latest_batch AS (
                SELECT run_id
                FROM stock_recommendations
                ORDER BY created_at DESC
                LIMIT 1
            )
            SELECT
                symbol, signal, score, close, price_date, rsi_14,
                macd_histogram, bollinger_z, volume_ratio, volatility_20,
                tier, history_days,
                sentiment_score, sentiment_summary, rationale
            FROM stock_recommendations
            WHERE run_id = (SELECT run_id FROM latest_batch)
            ORDER BY score DESC
            """
        )

        columns = [
            "symbol", "signal", "score", "close", "price_date", "rsi_14",
            "macd_histogram", "bollinger_z", "volume_ratio", "volatility_20",
            "tier", "history_days",
            "sentiment_score", "sentiment_summary", "rationale",
        ]
        rows = []
        for record in records:
            row = dict(zip(columns, record))
            row["price_date"] = str(row["price_date"])
            for key in ("score", "close", "rsi_14", "macd_histogram",
                        "bollinger_z", "volume_ratio", "volatility_20",
                        "history_days", "sentiment_score"):
                if row[key] is not None:
                    row[key] = float(row[key])
            # Batches written before tiering existed lack these fields.
            row["tier"] = row["tier"] or "full"
            row["history_days"] = row["history_days"] or 0.0
            row["volatility_20"] = row["volatility_20"] if row["volatility_20"] is not None else 0.0
            rows.append(row)

        if not rows:
            print("No recommendations found; nothing to deliver")

        # Ingest failures from the current cascade window, newest per
        # symbol/stage, so bad tickers are surfaced rather than silently absent.
        issue_records = hook.get_records(
            """
            SELECT symbol, stage, error
            FROM ingest_issues
            WHERE created_at >= DATEADD(hour, -4, CURRENT_TIMESTAMP())
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY symbol, stage ORDER BY created_at DESC
            ) = 1
            ORDER BY symbol, stage
            """
        )
        issues = [
            {"symbol": symbol, "stage": stage, "error": error}
            for symbol, stage, error in issue_records
        ]
        if issues:
            print(f"Surfacing {len(issues)} ingest issue(s) in this delivery")

        return {"recommendations": rows, "issues": issues}

    @task
    def send_webhook(batch: dict) -> None:
        import requests

        rows, issues = batch["recommendations"], batch["issues"]
        if not rows:
            return
        message = _format_text_leaderboard(rows, issues)

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

    batch = fetch_latest_batch()
    send_webhook(batch)


deliver_advice()
