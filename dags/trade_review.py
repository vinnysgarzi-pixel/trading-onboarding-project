"""
# Trade Review (Human-in-the-Loop)

On-demand DAG: an analyst triggers this when the `trade_advisor` LLM branch has
flagged high-conviction signals for sign-off (rows marked `PENDING_REVIEW` in
`stock_recommendations`). It is intentionally **not** scheduled — the automated
pipeline keeps running and delivering; this is the manual gate for acting on
the strong calls.

Flow:
1. `fetch_pending_review` — pull the latest batch's PENDING_REVIEW rows and
   format them for the approval prompt. Skips the run if there are none.
2. `analyst_decision` — a Human-in-the-Loop `HITLBranchOperator` (Airflow 3.1+).
   The task defers and waits in the Airflow UI's "Required Actions" tab for an
   analyst to choose Approve or Reject.
3. `mark_approved` / `mark_rejected` — record the decision back to Snowflake.
   (Approve is where a real deployment would place paper trades via Alpaca.)

Required Airflow configuration:
- Snowflake connection: `stock_signal_snowflake`

This is a demo signal pipeline, not investment advice.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.hitl import HITLBranchOperator
from airflow.sdk import dag, task

SNOWFLAKE_CONN_ID = "stock_signal_snowflake"


def _update_latest_pending(new_status: str) -> None:
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    hook.run(
        f"""
        UPDATE stock_recommendations
        SET review_status = '{new_status}'
        WHERE review_status = 'PENDING_REVIEW'
          AND run_id = (
            SELECT run_id FROM stock_recommendations
            WHERE review_status = 'PENDING_REVIEW'
            ORDER BY created_at DESC LIMIT 1
          )
        """
    )


@dag(
    dag_id="trade_review",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/New_York"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Astro", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["trade-advisor", "hitl", "onboarding"],
    doc_md=__doc__,
)
def trade_review():
    @task
    def fetch_pending_review() -> str:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        rows = hook.get_records(
            """
            SELECT symbol, signal, score, close, rationale
            FROM stock_recommendations
            WHERE review_status = 'PENDING_REVIEW'
              AND run_id = (
                SELECT run_id FROM stock_recommendations
                WHERE review_status = 'PENDING_REVIEW'
                ORDER BY created_at DESC LIMIT 1
              )
            ORDER BY score DESC
            """
        )
        if not rows:
            raise AirflowSkipException("No PENDING_REVIEW signals; nothing to approve")

        lines = ["The following high-conviction signals need analyst sign-off:\n"]
        for symbol, signal, score, close, rationale in rows:
            lines.append(f"• {signal} {symbol} @ ${close:.2f} — {score:.0f}/100\n  {rationale}")
        summary = "\n".join(lines)
        print(summary)
        return summary

    @task
    def mark_approved() -> None:
        _update_latest_pending("APPROVED")
        # A live deployment would place paper trades via the Alpaca API here.
        print("Signals APPROVED — cleared for execution")

    @task
    def mark_rejected() -> None:
        _update_latest_pending("REJECTED")
        print("Signals REJECTED — no action taken")

    pending = fetch_pending_review()

    decision = HITLBranchOperator(
        task_id="analyst_decision",
        subject="Trade Advisor: high-conviction signals need your approval",
        body="{{ ti.xcom_pull(task_ids='fetch_pending_review') }}",
        options=["Approve", "Reject"],
        options_mapping={"Approve": "mark_approved", "Reject": "mark_rejected"},
    )

    pending >> decision >> [mark_approved(), mark_rejected()]


trade_review()
