"""
# Failure Analyst (LLM-powered observability)

Uses the Common AI provider's `@task.llm` to perform root-cause analysis on
failed Airflow runs and write an on-call-ready triage to the task logs. Two
ways to run:

1. **Triggered by an Astro Dag-Failure alert** (Dag Trigger channel): Astro
   passes the real failure context in the run `conf` and this DAG triages it.
2. **Standalone manual run (no conf)**: it randomly encounters a couple of
   built-in, realistic failure scenarios for *this* pipeline — so every bare run
   delivers a useful AI debug and a *different* mix each time, showcasing RCA
   across failure classes (external-API rate-limit, credential/auth,
   data-quality, infra, delivery).

This is the concrete "a DAG alert that calls an LLM operator" observability
example — and on its own, a self-contained demo of AI-driven RCA.

Required Airflow configuration:
- LLM connection: `pydanticai_default`

The diagnosis is written to the task logs of the `log_diagnoses` task.

This is a demo signal pipeline, not investment advice.
"""

from __future__ import annotations

import json
from datetime import timedelta

import pendulum
from pydantic import BaseModel

from airflow.providers.common.compat.sdk import dag, task

LLM_CONN_ID = "pydanticai_default"

# Realistic failure modes for this pipeline. When the DAG is run standalone
# (no alert conf), it randomly encounters a couple of these — so each demo run
# surfaces a different incident, the way any given day might. The pool spans
# distinct RCA classes (transient external-API, credential/config, data-quality,
# infra, and delivery) to show the breadth of AI triage.
DEMO_SCENARIOS = [
    {
        "failed_dag": "market_data_ingest",
        "failed_task": "fetch_and_store_prices",
        "error_log": (
            "requests.exceptions.HTTPError: 429 Too Many Requests for url "
            "https://data.alpaca.markets/v2/stocks/NVDA/bars?timeframe=1Day "
            "— body {\"message\":\"too many requests\"}. Task exhausted its 2 "
            "retries; 7 of 12 mapped ticker tasks failed in the same minute."
        ),
    },
    {
        "failed_dag": "trade_advisor",
        "failed_task": "score_news_sentiment",
        "error_log": (
            "pydantic_ai.exceptions.ModelHTTPError: status_code 401, "
            "model_name 'anthropic:claude-haiku-4-5', body "
            "{'type':'error','error':{'type':'authentication_error',"
            "'message':'invalid x-api-key'}}. First failure after the run had "
            "been green for two weeks."
        ),
    },
    {
        "failed_dag": "compute_indicators",
        "failed_task": "dbt_indicators.test.not_null_stock_indicators_close",
        "error_log": (
            "Failure in test not_null_stock_indicators_close "
            "(models/marts/schema.yml): Got 14 results, configured to fail if "
            "!= 0. The 14 rows are all for ticker ELMT, which IPO'd 9 trading "
            "days ago; the SMA/return windows are still NULL."
        ),
    },
    {
        "failed_dag": "compute_indicators",
        "failed_task": "dbt_indicators.run.stock_indicators",
        "error_log": (
            "snowflake.connector.errors.ProgrammingError: 000606 (57P03): No "
            "active warehouse selected in the current session, and warehouse "
            "TINY_ROBOTS is SUSPENDED with AUTO_RESUME=FALSE. dbt model "
            "stock_indicators could not execute."
        ),
    },
    {
        "failed_dag": "deliver_advice",
        "failed_task": "send_webhook",
        "error_log": (
            "requests.exceptions.HTTPError: 404 Client Error: Not Found for url "
            "https://hooks.slack.com/services/T0XXX/B0XXX/xxxx. The configured "
            "stock_alert_webhook_url no longer resolves; the leaderboard was "
            "logged but not delivered to the webhook channel."
        ),
    },
]


class IncidentDiagnosis(BaseModel):
    """Root-cause analysis for a single failed run."""

    failed_dag: str
    failed_task: str
    severity: str  # low | medium | high | critical
    probable_cause: str
    suggested_actions: list[str]
    summary: str  # 2-3 sentence on-call-friendly summary


class DiagnosisBatch(BaseModel):
    results: list[IncidentDiagnosis]


@dag(
    dag_id="failure_analyst",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/New_York"),
    schedule=None,
    catchup=False,
    max_active_runs=3,
    default_args={"owner": "Astro", "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["trade-advisor", "observability", "llm", "onboarding"],
    doc_md=__doc__,
)
def failure_analyst():
    @task
    def gather_incidents(**context) -> list[dict]:
        conf = context["dag_run"].conf or {}
        if conf.get("dagName") or conf.get("dag_id") or conf.get("message"):
            incident = {
                "failed_dag": conf.get("dagName") or conf.get("dag_id") or "unknown",
                "failed_task": conf.get("taskId") or conf.get("task_id") or "(unspecified)",
                "error_log": conf.get("message") or conf.get("note") or "(no message provided)",
                "source": conf.get("alertType") or "alert",
            }
            print(f"Triaging real incident from conf: {incident['failed_dag']}")
            return [incident]

        # Standalone run: randomly encounter a couple of the possible incidents,
        # so each demo run surfaces a different mix.
        import random

        chosen = random.sample(DEMO_SCENARIOS, k=random.randint(2, 3))
        print(f"No conf provided — triaging {len(chosen)} randomly-encountered demo incidents")
        return [dict(scenario, source="demo-scenario") for scenario in chosen]

    @task.llm(
        llm_conn_id=LLM_CONN_ID,
        system_prompt=(
            "You are a senior data-platform on-call engineer doing root-cause "
            "analysis on failed Apache Airflow runs (Airflow 3 on Astronomer "
            "Astro). For EACH incident you are given, return: the failed_dag and "
            "failed_task (echo them back), a severity (low/medium/high/critical), "
            "the most probable root cause, 2-4 concrete suggested actions, and a "
            "2-3 sentence summary an on-call analyst can act on immediately. "
            "Context: this pipeline ingests market data + news from Alpaca, "
            "transforms it with dbt, scores it with an LLM via the Astro LLM "
            "gateway, and delivers recommendations. Be specific and technical; "
            "distinguish transient issues (retry/backoff) from config/credential "
            "issues (human fix) from data-quality issues (model/logic fix)."
        ),
        output_type=DiagnosisBatch,
    )
    def diagnose(incidents: list[dict]) -> str:
        return (
            "Perform root-cause analysis on each of these failed runs:\n\n"
            + json.dumps(incidents, indent=2)
        )

    @task
    def log_diagnoses(diagnosis) -> None:
        results = diagnosis["results"] if isinstance(diagnosis, dict) else [
            r.model_dump() for r in diagnosis.results
        ]
        for d in results:
            print(f"[{d['severity'].upper()}] {d['failed_dag']}.{d['failed_task']} — {d['summary']}")
            print(f"    cause: {d['probable_cause']}")
            for action in d["suggested_actions"]:
                print(f"    • {action}")

    incidents = gather_incidents()
    diagnosis = diagnose(incidents)
    log_diagnoses(diagnosis)


failure_analyst()
