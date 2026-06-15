"""
# Failure Analyst (LLM-powered observability)

Uses the Common AI provider's `@task.llm` to perform root-cause analysis on
failed Airflow runs and email an on-call-ready triage. Two ways to run:

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
- Optional (for email): connection `smtp_default` + Variable
  `stock_alert_email_recipients`

This is a demo signal pipeline, not investment advice.
"""

from __future__ import annotations

import json
from datetime import timedelta

import pendulum
from pydantic import BaseModel

from airflow.exceptions import AirflowSkipException
from airflow.providers.common.compat.sdk import dag, task
from airflow.providers.smtp.operators.smtp import EmailOperator

LLM_CONN_ID = "pydanticai_default"
SMTP_CONN_ID = "smtp_default"

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
            "model_name 'anthropic:claude-opus-4-8', body "
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
        "failed_task": "email_oncall",
        "error_log": (
            "smtplib.SMTPSenderRefused: (550, b'The from address does not match "
            "a verified Sender Identity', 'vinny.sgarzi@astronomer.io'). "
            "SendGrid rejected the message; 0 recipients delivered."
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


SEVERITY_COLOR = {
    "critical": "#dc2626", "high": "#ea580c",
    "medium": "#d97706", "low": "#16a34a",
}


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
            "gateway, and emails recommendations. Be specific and technical; "
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

    @task
    def render_email(diagnosis) -> dict:
        from airflow.sdk import BaseHook, Variable

        recipients = Variable.get("stock_alert_email_recipients", default=None)
        if not recipients:
            raise AirflowSkipException("No recipients configured; skipping failure email")
        try:
            BaseHook.get_connection(SMTP_CONN_ID)
        except Exception:
            raise AirflowSkipException(f"{SMTP_CONN_ID} not configured; skipping failure email")

        results = diagnosis["results"] if isinstance(diagnosis, dict) else [
            r.model_dump() for r in diagnosis.results
        ]
        cards = []
        for d in results:
            sev = d["severity"].lower()
            color = SEVERITY_COLOR.get(sev, "#6b7280")
            actions = "".join(f"<li>{a}</li>" for a in d["suggested_actions"])
            cards.append(f"""
              <tr><td style="padding:14px 16px;background:#fff;border:1px solid #e5e7eb;
                  border-radius:10px;font-family:Helvetica,Arial,sans-serif;">
                <div style="font-size:15px;font-weight:700;color:#111827;">
                  {d['failed_dag']} · {d['failed_task']}
                  &nbsp;<span style="background:{color};color:#fff;border-radius:12px;
                  padding:2px 10px;font-size:12px;">{sev.upper()}</span></div>
                <div style="font-size:13px;color:#374151;padding-top:6px;">{d['summary']}</div>
                <div style="font-size:12px;color:#111827;font-weight:600;padding-top:8px;">Probable cause</div>
                <div style="font-size:12px;color:#374151;">{d['probable_cause']}</div>
                <div style="font-size:12px;color:#111827;font-weight:600;padding-top:8px;">Suggested actions</div>
                <ul style="font-size:12px;color:#374151;margin:4px 0;">{actions}</ul>
              </td></tr><tr><td style="height:10px;"></td></tr>
            """)
        worst = max(results, key=lambda d: ["low", "medium", "high", "critical"].index(d["severity"].lower())
                    if d["severity"].lower() in ["low", "medium", "high", "critical"] else 0)
        n = len(results)
        subject = (
            f"⚠️ Failure Analyst: {worst['failed_dag']} — {worst['summary'][:55]}"
            if n == 1 else f"⚠️ Failure Analyst: {n} incidents triaged ({worst['severity'].upper()} max)"
        )
        html = f"""
        <html><body style="margin:0;padding:24px;background:#f9fafb;">
          <table width="600"><tr><td style="font-family:Helvetica,Arial,sans-serif;
            font-size:20px;font-weight:800;color:#111827;padding-bottom:4px;">
            ⚠️ Failure Analyst — AI Root-Cause Analysis</td></tr>
            <tr><td style="font-family:Helvetica,Arial,sans-serif;font-size:13px;
            color:#6b7280;padding-bottom:16px;">{n} incident(s) triaged by the
            Common AI provider on Astro</td></tr>
            {''.join(cards)}
          </table></body></html>
        """
        return {
            "to": [a.strip() for a in recipients.split(",")],
            "subject": subject,
            "html": html,
        }

    incidents = gather_incidents()
    diagnosis = diagnose(incidents)
    log_diagnoses(diagnosis)
    payload = render_email(diagnosis)

    EmailOperator(
        task_id="email_oncall",
        conn_id=SMTP_CONN_ID,
        to=payload["to"],
        subject=payload["subject"],
        html_content=payload["html"],
    )


failure_analyst()
