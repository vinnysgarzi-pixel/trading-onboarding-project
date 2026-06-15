"""
# Failure Analyst (LLM-powered observability)

Triggered by an **Astro Dag-Failure alert** through the **Dag Trigger** channel:
when any pipeline DAG fails, Astro launches this DAG and passes the failure
context in the run `conf`. It uses the Common AI provider's `@task.llm` to turn
that context into a plain-English root-cause summary with suggested next steps,
then emails the on-call analyst (and always logs it).

This is the concrete "a DAG alert that calls an LLM operator" observability
example: the platform detects the failure, and an LLM operator triages it.

Can also be run manually — pass conf like:
`{"dagName": "market_data_ingest", "airflowDagRunId": "manual__...", "message": "..."}`

Required Airflow configuration:
- LLM connection: `pydanticai_default`
- Optional (for email): connection `smtp_default` + Variable
  `stock_alert_email_recipients`

Note: do NOT point the Dag-Failure alert at this DAG itself, or a failure here
would retrigger it. Scope the alert to the four pipeline DAGs.

This is a demo signal pipeline, not investment advice.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from pydantic import BaseModel

from airflow.exceptions import AirflowSkipException
from airflow.providers.common.compat.sdk import dag, task
from airflow.providers.smtp.operators.smtp import EmailOperator

LLM_CONN_ID = "pydanticai_default"
SMTP_CONN_ID = "smtp_default"


class FailureDiagnosis(BaseModel):
    """Structured triage output for a failed DAG run."""

    severity: str  # one of: low, medium, high, critical
    probable_cause: str
    suggested_actions: list[str]
    summary: str  # 2-3 sentence on-call-friendly summary


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
    def extract_context(**context) -> dict:
        conf = context["dag_run"].conf or {}
        # Astro's Dag-Trigger channel sends dagName / airflowDagRunId / message;
        # support plain dag_id / run_id for manual triggers too.
        ctx = {
            "failed_dag": conf.get("dagName") or conf.get("dag_id") or "unknown",
            "failed_run_id": conf.get("airflowDagRunId") or conf.get("run_id") or "unknown",
            "alert_type": conf.get("alertType") or "manual",
            "message": conf.get("message") or conf.get("note") or "(no message provided)",
        }
        print(f"Triaging failure: {ctx}")
        return ctx

    @task.llm(
        llm_conn_id=LLM_CONN_ID,
        system_prompt=(
            "You are a senior data-platform on-call engineer triaging a failed "
            "Apache Airflow DAG run on Astronomer Astro. Given the failure "
            "context, produce: a severity (low/medium/high/critical), the most "
            "probable root cause, 2-4 concrete suggested actions, and a 2-3 "
            "sentence summary an on-call analyst can act on immediately. This "
            "pipeline ingests market data from Alpaca, transforms it with dbt, "
            "scores it with an LLM, and emails recommendations; common failure "
            "modes are upstream API/credential issues, Snowflake connectivity, "
            "and the LLM gateway. Be specific and avoid generic boilerplate."
        ),
        output_type=FailureDiagnosis,
    )
    def diagnose(ctx: dict) -> str:
        import json

        return "Triage this failed Airflow DAG run:\n\n" + json.dumps(ctx, indent=2)

    @task
    def log_diagnosis(ctx: dict, diagnosis) -> None:
        d = diagnosis if isinstance(diagnosis, dict) else diagnosis.model_dump()
        print(f"[{d['severity'].upper()}] {ctx['failed_dag']} — {d['summary']}")
        print(f"Probable cause: {d['probable_cause']}")
        for action in d["suggested_actions"]:
            print(f"  • {action}")

    @task
    def render_email(ctx: dict, diagnosis) -> dict:
        from airflow.sdk import BaseHook, Variable

        recipients = Variable.get("stock_alert_email_recipients", default=None)
        if not recipients:
            raise AirflowSkipException("No recipients configured; skipping failure email")
        try:
            BaseHook.get_connection(SMTP_CONN_ID)
        except Exception:
            raise AirflowSkipException(f"{SMTP_CONN_ID} not configured; skipping failure email")

        d = diagnosis if isinstance(diagnosis, dict) else diagnosis.model_dump()
        severity = d["severity"].lower()
        color = SEVERITY_COLOR.get(severity, "#6b7280")
        actions = "".join(f"<li>{a}</li>" for a in d["suggested_actions"])
        html = f"""
        <html><body style="margin:0;padding:24px;background:#f9fafb;">
          <table width="560" style="font-family:Helvetica,Arial,sans-serif;">
            <tr><td style="font-size:20px;font-weight:800;color:#111827;">
              ⚠️ Pipeline Failure — {ctx['failed_dag']}</td></tr>
            <tr><td style="padding:6px 0 14px;">
              <span style="background:{color};color:#fff;border-radius:12px;
              padding:2px 12px;font-size:13px;font-weight:600;">{severity.upper()}</span></td></tr>
            <tr><td style="font-size:14px;color:#374151;line-height:1.5;padding-bottom:12px;">
              {d['summary']}</td></tr>
            <tr><td style="font-size:13px;color:#111827;font-weight:600;">Probable cause</td></tr>
            <tr><td style="font-size:13px;color:#374151;padding-bottom:12px;">{d['probable_cause']}</td></tr>
            <tr><td style="font-size:13px;color:#111827;font-weight:600;">Suggested actions</td></tr>
            <tr><td style="font-size:13px;color:#374151;"><ul>{actions}</ul></td></tr>
            <tr><td style="font-size:11px;color:#9ca3af;padding-top:12px;">
              Run: {ctx['failed_run_id']} · alert: {ctx['alert_type']} ·
              AI-triaged via the Common AI provider on Astro.</td></tr>
          </table></body></html>
        """
        return {
            "to": [a.strip() for a in recipients.split(",")],
            "subject": f"⚠️ [{severity.upper()}] {ctx['failed_dag']} failed — {d['summary'][:60]}",
            "html": html,
        }

    ctx = extract_context()
    diagnosis = diagnose(ctx)
    log_diagnosis(ctx, diagnosis)
    payload = render_email(ctx, diagnosis)

    EmailOperator(
        task_id="email_oncall",
        conn_id=SMTP_CONN_ID,
        to=payload["to"],
        subject=payload["subject"],
        html_content=payload["html"],
    )


failure_analyst()
