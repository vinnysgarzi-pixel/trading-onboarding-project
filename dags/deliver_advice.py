"""
# Deliver Advice

Asset-triggered DAG: runs whenever the trade advisor writes a new batch to
`stock_recommendations`. Delivers the ranked leaderboard through two
independent channels, each skipping gracefully when unconfigured:

- Slack-compatible webhook (Variable `stock_alert_webhook_url`)
- HTML email (connection `smtp_default` + Variable
  `stock_alert_email_recipients`, comma-separated addresses)

The plain-text leaderboard is always written to task logs.

Required Airflow configuration:
- Snowflake connection: `stock_signal_snowflake`
- Optional connection: `smtp_default` (host/port/login/password; Extra JSON
  may set `from_email`)
- Optional Variable: `stock_alert_webhook_url`
- Optional Variable: `stock_alert_email_recipients`
"""

from __future__ import annotations

import json
from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.sdk import Asset, Variable, dag, task

SNOWFLAKE_CONN_ID = "stock_signal_snowflake"
SMTP_CONN_ID = "smtp_default"
RECOMMENDATIONS_ASSET = Asset(name="stock_recommendations")

SIGNAL_EMOJI = {"BUY": "🟢", "SELL": "🔴", "HOLD": "⚪"}
SIGNAL_COLOR = {"BUY": "#16a34a", "SELL": "#dc2626", "HOLD": "#6b7280"}
SCORE_COLOR = {"BUY": "#dcfce7", "SELL": "#fee2e2", "HOLD": "#f3f4f6"}


def _format_text_leaderboard(rows: list[dict], issues: list[dict]) -> str:
    lines = [f"📈 *Trade Advisor* — signals as of {rows[0]['price_date']}"]
    for rank, row in enumerate(rows, 1):
        emoji = SIGNAL_EMOJI.get(row["signal"], "")
        lines.append(
            f"{rank}. {emoji} *{row['symbol']}* — {row['signal']} "
            f"({row['score']:.0f}/100) at ${row['close']:.2f}\n   {row['rationale']}"
        )
    if issues:
        issue_lines = "\n".join(
            f"   • {issue['symbol']} ({issue['stage']}): {issue['error']}"
            for issue in issues
        )
        lines.append(f"⚠️ *Data issues this run:*\n{issue_lines}")
    lines.append("_Demo signal pipeline on Astro — not investment advice._")
    return "\n\n".join(lines)


def _format_issues_card(issues: list[dict]) -> str:
    if not issues:
        return ""
    items = "".join(
        f"""<div style="padding-top:4px;">
              <strong>{issue["symbol"]}</strong> ({issue["stage"]}): {issue["error"]}
            </div>"""
        for issue in issues
    )
    return f"""
        <tr>
          <td style="padding:14px 16px;background:#fef3c7;border:1px solid #f59e0b;
                     border-radius:10px;font-family:Helvetica,Arial,sans-serif;
                     font-size:13px;color:#92400e;line-height:1.5;">
            <strong>⚠️ Data issues this run</strong> — these tickers could not be
            refreshed and are missing or stale below:
            {items}
          </td>
        </tr>
        <tr><td style="height:10px;"></td></tr>
    """


def _format_html_email(rows: list[dict], issues: list[dict]) -> str:
    cards = []
    for rank, row in enumerate(rows, 1):
        signal = row["signal"]
        badge = (
            f'<span style="background:{SIGNAL_COLOR[signal]};color:#ffffff;'
            'border-radius:12px;padding:2px 12px;font-size:13px;font-weight:600;">'
            f"{signal}</span>"
        )
        cards.append(
            f"""
            <tr>
              <td style="padding:14px 16px;background:{SCORE_COLOR[signal]};
                         border-radius:10px;border:1px solid #e5e7eb;">
                <table width="100%" cellpadding="0" cellspacing="0" style="font-family:Helvetica,Arial,sans-serif;">
                  <tr>
                    <td style="font-size:17px;font-weight:700;color:#111827;">
                      {rank}. {row["symbol"]} &nbsp;{badge}
                    </td>
                    <td align="right" style="font-size:15px;color:#111827;">
                      <strong>{row["score"]:.0f}</strong><span style="color:#6b7280;">/100</span>
                      &nbsp;·&nbsp; ${row["close"]:.2f}
                    </td>
                  </tr>
                  <tr>
                    <td colspan="2" style="padding-top:6px;font-size:13px;color:#374151;line-height:1.5;">
                      {row["rationale"]}
                    </td>
                  </tr>
                  <tr>
                    <td colspan="2" style="padding-top:6px;font-size:12px;color:#6b7280;">
                      RSI {row["rsi_14"]:.0f} · MACD hist {row["macd_histogram"]:.2f} ·
                      Bollinger z {row["bollinger_z"]:.2f} · Volume {row["volume_ratio"]:.2f}× ·
                      News sentiment {row["sentiment_score"]:+.2f}
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr><td style="height:10px;"></td></tr>
            """
        )

    return f"""
    <html>
      <body style="margin:0;padding:24px;background:#f9fafb;">
        <table width="100%" cellpadding="0" cellspacing="0">
          <tr><td align="center">
            <table width="560" cellpadding="0" cellspacing="0"
                   style="font-family:Helvetica,Arial,sans-serif;">
              <tr>
                <td style="padding-bottom:4px;font-size:22px;font-weight:800;color:#111827;">
                  📈 Trade Advisor
                </td>
              </tr>
              <tr>
                <td style="padding-bottom:18px;font-size:13px;color:#6b7280;">
                  Signals as of {rows[0]["price_date"]} · powered by Apache Airflow on Astro
                </td>
              </tr>
              {"".join(cards)}
              {_format_issues_card(issues)}
              <tr>
                <td style="padding-top:12px;font-size:11px;color:#9ca3af;">
                  Composite score blends trend (25%), MACD (20%), RSI (15%),
                  Bollinger (15%), volume (5%), and AI-scored news sentiment (20%).
                  BUY ≥ 65 · SELL ≤ 40. Demo signal pipeline — not investment advice.
                </td>
              </tr>
            </table>
          </td></tr>
        </table>
      </body>
    </html>
    """


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
                macd_histogram, bollinger_z, volume_ratio,
                sentiment_score, sentiment_summary, rationale
            FROM stock_recommendations
            WHERE run_id = (SELECT run_id FROM latest_batch)
            ORDER BY score DESC
            """
        )

        columns = [
            "symbol", "signal", "score", "close", "price_date", "rsi_14",
            "macd_histogram", "bollinger_z", "volume_ratio",
            "sentiment_score", "sentiment_summary", "rationale",
        ]
        rows = []
        for record in records:
            row = dict(zip(columns, record))
            row["price_date"] = str(row["price_date"])
            for key in ("score", "close", "rsi_14", "macd_histogram",
                        "bollinger_z", "volume_ratio", "sentiment_score"):
                if row[key] is not None:
                    row[key] = float(row[key])
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

    @task
    def render_email(batch: dict) -> dict:
        from airflow.sdk import BaseHook

        rows, issues = batch["recommendations"], batch["issues"]
        if not rows:
            raise AirflowSkipException("No recommendations to email")

        recipients = Variable.get("stock_alert_email_recipients", default=None)
        if not recipients:
            raise AirflowSkipException(
                "Variable stock_alert_email_recipients not set; skipping email"
            )

        try:
            BaseHook.get_connection(SMTP_CONN_ID)
        except Exception:
            raise AirflowSkipException(
                f"Connection {SMTP_CONN_ID} not configured; skipping email"
            )

        top = rows[0]
        subject = (
            f"📈 Trade Advisor {top['price_date']}: "
            f"{top['symbol']} {top['signal']} ({top['score']:.0f}/100)"
        )
        if issues:
            subject += f" — ⚠️ {len(issues)} data issue(s)"
        return {
            "to": [address.strip() for address in recipients.split(",")],
            "subject": subject,
            "html": _format_html_email(rows, issues),
        }

    batch = fetch_latest_batch()
    send_webhook(batch)
    email_payload = render_email(batch)

    # The sender address comes from the smtp_default connection's
    # Extra JSON `from_email`; skips cascade from render_email.
    EmailOperator(
        task_id="send_email",
        conn_id=SMTP_CONN_ID,
        to=email_payload["to"],
        subject=email_payload["subject"],
        html_content=email_payload["html"],
    )


deliver_advice()
