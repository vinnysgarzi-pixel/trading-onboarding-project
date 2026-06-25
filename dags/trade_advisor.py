"""
# Trade Advisor

Asset-triggered DAG: runs when both `stock_indicators` (from the dbt layer)
and `stock_news` (from ingest) have fresh data. For each tracked ticker it:

1. Reads the latest two indicator rows from Snowflake.
2. Scores recent news headlines for sentiment via the Common AI provider's
   `@task.llm` (structured output, -1.0 to 1.0).
3. Blends tiered signals into a 0-100 conviction score (see TIER_CONFIG).
4. Classifies BUY / HOLD / SELL per the ticker's tier thresholds.
5. Writes a one-paragraph analyst rationale per ticker, again via `@task.llm`.
6. Writes the batch to `stock_recommendations` and emits its Asset.

The LLM tasks call the Apache Airflow **Common AI provider** (pydantic-ai under
the hood), configured entirely through a connection — no model SDK is imported
here. The connection points at Astronomer's LLM gateway.

Required Airflow configuration:
- Snowflake connection: `stock_signal_snowflake`
- LLM connection: `pydanticai_default` (conn_type `pydanticai`; host = gateway
  base URL; password = Astro API token; extra `{"model": "anthropic:claude-haiku-4-5"}`)

This is a demo signal pipeline, not investment advice.
"""

from __future__ import annotations

import json
from datetime import timedelta

import pendulum
from pydantic import BaseModel

from airflow.providers.common.compat.sdk import dag, task
from airflow.sdk import Asset

SNOWFLAKE_CONN_ID = "stock_signal_snowflake"
LLM_CONN_ID = "pydanticai_default"
NEWS_LOOKBACK_DAYS = 5
MAX_HEADLINES_PER_SYMBOL = 8

STOCK_INDICATORS_ASSET = Asset(name="stock_indicators")
STOCK_NEWS_ASSET = Asset(name="stock_news")
RECOMMENDATIONS_ASSET = Asset(name="stock_recommendations")

RATIONALE_SYSTEM_PROMPT = (
    "You are a buy-side equity analyst writing brief daily notes. For each "
    "ticker scorecard, write a 2-3 sentence rationale explaining the signal in "
    "plain language, citing the most decisive indicators by name and value. "
    "Match the tone of a morning desk note. Do not hedge with generic "
    "disclaimers. When a ticker's tier is 'new_ipo' or 'developing', open by "
    "noting it is a recently listed stock evaluated on limited history, with "
    "news sentiment and volatility weighted more heavily."
)


# Structured-output models for the @task.llm calls. Defined at module scope so
# their qualified names survive XCom serialization and re-import downstream.
class SymbolSentiment(BaseModel):
    symbol: str
    sentiment_score: float  # -1.0 (very bearish) .. 1.0 (very bullish)
    summary: str


class SentimentBatch(BaseModel):
    results: list[SymbolSentiment]


class SymbolRationale(BaseModel):
    symbol: str
    rationale: str


class RationaleBatch(BaseModel):
    results: list[SymbolRationale]


def _as_dict(item) -> dict:
    """Normalize an LLM result item that may be a pydantic model or a dict."""
    return item if isinstance(item, dict) else item.model_dump()


def _results_of(batch) -> list:
    """Pull the `results` list off a batch that may be a model or a dict."""
    if hasattr(batch, "results"):
        return batch.results
    if isinstance(batch, dict):
        return batch.get("results", [])
    return []


def _sentiment_to_dict(batch) -> dict[str, dict]:
    sentiment: dict[str, dict] = {}
    for item in _results_of(batch):
        record = _as_dict(item)
        sentiment[record["symbol"]] = {
            "sentiment_score": max(-1.0, min(1.0, float(record["sentiment_score"]))),
            "summary": record["summary"],
        }
    return sentiment


def _rationales_to_dict(batch) -> dict[str, str]:
    return {
        _as_dict(item)["symbol"]: _as_dict(item)["rationale"]
        for item in _results_of(batch)
    }

# Evaluation tiers keyed by available trading history. Young listings can't
# support long-window indicators, so scoring shifts weight toward news
# sentiment, short-window signals, and volatility risk — and the newest
# tier demands more conviction before moving off HOLD.
TIER_CONFIG = {
    "full": {
        "min_history": 200,
        "label": "full history",
        "weights": {
            "trend": 0.25, "macd": 0.15, "rsi": 0.15, "bollinger": 0.10,
            "volume": 0.05, "volatility": 0.10, "sentiment": 0.20,
        },
        "buy": 65,
        "sell": 40,
    },
    "developing": {
        "min_history": 60,
        "label": "developing history — no SMA-200 yet",
        "weights": {
            "trend": 0.15, "macd": 0.15, "rsi": 0.15, "bollinger": 0.10,
            "volume": 0.05, "volatility": 0.10, "sentiment": 0.30,
        },
        "buy": 65,
        "sell": 40,
    },
    "new_ipo": {
        "min_history": 20,
        "label": "new listing",
        "weights": {
            "rsi": 0.15, "bollinger": 0.15, "volume": 0.10,
            "volatility": 0.15, "sentiment": 0.45,
        },
        "buy": 70,  # thin data: demand more conviction in either direction
        "sell": 35,
    },
}


def _tier_for(history_days: float) -> str:
    for tier_name in ("full", "developing", "new_ipo"):
        if history_days >= TIER_CONFIG[tier_name]["min_history"]:
            return tier_name
    return "new_ipo"


def _score_trend(latest: dict) -> tuple[float, str]:
    close, sma_50, sma_200 = latest["close"], latest["sma_50"], latest["sma_200"]
    if close > sma_50 > sma_200:
        return 90, "uptrend (close > SMA50 > SMA200)"
    if close > sma_50:
        return 70, "above SMA50"
    if close > sma_200:
        return 55, "above SMA200 but below SMA50"
    if sma_50 > sma_200:
        return 45, "below SMA50 in a long-term uptrend"
    return 20, "downtrend (close < SMA50 < SMA200)"


def _score_macd(latest: dict, previous: dict | None) -> float:
    histogram = latest["macd_histogram"]
    prev_histogram = previous["macd_histogram"] if previous else histogram
    if histogram > 0:
        return 90 if histogram >= prev_histogram else 70
    return 30 if histogram >= prev_histogram else 15


def _score_rsi(rsi: float) -> float:
    if rsi < 30:
        return 75  # oversold: mean-reversion buy setup
    if rsi < 45:
        return 40
    if rsi < 55:
        return 55
    if rsi < 70:
        return 85  # healthy bullish momentum
    if rsi < 80:
        return 40  # overbought
    return 20


def _score_bollinger(z: float) -> float:
    if z < -2:
        return 85  # stretched below the band: mean-reversion buy
    if z < -1:
        return 65
    if z <= 1:
        return 50
    if z <= 2:
        return 35
    return 15  # stretched above the band


def _score_trend_short(latest: dict) -> tuple[float, str]:
    """Trend read for tickers too young to have an SMA-200 anchor."""
    close, sma_20, sma_50 = latest["close"], latest["sma_20"], latest["sma_50"]
    if close > sma_20 > sma_50:
        return 85, "short-term uptrend (close > SMA20 > SMA50)"
    if close > sma_50:
        return 70, "above SMA50"
    if close > sma_20:
        return 50, "above SMA20 but below SMA50"
    return 25, "below short-term moving averages"


def _score_volume(ratio: float) -> float:
    if ratio >= 2:
        return 90
    if ratio >= 1.25:
        return 70
    if ratio >= 0.75:
        return 50
    return 30


def _score_volatility(annualized_pct: float) -> float:
    if annualized_pct < 20:
        return 70  # calm
    if annualized_pct < 35:
        return 60  # normal
    if annualized_pct < 60:
        return 40  # elevated
    return 20  # speculative


@dag(
    dag_id="trade_advisor",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/New_York"),
    schedule=(STOCK_INDICATORS_ASSET & STOCK_NEWS_ASSET),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Astro", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["trade-advisor", "llm", "onboarding"],
    doc_md=__doc__,
)
def trade_advisor():
    @task
    def initialize_snowflake_tables() -> None:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(
            """
            CREATE TABLE IF NOT EXISTS stock_recommendations (
                run_id STRING NOT NULL,
                symbol STRING NOT NULL,
                price_date DATE,
                signal STRING NOT NULL,
                score NUMBER NOT NULL,
                close FLOAT,
                trend_state STRING,
                rsi_14 FLOAT,
                macd_histogram FLOAT,
                bollinger_z FLOAT,
                volume_ratio FLOAT,
                volatility_20 FLOAT,
                tier STRING,
                history_days NUMBER,
                sentiment_score FLOAT,
                sentiment_summary STRING,
                rationale STRING,
                review_status STRING DEFAULT 'AUTO_CLEARED',
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
        )
        # Upgrade tables created before tiering/volatility/review existed.
        hook.run(
            [
                "ALTER TABLE stock_recommendations ADD COLUMN IF NOT EXISTS volatility_20 FLOAT",
                "ALTER TABLE stock_recommendations ADD COLUMN IF NOT EXISTS tier STRING",
                "ALTER TABLE stock_recommendations ADD COLUMN IF NOT EXISTS history_days NUMBER",
                "ALTER TABLE stock_recommendations ADD COLUMN IF NOT EXISTS review_status STRING",
            ]
        )

    @task
    def fetch_indicator_snapshot() -> dict[str, list[dict]]:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        rows = hook.get_records(
            """
            WITH ranked AS (
                SELECT
                    symbol, price_date, history_days, close, sma_20, sma_50,
                    sma_200, rsi_14, macd, macd_signal, macd_histogram,
                    bollinger_z, volume_ratio, volatility_20,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol ORDER BY price_date DESC
                    ) AS recency
                FROM stock_indicators
            )
            SELECT
                symbol, price_date, history_days, close, sma_20, sma_50,
                sma_200, rsi_14, macd, macd_signal, macd_histogram,
                bollinger_z, volume_ratio, volatility_20, recency
            FROM ranked
            WHERE recency <= 2
            ORDER BY symbol, recency
            """
        )

        columns = [
            "symbol", "price_date", "history_days", "close", "sma_20", "sma_50",
            "sma_200", "rsi_14", "macd", "macd_signal", "macd_histogram",
            "bollinger_z", "volume_ratio", "volatility_20", "recency",
        ]
        snapshot: dict[str, list[dict]] = {}
        for row in rows:
            record = dict(zip(columns, row))
            record["price_date"] = str(record["price_date"])
            for key, value in record.items():
                if key not in ("symbol", "price_date") and value is not None:
                    record[key] = float(value)
            snapshot.setdefault(record["symbol"], []).append(record)

        print(f"Loaded indicator snapshots for {sorted(snapshot)}")
        return snapshot

    @task
    def fetch_recent_headlines() -> dict[str, list[str]]:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        rows = hook.get_records(
            f"""
            WITH ranked AS (
                SELECT
                    symbol, headline,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol ORDER BY published_at DESC
                    ) AS recency
                FROM stock_news
                WHERE published_at >= DATEADD(day, -{NEWS_LOOKBACK_DAYS}, CURRENT_TIMESTAMP())
                AND headline IS NOT NULL AND headline <> ''
            )
            SELECT symbol, headline
            FROM ranked
            WHERE recency <= {MAX_HEADLINES_PER_SYMBOL}
            ORDER BY symbol, recency
            """
        )

        headlines: dict[str, list[str]] = {}
        for symbol, headline in rows:
            headlines.setdefault(symbol, []).append(headline)
        return headlines

    @task.llm(
        llm_conn_id=LLM_CONN_ID,
        system_prompt=(
            "You are a financial news sentiment analyst. You are given recent "
            "headlines per stock symbol. Return exactly one result per symbol "
            "with a sentiment_score between -1.0 (very bearish) and 1.0 (very "
            "bullish) and a one-sentence summary of the news tone. Score 0.0 "
            "when the headlines are mixed, immaterial, or empty."
        ),
        output_type=SentimentBatch,
    )
    def score_news_sentiment(headlines_by_symbol: dict[str, list[str]]) -> str:
        return (
            "Score the news sentiment for each symbol below. Include every "
            "symbol in your response, even those with an empty headline list "
            "(score those 0.0).\n\n" + json.dumps(headlines_by_symbol, indent=2)
        )

    @task
    def compute_scorecards(
        snapshot: dict[str, list[dict]],
        sentiment_batch,
    ) -> list[dict]:
        sentiment = _sentiment_to_dict(sentiment_batch)
        scorecards = []
        for symbol, rows in sorted(snapshot.items()):
            latest = rows[0]
            previous = rows[1] if len(rows) > 1 else None
            symbol_sentiment = sentiment.get(
                symbol,
                {"sentiment_score": 0.0, "summary": "No sentiment available."},
            )

            tier_name = _tier_for(latest["history_days"])
            tier = TIER_CONFIG[tier_name]
            weights = tier["weights"]

            available = {
                "rsi": _score_rsi(latest["rsi_14"]),
                "bollinger": _score_bollinger(latest["bollinger_z"]),
                "volume": _score_volume(latest["volume_ratio"]),
                "volatility": _score_volatility(latest["volatility_20"]),
                "sentiment": (symbol_sentiment["sentiment_score"] + 1) * 50,
            }
            if tier_name == "full":
                trend_score, trend_state = _score_trend(latest)
                available["trend"] = trend_score
            elif tier_name == "developing":
                trend_score, trend_state = _score_trend_short(latest)
                available["trend"] = trend_score
            else:
                trend_state = "not evaluated — insufficient history"
            if "macd" in weights:
                available["macd"] = _score_macd(latest, previous)

            subscores = {name: available[name] for name in weights}
            composite = round(
                sum(weight * subscores[name] for name, weight in weights.items())
            )
            if composite >= tier["buy"]:
                signal = "BUY"
            elif composite <= tier["sell"]:
                signal = "SELL"
            else:
                signal = "HOLD"

            scorecards.append(
                {
                    "symbol": symbol,
                    "price_date": latest["price_date"],
                    "signal": signal,
                    "score": composite,
                    "close": round(latest["close"], 2),
                    "tier": tier_name,
                    "tier_label": tier["label"],
                    "history_days": int(latest["history_days"]),
                    "trend_state": trend_state,
                    "rsi_14": latest["rsi_14"],
                    "macd_histogram": latest["macd_histogram"],
                    "bollinger_z": latest["bollinger_z"],
                    "volume_ratio": latest["volume_ratio"],
                    "volatility_20": latest["volatility_20"],
                    "sentiment_score": symbol_sentiment["sentiment_score"],
                    "sentiment_summary": symbol_sentiment["summary"],
                    "subscores": subscores,
                    "weights_used": weights,
                    "thresholds": {"buy": tier["buy"], "sell": tier["sell"]},
                }
            )

        print(f"Computed {len(scorecards)} scorecards")
        return scorecards

    @task.llm(
        llm_conn_id=LLM_CONN_ID,
        system_prompt=RATIONALE_SYSTEM_PROMPT,
        output_type=RationaleBatch,
    )
    def generate_rationales(scorecards: list[dict]) -> str:
        return (
            "Write a 2-3 sentence analyst rationale for each ticker scorecard "
            "below. The composite score is 0-100; each card lists its evaluation "
            "tier, the exact weights_used, its BUY/SELL thresholds, and per-signal "
            "subscores. Newer listings (tier 'developing' or 'new_ipo') lack "
            "long-window indicators, so open their note by flagging the limited "
            "history and the heavier weight on sentiment and volatility.\n\n"
            + json.dumps(scorecards, indent=2)
        )

    @task(outlets=[RECOMMENDATIONS_ASSET])
    def persist_recommendations(
        scorecards: list[dict],
        rationale_batch,
        **context,
    ) -> list[dict]:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        run_id = context["run_id"]
        if not scorecards:
            print("No tickers had a complete indicator snapshot; nothing to recommend")
            return []

        rationales = _rationales_to_dict(rationale_batch)
        for card in scorecards:
            card["rationale"] = rationales.get(card["symbol"]) or _template_rationale(card)
            for transient in ("subscores", "weights_used", "thresholds"):
                card.pop(transient, None)

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO stock_recommendations (
                        run_id, symbol, price_date, signal, score, close,
                        trend_state, rsi_14, macd_histogram, bollinger_z,
                        volume_ratio, volatility_20, tier, history_days,
                        sentiment_score, sentiment_summary, rationale
                    ) VALUES (
                        %s, %s, TO_DATE(%s), %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    [
                        (
                            run_id,
                            card["symbol"],
                            card["price_date"],
                            card["signal"],
                            card["score"],
                            card["close"],
                            card["trend_state"],
                            card["rsi_14"],
                            card["macd_histogram"],
                            card["bollinger_z"],
                            card["volume_ratio"],
                            card["volatility_20"],
                            card["tier"],
                            card["history_days"],
                            card["sentiment_score"],
                            card["sentiment_summary"],
                            card["rationale"],
                        )
                        for card in scorecards
                    ],
                )

        ranked = sorted(scorecards, key=lambda card: card["score"], reverse=True)
        print(
            "Recommendations written: "
            + ", ".join(f"{c['symbol']} {c['signal']} ({c['score']})" for c in ranked)
        )
        return ranked

    def _template_rationale(card: dict) -> str:
        prefix = ""
        if card["tier"] != "full":
            prefix = (
                f"Recently listed ({card['history_days']} trading days; "
                f"{card['tier_label']}) — scored on a reduced indicator set with "
                "sentiment and volatility weighted more heavily. "
            )
        return (
            f"{prefix}{card['signal']} at {card['score']}/100. "
            f"Trend: {card['trend_state']}; "
            f"RSI {card['rsi_14']:.0f}, MACD histogram {card['macd_histogram']:.2f}, "
            f"Bollinger z {card['bollinger_z']:.2f}, volume ratio "
            f"{card['volume_ratio']:.2f}, volatility {card['volatility_20']:.0f}% "
            f"annualized. News sentiment "
            f"{card['sentiment_score']:+.2f}: {card['sentiment_summary']}"
        )

    @task.llm_branch(
        llm_conn_id=LLM_CONN_ID,
        system_prompt=(
            "You are a risk officer reviewing a batch of daily trade "
            "recommendations before the desk acts on them. Decide whether the "
            "batch needs a human analyst's sign-off. Choose 'flag_for_review' "
            "if any recommendation is high-conviction (a strong BUY or SELL) or "
            "carries notable risk such as elevated volatility, a brand-new "
            "listing, or signals that conflict with the news sentiment. Choose "
            "'clear_for_delivery' only if everything is routine and "
            "low-conviction (mostly HOLDs with unremarkable risk)."
        ),
    )
    def route_review(ranked: list[dict]) -> str:
        return (
            "Assess whether this batch of recommendations needs analyst review "
            "before distribution:\n\n" + json.dumps(ranked, indent=2)
        )

    @task
    def flag_for_review() -> None:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        # High-conviction = an actionable BUY/SELL at a decisive score. Those
        # rows go to the review queue; everything else in the batch is cleared.
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(
            """
            UPDATE stock_recommendations
            SET review_status = CASE
                WHEN signal IN ('BUY', 'SELL') AND (score >= 70 OR score <= 35)
                THEN 'PENDING_REVIEW' ELSE 'AUTO_CLEARED'
            END
            WHERE run_id = (
                SELECT run_id FROM stock_recommendations
                ORDER BY created_at DESC LIMIT 1
            )
            """
        )
        print("Routed batch through analyst review: high-conviction rows flagged PENDING_REVIEW")

    @task
    def clear_for_delivery() -> None:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(
            """
            UPDATE stock_recommendations
            SET review_status = 'AUTO_CLEARED'
            WHERE run_id = (
                SELECT run_id FROM stock_recommendations
                ORDER BY created_at DESC LIMIT 1
            )
            """
        )
        print("Batch cleared for delivery; no analyst review required")

    init = initialize_snowflake_tables()
    snapshot = fetch_indicator_snapshot()
    headlines = fetch_recent_headlines()
    init >> [snapshot, headlines]
    sentiment = score_news_sentiment(headlines)
    scorecards = compute_scorecards(snapshot, sentiment)
    rationales = generate_rationales(scorecards)
    ranked = persist_recommendations(scorecards, rationales)
    route_review(ranked) >> [flag_for_review(), clear_for_delivery()]


trade_advisor()
