"""
# Compute Indicators

Asset-triggered DAG: runs whenever `market_data_ingest` publishes fresh rows
to `stock_prices`. Uses Astronomer Cosmos to render the `trading_indicators`
dbt project (SMA trend, RSI-14, MACD, Bollinger z-score, volume ratio) as
Airflow tasks, materializing the `stock_indicators` mart in Snowflake, then
emits the `stock_indicators` Asset for the trade advisor.

Required Airflow configuration:
- Snowflake connection: `stock_signal_snowflake` (Cosmos builds the dbt
  profile from this connection at runtime — no profiles.yml needed)
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Asset, dag, task
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import LoadMode
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

SNOWFLAKE_CONN_ID = "stock_signal_snowflake"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
# Resolve the dbt project relative to this file so the path works both
# locally and inside Astro's versioned DAG-bundle extraction directories.
DBT_PROJECT_PATH = str(Path(__file__).parent / "dbt" / "trading_indicators")
DBT_EXECUTABLE_PATH = f"{AIRFLOW_HOME}/dbt_venv/bin/dbt"

STOCK_PRICES_ASSET = Asset(name="stock_prices")
STOCK_INDICATORS_ASSET = Asset(name="stock_indicators")

profile_config = ProfileConfig(
    profile_name="trading_indicators",
    target_name="prod",
    profile_mapping=SnowflakeUserPasswordProfileMapping(conn_id=SNOWFLAKE_CONN_ID),
)


@dag(
    dag_id="compute_indicators",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/New_York"),
    schedule=[STOCK_PRICES_ASSET],
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Astro", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["trade-advisor", "dbt", "onboarding"],
    doc_md=__doc__,
)
def compute_indicators():
    dbt_indicators = DbtTaskGroup(
        group_id="dbt_indicators",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH),
        # LoadMode.CUSTOM parses the dbt project with Cosmos's own parser, so
        # DAG parsing never needs the dbt binary or a live Snowflake profile.
        render_config=RenderConfig(load_method=LoadMode.CUSTOM),
    )

    @task(outlets=[STOCK_INDICATORS_ASSET])
    def publish_indicators() -> None:
        print("stock_indicators mart refreshed; asset event emitted")

    dbt_indicators >> publish_indicators()


compute_indicators()
