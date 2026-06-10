FROM astrocrpublic.azurecr.io/runtime:3.2-5

# dbt runs from its own virtualenv so its dependencies never conflict with
# Airflow's. Cosmos points at this binary via ExecutionConfig.
RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && \
    deactivate
