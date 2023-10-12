FROM quay.io/astronomer/astro-runtime:9.1.0

ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
ENV AIRFLOW__ASTRO_SDK__DATAFRAME_ALLOW_UNSAFE_STORAGE=True
ENV AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=30

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres && \
    pip install --no-cache-dir dbt-bigquery && deactivate