## Run dbt DAG
#  Author: Huang Pan
#  Use dbt models to answer the following qs:
#  - How many bike rides were taken during the holidays
#  - Which zip codes are the most popular in terms of starting a bike ride
#  This DAG:
#  - Depends on the ingest_allmonths, ingest_holidays DAGs
#  - Runs dbt models to answer the above questions
#  - Displays dbt model build status in the Airflow UI
#  TO DO:
#  - Use profile_mapping for profile_config (currently bugged, need to file Astro ticket)
#    https://astronomer.github.io/astronomer-cosmos/profiles/index.html
#    https://astronomer.github.io/astronomer-cosmos/profiles/GoogleCloudServiceAccountDict.html
import os
from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup
#from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

# Astronomer Cosmos dev / prod dbt connection set up here
profile_config = ProfileConfig(
    profile_name="bigquery",
    target_name=Variable.get("ENV_TYPE", default_var="dev"), # Set in Airflow UI of Airflow dev / prod instance
    profiles_yml_filepath="/usr/local/airflow/dbt/profiles.yml",
    #profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
    #    conn_id='bigquery_conn',
    #    profile_args = {
    #        "project": "ny-citbike-pipeline",
    #        "dataset": "cbdev"
    #    }
    #)
)

default_args = {
    "owner": "huangpan",
    "start_date": datetime(2023, 10, 1),
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="run_dbt",
    max_active_runs=1,
#    schedule_interval="@monthly",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
):
    # Can replace with ExternalTaskSensor or Dataset if we have a schedule
    e1 = EmptyOperator(task_id="pre_dbt")

    # Displays dbt model build status in the Airflow UI
    dbt_tg = DbtTaskGroup(
        group_id="ny-citibike-pipeline-dbt",
        project_config=ProjectConfig(
            "/usr/local/airflow/dbt",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
        ),
    )

    e2 = EmptyOperator(task_id="post_dbt")

    # Chain the tasks together
    e1 >> dbt_tg >> e2