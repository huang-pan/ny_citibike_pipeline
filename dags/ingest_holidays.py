## Ingest US Holidays DAG
#  Author: Huang Pan
#  US Public Holidays
#  - [date.nager.at/api/v2/publicholidays/2013/US](https://date.nager.at/api/v2/publicholidays/2013/US)
#  - [date.nager.at/api/v2/publicholidays/2014/US](https://date.nager.at/api/v2/publicholidays/2014/US)
#  - API to get 2013 and 2014 US holidays.
#  This DAG:
#  - Downloads US holidays from API to local disk
#  - Uploads US holidays from local disk to GCS
#  - Loads US holidays from GCS to BigQuery
import requests
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    "owner": "huangpan",
    "start_date": datetime(2023, 10, 1),
}

# https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html
@dag(
     dag_id="ingest_holidays",
     max_active_runs=1,
#     schedule_interval="@monthly",
     schedule_interval=None,
     catchup=False,
     default_args=default_args,
)
def ingest_holidays():
    # Helper function to download US holidays from API to local disk
    def download_holidays(year):
        resp = requests.get(f"https://date.nager.at/api/v2/publicholidays/{year}/US")
        # https://stackoverflow.com/questions/1871524/how-can-i-convert-json-to-csv
        df = pd.json_normalize(resp.json())
        df.to_csv(f"us-public-holidays-{year}.csv", encoding="utf-8", index=False)

    # Download US holidays from API to local disk
    @task()
    def download_holidays_2013():
        download_holidays("2013")

    # Upload US holidays from local disk to GCS
    local_to_gcs_operator_2013 = LocalFilesystemToGCSOperator(
        task_id="local_to_gcs_operator_2013",
        src="us-public-holidays-2013.csv",
        dst="transfer/us-public-holidays/us-public-holidays-2013.csv",
        bucket="ny-citibike-pipeline",
        gcp_conn_id="bigquery_conn",
    )

    # Load US holidays from GCS to BigQuery
    gcs_to_big_query_operator_holidays_2013 = GCSToBigQueryOperator(
        task_id="gcs_to_big_query_operator_holidays_2013",
        bucket="ny-citibike-pipeline",
        source_objects="transfer/us-public-holidays/us-public-holidays-2013.csv",
        destination_project_dataset_table="ny-citibike-pipeline.raw.us_holidays_2013",
        autodetect=True,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="bigquery_conn",
    )

    # Download US holidays from API to local disk
    @task()
    def download_holidays_2014():
        download_holidays("2014")

    # Upload US holidays from local disk to GCS
    local_to_gcs_operator_2014 = LocalFilesystemToGCSOperator(
        task_id="local_to_gcs_operator_2014",
        src="us-public-holidays-2014.csv",
        dst="transfer/us-public-holidays/us-public-holidays-2014.csv",
        bucket="ny-citibike-pipeline",
        gcp_conn_id="bigquery_conn",
    )

    # Load US holidays from GCS to BigQuery
    gcs_to_big_query_operator_holidays_2014 = GCSToBigQueryOperator(
        task_id="gcs_to_big_query_operator_holidays_2014",
        bucket="ny-citibike-pipeline",
        source_objects="transfer/us-public-holidays/us-public-holidays-2014.csv",
        destination_project_dataset_table="ny-citibike-pipeline.raw.us_holidays_2014",
        autodetect=True,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="bigquery_conn",
    )

    # Chain the tasks together
    download_holidays_2013() >> local_to_gcs_operator_2013 >> gcs_to_big_query_operator_holidays_2013
    download_holidays_2014() >> local_to_gcs_operator_2014 >> gcs_to_big_query_operator_holidays_2014

dag = ingest_holidays()