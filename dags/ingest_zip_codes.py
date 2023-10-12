## Ingest Zip Codes DAG
#  Author: Huang Pan
#  US Zip Codes
#  - [All US zip codes with their corresponding latitude and longitude coordinates. Comma delimited for your database goodness. 
#    Source: http://www.census.gov/geo/maps-data/data/gazetteer.html (github.com)](https://gist.github.com/erichurst/7882666)
#  - This contains all US zip codes and their geo-coordinates
#  - Assume that the lat/long is the geo center of the zip code and that all zip codes are 1 mile in radius from that center lat/long.
#  This DAG:
#  - Downloads zip codes from API to local disk
#  - Uploads zip codes from local disk to GCS
#  - Loads zip codes from GCS to BigQuery
import csv
import requests
from io import StringIO
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
     dag_id="ingest_zip_codes",
     max_active_runs=1,
#     schedule_interval="@monthly",
     schedule_interval=None,
     catchup=False,
     default_args=default_args,
)
def ingest_zip_codes():
    # Download zip codes from API to local disk
    @task()
    def download_zip_codes():
        # https://stackoverflow.com/questions/23464138/downloading-and-accessing-data-from-github-python
        resp = requests.get("https://gist.githubusercontent.com/erichurst/7882666/raw/5bdc46db47d9515269ab12ed6fb2850377fd869e/US%2520Zip%2520Codes%2520from%25202013%2520Government%2520Data")
        # https://stackoverflow.com/questions/42834861/csv-file-from-string-output
        buffer = StringIO(resp.text)
        reader = csv.reader(buffer, skipinitialspace=True)
        with open("us_zip_codes.csv", "w") as out_file:
            writer = csv.writer(out_file)
            writer.writerows(reader)

    # Upload zip codes from local disk to GCS
    local_to_gcs_operator_zip = LocalFilesystemToGCSOperator(
        task_id="local_to_gcs_operator_zip",
        src="us_zip_codes.csv",
        dst="transfer/us-zip-codes/us_zip_codes.csv",
        bucket="ny-citibike-pipeline",
        gcp_conn_id="bigquery_conn",
    )

    # Load zip codes from GCS to BigQuery
    gcs_to_big_query_operator_zip = GCSToBigQueryOperator(
        task_id="gcs_to_big_query_operator_zip",
        bucket="ny-citibike-pipeline",
        source_objects="transfer/us-zip-codes/us_zip_codes.csv",
        destination_project_dataset_table="ny-citibike-pipeline.raw.us_zip_codes",
        autodetect=True,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="bigquery_conn",
    )

    # Chain the tasks together
    download_zip_codes() >> local_to_gcs_operator_zip >> gcs_to_big_query_operator_zip

dag = ingest_zip_codes()