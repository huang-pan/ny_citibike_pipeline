## Ingest all months of NY Citibike trips DAG
#  Author: Huang Pan
#  Citi Bike Dataset
#  - Link: [Index of bucket "tripdata"](https://s3.amazonaws.com/tripdata/index.html)
#  - Get all files that contains rides taken in 2013 and 2014.
#  - Load these files into a Postgres DB which will serve as your source of truth for the ride history. 
#    Bonus points for ingesting the data into Postgres using Airflow
#  This DAG:
#  - Depends on the ingest_postgres DAG
#  - Downloads Citibike trips from Postgres to GCS
#  - Uploads Citibike trips from GCS to BigQuery
#  Other possible solutions:
#  - https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database
#  - https://dlthub.com/docs/dlt-ecosystem/destinations/bigquery
from datetime import datetime
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    "owner": "huangpan",
    "start_date": datetime(2023, 10, 1)
}

with DAG(
    dag_id=f"ingest_allmonths",
    max_active_runs=1,
#    schedule_interval="@monthly",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
):
    # https://stackoverflow.com/questions/71144263/airflow-tasks-iterating-over-list-should-run-sequentially
    # Can put this into an Airflow Variable or BigQuery table
    ALLMONTHS = ['201306', '201307', '201308', '201309', '201310', '201311', '201312', '201401', '201402', '201403', '201404', '201405', '201406', '201407', '201408', '201409', '201410', '201411', '201412']
    op_list = []

    # Chains all the tasks together
    for tripmonth in ALLMONTHS:
        op_list += [
            # Download Citibike trips from Postgres to GCS
            PostgresToGCSOperator(
                task_id=f"postgres_to_gcs_operator_{tripmonth}",
                sql=f"SELECT starttime, stoptime, start_station_id, start_station_latitude, start_station_longitude, bikeid FROM raw.citibike_trips_{tripmonth}",
                bucket="ny-citibike-pipeline",
                filename=f"transfer/citibike-tripdata/citibike_trips_{tripmonth}.csv",
                export_format="csv",
                gcp_conn_id="bigquery_conn",
                postgres_conn_id="postgres_conn",
            )
        ]

        op_list += [
            # Upload Citibike trips from GCS to BigQuery
            GCSToBigQueryOperator(
                task_id=f"gcs_to_big_query_operator_{tripmonth}",
                bucket="ny-citibike-pipeline",
                source_objects=f"transfer/citibike-tripdata/citibike_trips_{tripmonth}.csv",
                destination_project_dataset_table=f"ny-citibike-pipeline.raw.citibike_trips_{tripmonth}",
                autodetect=False,
                skip_leading_rows=1,
                schema_object="transfer/citibike-tripdata/citibike_trips_schema.json",
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_TRUNCATE",
                gcp_conn_id="bigquery_conn",
            )
        ]

    # Chains all the tasks together
    chain(*op_list)
