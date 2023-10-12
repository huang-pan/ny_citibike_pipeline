## Ingest all months of NY Citibike trips Dynamic DAG
#  - Dynamically generates DAGs for each month
#  Author: Huang Pan
#  Citi Bike Dataset
#  - Link: [Index of bucket "tripdata"](https://s3.amazonaws.com/tripdata/index.html)
#  - Get all files that contains rides taken in 2013 and 2014.
#  - Load these files into a Postgres DB which will serve as your source of truth for the ride history. 
#    Bonus points for ingesting the data into Postgres using Airflow
#  This DAG dynamically generates DAGs for each month that:
#  - Depends on the ingest_postgres DAG
#  - Downloads Citibike trips from Postgres to GCS
#  - Uploads Citibike trips from GCS to BigQuery
from datetime import datetime
from dateutil.relativedelta import relativedelta
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# https://hevodata.com/learn/airflow-dynamic-dags/
#   use simplest method
# https://academy.astronomer.io/astro-runtime-dynamic-dags
def create_dag(dag_id,
               tripmonth,
               default_args):

    '''
    # Helper function to get previous month
    def get_prevmonth(month):
        format = "%Y%m"
        currmonth = datetime.strptime(month, format)
        prevmonth = currmonth - relativedelta(months=1)
        return prevmonth.strftime("%Y%m")
    '''

    dag = DAG(
        dag_id,
        max_active_runs=1,
#        schedule_interval="@monthly",
        schedule_interval=None,
        catchup=False,
        default_args=default_args,
    )

    with dag:
        '''
        # Wait for data from previous month to be loaded into BigQuery
        # Doesn't work for manual triggers
        # https://stackoverflow.com/questions/65833817/airflow-externaltasksensor-manually-triggered
        prevmonth = get_prevmonth(tripmonth)
        external_task_sensor1 = ExternalTaskSensor(
            task_id = "wait_sensor",
            external_dag_id=f"ingest_{prevmonth}",
            external_task_id = "gcs_to_big_query_operator_1",
        )
        '''
        # Download Citibike trips from Postgres to GCS
        postgres_to_gcs_operator_1 = PostgresToGCSOperator(
            task_id="postgres_to_gcs_operator_1",
            sql=f"SELECT starttime, stoptime, start_station_id, start_station_latitude, start_station_longitude, bikeid FROM raw.citibike_trips_{tripmonth}",
            bucket="ny-citibike-pipeline",
            filename=f"transfer/citibike-tripdata/citibike_trips_{tripmonth}.csv",
            export_format="csv",
            gcp_conn_id="bigquery_conn",
            postgres_conn_id="postgres_conn",
        )

        # Upload Citibike trips from GCS to BigQuery
        gcs_to_big_query_operator_1 = GCSToBigQueryOperator(
            task_id="gcs_to_big_query_operator_1",
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

        # Chain tasks together
        #external_task_sensor1 >> postgres_to_gcs_operator_1 >> gcs_to_big_query_operator_1
        postgres_to_gcs_operator_1 >> gcs_to_big_query_operator_1

    return dag

# Can put this into an Airflow Variable or BigQuery table
ALLMONTHS = ['201306', '201307', '201308', '201309', '201310', '201311', '201312', '201401', '201402', '201403', '201404', '201405', '201406', '201407', '201408', '201409', '201410', '201411', '201412']
for month in ALLMONTHS:
    dag_id = f"ingest_{month}"
    default_args = {
        "owner": "huangpan",
        "start_date": datetime(2023, 10, 1)
    }

    globals()[dag_id] = create_dag(dag_id,
                                   month,
                                   default_args)