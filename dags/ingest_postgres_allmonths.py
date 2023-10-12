## Ingest NY Citibike Trips to Postgres DB All Months DAG
#  Author: Huang Pan
#  Citi Bike Dataset
#  - Link: [Index of bucket "tripdata"](https://s3.amazonaws.com/tripdata/index.html)
#  - Get all files that contains rides taken in 2013 and 2014.
#  - Load these files into a Postgres DB which will serve as your source of truth for the ride history. 
#    Bonus points for ingesting the data into Postgres using Airflow
#  This DAG:
#  - Downloads 2013 / 2014 zipped files from https://s3.amazonaws.com/tripdata/index.html to local disk
#  - Unzips the files and cleans them in a pandas dataframe
#  - Loads the cleaned dataframes into Postgres
#  TO DO:
#  - Can probably combine cleaning / type casting steps into pandas dataframe and reduce number of SQL statements
from datetime import datetime
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import os
import pandas as pd
import zipfile
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    "owner": "huangpan",
    "start_date": datetime(2023, 10, 1),
}

# https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html
with DAG(
     dag_id="ingest_postgres_allmonths",
     max_active_runs=1,
#     schedule_interval="@monthly",
     schedule_interval=None,
     catchup=False,
     default_args=default_args,
):

    # https://s3.amazonaws.com/tripdata/201306-citibike-tripdata.zip
    # Can put this into an Airflow Variable or BigQuery table
    FILE_DICT = {'201306-citibike-tripdata.zip':'201306-citibike-tripdata.csv',
                 '201307-citibike-tripdata.zip':'2013-07 - Citi Bike trip data.csv',
                 '201308-citibike-tripdata.zip':'2013-08 - Citi Bike trip data.csv',
                 '201309-citibike-tripdata.zip':'2013-09 - Citi Bike trip data.csv', 
                 '201310-citibike-tripdata.zip':'2013-10 - Citi Bike trip data.csv', 
                 '201311-citibike-tripdata.zip':'2013-11 - Citi Bike trip data.csv', 
                 '201312-citibike-tripdata.zip':'2013-12 - Citi Bike trip data.csv', 
                 '201401-citibike-tripdata.zip':'2014-01 - Citi Bike trip data.csv', 
                 '201402-citibike-tripdata.zip':'2014-02 - Citi Bike trip data.csv', 
                 '201403-citibike-tripdata.zip':'2014-03 - Citi Bike trip data.csv', 
                 '201404-citibike-tripdata.zip':'2014-04 - Citi Bike trip data.csv', 
                 '201405-citibike-tripdata.zip':'2014-05 - Citi Bike trip data.csv', 
                 '201406-citibike-tripdata.zip':'2014-06 - Citi Bike trip data.csv', 
                 '201407-citibike-tripdata.zip':'2014-07 - Citi Bike trip data.csv', 
                 '201408-citibike-tripdata.zip':'2014-08 - Citi Bike trip data.csv', 
                 '201409-citibike-tripdata.zip':'201409-citibike-tripdata.csv', 
                 '201410-citibike-tripdata.zip':'201410-citibike-tripdata.csv', 
                 '201411-citibike-tripdata.zip':'201411-citibike-tripdata.csv', 
                 '201412-citibike-tripdata.zip':'201412-citibike-tripdata.csv'}
    op_list = []

    # Download Citibike trips from S3 to local disk
    def download_zipped():
        # https://stackoverflow.com/questions/75796595/download-files-from-a-public-s3-bucket
        # Create the s3 client and assign credentials (UNSIGEND for public bucket)
        client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

        # create a list of "Contect" objects from the s3 bucket
        list_files = client.list_objects(Bucket="tripdata")["Contents"]

        print(FILE_DICT.keys())
        for key in list_files:
            filename = key["Key"]
            if filename in FILE_DICT.keys():
                print(filename)
                file_month = filename[:6]
                # Download zip file
                client.download_file(Bucket="tripdata", # assign bucket name
                                     Key=filename,
                                     Filename=os.path.join("./", filename)) # local storage file path
                # Unzip file
                with zipfile.ZipFile("./" + filename, "r") as zip_ref:
                    zip_ref.extractall("./")
                # Remove zip file
                os.remove("./" + filename)
                # Clean csv file: output in consistent format
                print(FILE_DICT[filename])
                df = pd.read_csv(FILE_DICT[filename], na_values="\\N") # NULL, \N, ""
                df.rename(columns={"start station id": "start_station_id",
                                   "start station name": "start_station_name",
                                   "start station latitude": "start_station_latitude",
                                   "start station longitude": "start_station_longitude",
                                   "end station id": "end_station_id",
                                   "end station name": "end_station_name",
                                   "end station latitude": "end_station_latitude",
                                   "end station longitude": "end_station_longitude",
                                   "birth year": "birth_year"}, inplace=True)
                print(df.head())
                print(df.dtypes)
                # https://stackoverflow.com/questions/69908003/dataframe-to-postgresql-db
                # Use this method instead of PostgresHook due to null cleaning above
                # https://patrickod.com/2022/11/29/til-a-simple-etl-task-in-airflow-using-postgreshook/
                #df.to_csv(FILE_DICT[filename], index=False)
                #print("Done exporting " + FILE_DICT[filename])
                output_filename = file_month + "-citibike-tripdata.csv"
                df.to_csv(output_filename, index=False)
                print("Done exporting " + output_filename)

    op_list += [
        PythonOperator(
            task_id=f"download_zipped",
            python_callable=download_zipped,
            show_return_value_in_logs="True",
        )
    ]

    # Chains all the tasks together
    for input_file in FILE_DICT.keys():

        file_month = input_file[:6]
        print(input_file)
        print(file_month)

        # Create the Postgres table
        # Ingest to public schema for now; original data is in raw schema
        SQL_STATEMENT = f"""
            set schema 'public';

            drop table if exists citibike_trips_temp;

            create table if not exists citibike_trips_temp
            (
                tripduration integer,
                starttime character varying,
                stoptime character varying,
                start_station_id integer,
                start_station_name character varying,
                start_station_latitude double precision,
                start_station_longitude double precision,
                end_station_id double precision,
                end_station_name character varying,
                end_station_latitude double precision,
                end_station_longitude double precision,
                bikeid integer,
                usertype character varying,
                birth_year double precision,
                gender integer
            );
        """
        op_list += [
            PostgresOperator(
                task_id=f"create_postgres_table_temp_{file_month}",
                database="ny-citibike-pipeline",
                retry_on_failure="False",
                sql=SQL_STATEMENT,
                autocommit="True",
                postgres_conn_id="postgres_conn",
            )
        ]

        # Load csv file into Postgres
        # https://patrickod.com/2022/11/29/til-a-simple-etl-task-in-airflow-using-postgreshook/
        def load_postgres(file_month):
            pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
            pg_hook.copy_expert(
                f"COPY citibike_trips_temp(tripduration, starttime, stoptime, start_station_id, start_station_name, start_station_latitude, start_station_longitude, end_station_id, end_station_name, end_station_latitude, end_station_longitude, bikeid, usertype, birth_year, gender) FROM STDIN WITH CSV HEADER",
                filename=f"{file_month}-citibike-tripdata.csv"
            )

        op_list += [
            PythonOperator(
                task_id=f"load_postgres_{file_month}",
                python_callable=load_postgres,
                op_args=[file_month],
                show_return_value_in_logs="True",
            )
        ]

        if file_month in ['201409', '201410', '201411', '201412']:
            # starttime, stoptime format is different
            SQL_STATEMENT = f"""
                drop table if exists citibike_trips_{file_month};

                create table citibike_trips_{file_month} as
                select
                    tripduration,
                    to_timestamp(starttime, 'MM/DD/YYYY hh24:mi:ss')::timestamp without time zone starttime,
                    to_timestamp(stoptime, 'MM/DD/YYYY hh24:mi:ss')::timestamp without time zone stoptime,
                    start_station_id,
                    start_station_name,
                    start_station_latitude,
                    start_station_longitude,
                    floor(end_station_id::numeric)::integer end_station_id,
                    end_station_name,
                    end_station_latitude,
                    end_station_longitude,
                    bikeid,
                    usertype,
                    floor(birth_year::numeric)::integer birth_year,
                    gender::varchar gender
                from citibike_trips_temp;

                drop table if exists citibike_trips_temp;
            """
        else:
            SQL_STATEMENT = f"""
                drop table if exists citibike_trips_{file_month};

                create table citibike_trips_{file_month} as
                select
                    tripduration,
                    to_timestamp(starttime, 'YYYY-MM-DD hh24:mi:ss')::timestamp without time zone starttime,
                    to_timestamp(stoptime, 'YYYY-MM-DD hh24:mi:ss')::timestamp without time zone stoptime,
                    start_station_id,
                    start_station_name,
                    start_station_latitude,
                    start_station_longitude,
                    floor(end_station_id::numeric)::integer end_station_id,
                    end_station_name,
                    end_station_latitude,
                    end_station_longitude,
                    bikeid,
                    usertype,
                    floor(birth_year::numeric)::integer birth_year,
                    gender::varchar gender
                from citibike_trips_temp;

                drop table if exists citibike_trips_temp;
            """            
        op_list += [
            PostgresOperator(
                task_id=f"create_postgres_table_{file_month}",
                database="ny-citibike-pipeline",
                retry_on_failure="False",
                sql=SQL_STATEMENT,
                autocommit="True",
                postgres_conn_id="postgres_conn",
            )
        ]

    # Chain the tasks together
    chain(*op_list)
