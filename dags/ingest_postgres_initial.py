## Ingest NY Citibike Trips to Postgres DB Initial Dev DAG
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
#  - Loading cleaned dataframes into Postgres: quick and dirty solution. 
#    Can improve this by breaking up the individual csv loads into separate tasks and running them in parallel (need to upsize postgresDB).
#    See: ingest_postgres_allmonths DAG
#         https://stackoverflow.com/questions/67889241/airflow-chaining-tasks-in-parallel
#         https://stackoverflow.com/questions/76371020/how-can-i-create-a-task-workflow-in-airflow-based-on-a-list
from datetime import datetime
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import os
import pandas as pd
import zipfile
from sqlalchemy import create_engine
from airflow.decorators import dag, task
from airflow.models import Variable


postgres_password = Variable.get("POSTGRES_PASSWORD", default_var="falsepassword")
engine = create_engine(f"postgresql://postgres:{postgres_password}@104.196.255.61:5432/ny-citibike-pipeline")
conn = engine.raw_connection()
cur = conn.cursor()

default_args = {
    "owner": "huangpan",
    "start_date": datetime(2023, 10, 1),
}

# https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html
@dag(
     dag_id="ingest_postgres_initial",
     max_active_runs=1,
#     schedule_interval="@monthly",
     schedule_interval=None,
     catchup=False,
     default_args=default_args,
)
def ingest_postgres():

    # Create the Postgres table
    @task()
    def create_postgres_table():
        cur.execute("""
        set schema 'raw';

        drop table if exists citibike_trips;

        create table if not exists citibike_trips
        (
            tripduration integer,
            starttime timestamp without time zone,
            stoptime timestamp without time zone,
            start_station_id integer,
            start_station_name character varying,
            start_station_latitude double precision,
            start_station_longitude double precision,
            end_station_id integer,
            end_station_name character varying,
            end_station_latitude double precision,
            end_station_longitude double precision,
            bikeid integer,
            usertype character varying,
            birth_year integer,
            gender character varying
        );          
        """)
        conn.commit()

    # Download 2013 / 2014 zipped files from https://s3.amazonaws.com/tripdata/index.html to local disk and unzip them
    # Load the cleaned pandas dataframes into Postgres
    # NOTE: This is a quick and dirty solution - load takes a long time.
    #       Can improve this by breaking up the individual csv loads into separate tasks and running them in parallel (need to upsize postgresDB).
    @task()
    def load_postgres():
        # https://stackoverflow.com/questions/75796595/download-files-from-a-public-s3-bucket
        # Create the s3 client and assign credentials (UNSIGEND for public bucket)
        client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

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

        # create a list of "Contect" objects from the s3 bucket
        list_files = client.list_objects(Bucket="tripdata")["Contents"]
        if not os.path.isdir("./cbdata/"):
            os.mkdir("./cbdata/")

        print(FILE_DICT.keys())
        for key in list_files:
            filename = key["Key"]
            if filename in FILE_DICT.keys():
                print(filename)
                # Download zip file
                client.download_file(Bucket="tripdata", # assign bucket name
                                     Key=filename,
                                     Filename=os.path.join("./cbdata/", filename)) # local storage file path
                # Unzip file
                with zipfile.ZipFile("./cbdata/" + filename, "r") as zip_ref:
                    zip_ref.extractall("./cbdata/")
                # Remove zip file
                os.remove("./cbdata/" + filename)
                # Clean csv file: output in consistent format
                print(FILE_DICT[filename])
                df = pd.read_csv("./cbdata/" + FILE_DICT[filename], na_values="\\N") # NULL, \N, ""
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
                df.to_sql("citibike_trips", engine, schema="raw", index=False, if_exists="append")
                conn.commit()
                print("Done loading " + filename + " into Postgres")

    # Chain the tasks together
    create_postgres_table() >> load_postgres()

dag = ingest_postgres()