# ny-citibike-pipeline

You are assigned an exercise to analyze the NY Citibike trip data and come up with some statistics to present to your team to better understand usage of the bikes.

Citi Bike Dataset
- Link: [Index of bucket "tripdata"](https://s3.amazonaws.com/tripdata/index.html)
- Get all files that contains rides taken in 2013 and 2014.
- Load these files into a Postgres DB which will serve as your source of truth for the ride history. Bonus points for ingesting the data into Postgres using Airflow

US Zip Codes
- [All US zip codes with their corresponding latitude and longitude coordinates. Comma delimited for your database goodness. Source: http://www.census.gov/geo/maps-data/data/gazetteer.html (github.com)](https://gist.github.com/erichurst/7882666)
- This contains all US zip codes and their geo-coordinates
- Assume that the lat/long is the geo center of the zip code and that all zip codes are 1 mile in radius from that center lat/long.

US Public Holidays
- [date.nager.at/api/v2/publicholidays/2013/US](https://date.nager.at/api/v2/publicholidays/2013/US)
- [date.nager.at/api/v2/publicholidays/2014/US](https://date.nager.at/api/v2/publicholidays/2014/US)
- API to get 2013 and 2014 US holidays.

Create a data pipeline to load the Citibike data from Postgres into BigQuery or Snowflake (your choice). Load the Zip Codes and Holidays into the DWH as well. Use dbt models to answer the following qs:

1. How many bike rides were taken during the holidays
2. Which zip codes are the most popular in terms of starting a bike ride

Please note: for dimensional modeling, a star schema can be created from the original source data:\
https://github.com/huang-pan/ny_citibike_pipeline/blob/main/docs/citibike_trips_schema.txt
- The fact in the fact table is trip_duration
- You can have these dimension tables:
	- time / date at various granularities:
 		- time_id (HH:MM, 24 hours per day), is_morning, is_afternoon, is_evening
 		- date_id (365 per year), is_holiday, is_weekend, is_weekday
   		- you can generate the above dimension tables using a python script, save the tables into a csv, and ingest the csv into the data warehouse using the dbt seed command
     		- the dbt-utils date-spine macro can also be used\ https://discourse.getdbt.com/t/finding-active-days-for-a-subscription-user-account-date-spining/265
	- station: station_id, station_name, station_latitude, station_longitude
 	- bike: bike+id
  	- user: user_id key created from usertype, birth_year, gender

## Tools used:
- Astronomer / Cosmos / Airflow 2.7+
- dbt-core 1.6+, dbt-expectations
- GCP Cloud SQL Postgres DB, Cloud Storage, BigQuery
- Github Actions CI / CD

![cosmos_dbt](https://github.com/huang-pan/ny-citibike-pipeline/assets/10567714/b284c333-536a-4c6d-ad23-d732de2fef6c)


TO DO:
- Trigger dependent DAGs like dbt_run using datasets or ExternalTaskSensor if there is a schedule
- Add good data engineering best practices to Airflow pipelines: better error logging, more testing, etc. See https://www.soda.io/
- Clean up dbt SQL: https://sqlfluff.com/, sqlfmt.com
- Use Dbt data contracts: model versioning
	- https://docs.getdbt.com/docs/collaborate/govern/model-versions
	- https://docs.getdbt.com/reference/model-properties 
- Use Dbt semantic models if more complex metrics are needed
    - https://docs.getdbt.com/guides/best-practices/how-we-build-our-metrics/semantic-layer-3-build-semantic-models
	- just a dbt way of organizing and expressing KPIs consistently across the data warehouse
		- semantic models != logical layer models
			- https://github.com/dbt-labs/jaffle-sl-template/blob/main/models/marts/customers.yml 
	- entities: these describe the relationships between various semantic models (think ids)
	- dimensions: these are the columns you want to slice, dice, group, and filter by
	- measures: these are the quantitative values you want to aggregate, base measures that go into metrics / aggregations / KPIs
 - Try Y42 as cloud IDE for Dbt: https://youtu.be/_reNgMlqYu0?si=B6jUEOMsXqOojv1w


Overview
========

Welcome to Astronomer! This project was generated after you ran 'astro dev init' using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

Project Contents
================

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes an example DAG that runs every 30 minutes and simply prints the current date. It also includes an empty 'my_custom_function' that you can fill out to execute Python code.
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Deploy Your Project Locally
===========================

1. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 3 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks

2. Verify that all 3 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either stop your existing Docker containers or change the port.

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://docs.astronomer.io/cloud/deploy-code/

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support team: https://support.astronomer.io/
