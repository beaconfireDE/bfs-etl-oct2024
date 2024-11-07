# -*- coding: utf-8 -*-
"""Untitled25.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1vO1OlAdxqDrOgn_71dmy16or21P3pfPc
"""

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# Project-specific variables
SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'
SNOWFLAKE_ROLE = 'AW_developer'
SNOWFLAKE_WAREHOUSE = 'aw_etl'
SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER'
TABLE_NAME = 'StationRecords_Team1_'
DAG_ID = "airflow_s3_to_snowflake_project"

# SQL Commands

# 1. Create a table with 10 columns
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SCHEMA}.{TABLE_NAME} "
    "(record_id INT, station_id INT, date DATE, temperature FLOAT, humidity FLOAT, "
    "wind_speed FLOAT, precipitation FLOAT, station_name VARCHAR(255), zip_code VARCHAR(10), "
    "state VARCHAR(2), pressure FLOAT, visibility FLOAT, air_quality_index INT);"
)

# 2. Copy data incrementally from S3 to Snowflake
COPY_DATA_SQL = (
    f"COPY INTO {SNOWFLAKE_SCHEMA}.{TABLE_NAME} "
    f"FROM 's3://octde2024/aiflow_project/StationRecords_Team1_{{{{ ds }}}}.csv' "
    "FILE_FORMAT = (type = 'CSV', field_delimiter = ',', skip_header = 1);"
)
# Define the DAG
with DAG(
    DAG_ID,
    start_date=datetime(2024, 11, 6),
    schedule_interval='@daily',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    catchup=False,
    tags=['s3_to_snowflake'],
) as dag:

    # Task 1: Create the Snowflake Table
    create_table = SnowflakeOperator(
        task_id='create_snowflake_table',
        sql=CREATE_TABLE_SQL_STRING,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    # Task 2: Load data incrementally from S3 to Snowflake (use the current execution date)
    copy_data_to_snowflake = SnowflakeOperator(
        task_id='copy_data_to_snowflake',
        sql=COPY_DATA_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    # Define Task Dependencies
    create_table >> copy_data_to_snowflake