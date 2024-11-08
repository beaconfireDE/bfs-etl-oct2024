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
SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_WAREHOUSE = 'bf_etl1007'
SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER'
TABLE_NAME = 'plant_data_'
DAG_ID = "test_team1"

# SQL Commands

# 1. Create a table with 10 columns
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SCHEMA}.{TABLE_NAME} "
    "(Plant_ID VARCHAR(10) PRIMARY KEY,
    Plant_Name VARCHAR(50) NOT NULL,
    Species VARCHAR(50),
    Family VARCHAR(50),
    Height_cm INT,
    Leaf_Color VARCHAR(20),
    Bloom_Season VARCHAR(20),
    Water_Needs VARCHAR(20),
    Soil_Type VARCHAR(20),
    Growth_Rate VARCHAR(20));"
)

# 2. Copy data incrementally from S3 to Snowflake
COPY_DATA_SQL = (
    f"COPY INTO {SNOWFLAKE_SCHEMA}.{TABLE_NAME} "
    f"FROM 's3://octde2024/aiflow_project/plant_data_{{{{ ds }}}}.csv' "
    "FILE_FORMAT = (type = 'CSV', field_delimiter = ',', skip_header = 1);"
)
# Define the DAG
with DAG(
    DAG_ID,
    start_date=datetime(2024, 11, 8),
    schedule_interval='0 17 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    catchup=False,
    tags=['s3_to_snowflake_team1-1'],
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