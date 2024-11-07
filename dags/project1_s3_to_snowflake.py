# s3://octde2024/airflow_project/StationRecords_Team1_2024-11-06.csv
from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

# Snowflake Config
SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'
SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER'

PRESTAGE_TABLE = 'prestg_stationrecords_team1'
STAGING_TABLE = 'stg_stationrecords_team1'

# AWS S3 Config
S3_BUCKET_NAME = 'octde2024'

# Define the SQL to create the prestage table if it doesn’t exist
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{PRESTAGE_TABLE} (
    record_id INT,
    station_id STRING,
    date DATE,
    temperature FLOAT,
    humidity INT,
    wind_speed FLOAT,
    precipitation FLOAT,
    station_name STRING,
    zip_code INT,
    state STRING,
    pressure INT,
    visibility INT,
    air_quality_index INT
);
"""
# Define the SQL to insert the staging table into prestage table
INSERT_INTO_PRESTAGE_SQL = f"""
INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{PRESTAGE_TABLE}
SELECT * FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{STAGING_TABLE};
"""

# Generate the dynamic file name based on today's date
current_date = datetime.now().strftime('%Y-%m-%d')
file_name = f'StationRecords_Team1_{current_date}.csv'

with DAG(
    "project1_s3_to_snowflake_team1",
    start_date=datetime(2024, 11, 6),
    end_date=datetime(2024, 11, 8),
    schedule_interval='0 5 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=True,
) as dag:

    # Task 1: Check if the prestage table exists and create it if not
    create_table_if_not_exists = SnowflakeOperator(
        task_id='create_table_if_not_exists',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=CREATE_TABLE_SQL
    )
    
    # Task 2: Load data into Snowflake if the file exists
    copy_into_stage = CopyFromExternalStageToSnowflakeOperator(
        task_id='stage_stationrecords',
        files=[file_name],
        table=STAGING_TABLE,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
            NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
            ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    )

    # Task 3: Insert data from staging to prestage table
    insert_into_prestage = SnowflakeOperator(
        task_id='insert_into_prestage',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=INSERT_INTO_PRESTAGE_SQL
    )

    # Set dependencies
    create_table_if_not_exists >> copy_into_stage >> insert_into_prestage