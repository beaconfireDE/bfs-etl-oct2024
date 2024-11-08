from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

# Snowflake Config
SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'
SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER'
SNOWFLAKE_WAREHOUSE = 'bf_etl1007'

PRESTAGE_TABLE = 'prestage_stationrecords_team1'

# Define the SQL to create the prestage table if it doesn’t exist
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{PRESTAGE_TABLE} (
    record_id INT,
    station_id VARCHAR,
    date DATE,
    temperature FLOAT,
    humidity INT,
    wind_speed FLOAT,
    precipitation FLOAT,
    station_name VARCHAR,
    zip_code INT,
    state VARCHAR,
    pressure INT,
    visibility INT,
    air_quality_index INT
);
"""

with DAG(
    "project1_s3_to_snowflake_team1",
    start_date=datetime(2024, 11, 6),
    end_date=datetime(2024, 11, 8),
    schedule_interval='20 2 * * *',
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
        files=['StationRecords_Team1_{{ ds }}.csv'],
        table=PRESTAGE_TABLE,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1, DATE_FORMAT = 'MM/DD/YY' \
            NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
            ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    )

    # Set dependencies
    create_table_if_not_exists >> copy_into_stage