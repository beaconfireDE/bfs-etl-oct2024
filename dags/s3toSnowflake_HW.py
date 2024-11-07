import os
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define constants
S3_BUCKET = 's3://octde2024/aiflow_project/'
SNOWFLAKE_CONN_ID = 'snowflake_conn'
DATABASE = 'AIRFLOW1007'
SCHEMA = 'BF_DEV'
TABLE_NAME = 'prestage_AQ20241106_Team2'
S3_FILE_NAME = 'AQ_Team2_20241106.csv'
STAGE_NAME = 'S3_STAGE_TRANS_ORDER'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Define the DAG
with DAG(
    'AQ_S3tosnowflake_Team2',
    default_args=default_args,
    start_date=datetime(2024, 11, 6),
    end_date=datetime(2024, 11, 8),
    schedule_interval='0 * * * *',  # Every hour, on the hour
    description='Load data from S3 to Snowflake',
    catchup=True,
) as dag:

    # Task to load data from S3 to Snowflake
    # load_data_task = S3ToSnowflakeOperator(
    #     task_id='load_data_to_snowflake',
    #     snowflake_conn_id=SNOWFLAKE_CONN_ID,
    #     s3_keys=[f"{S3_BUCKET}{S3_FILE_NAME}"],
    #     table=TABLE_NAME,
    #     schema=SCHEMA,
    #     stage=f"{DATABASE}.{SCHEMA}.{STAGE_NAME}",
    #     file_format='(type = "CSV", skip_header = 1, FIELD_OPTIONALLY_ENCLOSED_BY = \'"\' )',
    #     warehouse='compute_wh',
    #     role='accountadmin',
    # )
    create_table = SnowflakeOperator(
        task_id='create_snowflake_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql = 
            '''
            CREATE TABLE AIRFLOW1007.BF_DEV.prestage_AirQuality_Team2 (
            Date DATE,
            Time TIME,
            "CO(GT)" FLOAT,
            "PT08.S1(CO)" INT,
            "NMHC(GT)" INT,
            "C6H6(GT)" FLOAT,
            "PT08.S2(NMHC)" INT,
            "NOx(GT)" INT,
            "PT08.S3(NOx)" INT,
            "NO2(GT)" INT,
            "PT08.S4(NO2)" INT,
            "PT08.S5(O3)" INT,
            T FLOAT,
            RH FLOAT,
            AH FLOAT)
            '''
    )
    copy_into_table = SnowflakeOperator(
        task_id='s3_to_snowflake',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql = 
        '''
        COPY INTO AIRFLOW1007.BF_DEV.prestage_AirQuality_Team2
        FROM @AIRFLOW1007.BF_DEV.S3_STAGE_TRANS_ORDER.AQ_Team2_20241106
        FILE_FORMAT = csv_format;

        COPY INTO "AIRFLOW1007"."BF_DEV"."PRESTAGE_AIRQUALITY_TEAM2" 
        FROM (
            SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
            FROM '@"AIRFLOW1007"."BF_DEV"."S3_STAGE_TRANS_ORDER"'
        )
        FILES = ('AQ_Team2_20241106.csv', 'AQ_Team2_20241107.csv', 'AQ_Team2_20241108.csv') 
        FILE_FORMAT = csv_format
        ON_ERROR=ABORT_STATEMENT
        '''
    )
    # copy_into_table = CopyFromExternalStageToSnowflakeOperator(
    #     task_id='s3_to_snowflake_load',
    #     files = [f"{S3_BUCKET}{S3_FILE_NAME}"],
    #     table = TABLE_NAME,
    #     schema = SCHEMA,
    #     stage = f"{DATABASE}.{SCHEMA}.{STAGE_NAME}",
    #     file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
    #         NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
    #         ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    # )

    create_table >> copy_into_table
