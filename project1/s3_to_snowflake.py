import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'

SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_WAREHOUSE = 'BF_ETL1007'
SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER'
SNOWFLAKE_TABLE = 'PRESTAGE_SALESDATA_6'


with DAG(
    "project1_s3_to_snowflake",
    start_date = datetime(2024, 11, 6),
    end_date = datetime(2024, 11, 8),
    schedule_interval='0 0 * * *', # send the data to snowflake every day at midnight
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['team6project1'],
    catchup=True,
) as dag:
    
    copy_into_prestg = CopyFromExternalStageToSnowflakeOperator(
        task_id='prestg_sales_data',
        files=['SalesData_Group6_{{ ds }}.csv'],
        # @TODO change to our table name if not already
        table=SNOWFLAKE_TABLE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
            NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
            ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',

    )
    copy_into_prestg
