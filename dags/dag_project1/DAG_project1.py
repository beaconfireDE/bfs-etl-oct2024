import os
from datetime import datetime

from airflow import DAG

from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

from helper import create_prestage_table


SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'

SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_WAREHOUSE = 'BF_ETL1007'
SNOWFLAKE_STAGE = 's3_stage_trans_order'

S3_FILE_PATH_TEMPLATE = 's3://octde2024/airflow_project/Transaction_Team3_{{ ds_nodash }}.csv'


with DAG(
    "Team3_s3_to_s_incremental_DAG",
    start_date=datetime(2024, 11, 6),
    end_date = datetime(2024, 11, 8),
    schedule_interval='0 1 * * *',# UTC timezone, everyday at 1am
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    catchup=True,
    tags=['Team3']
) as dag:
    dag_create_prestage_table = create_prestage_table(task_id='create_prestage_table', snowflake_conn_id=SNOWFLAKE_CONN_ID)

    copy_into_prestg = CopyFromExternalStageToSnowflakeOperator(
        task_id='copy_into_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        stage=SNOWFLAKE_STAGE,
        table='prestage_Transaction_Team3',
        schema=SNOWFLAKE_SCHEMA,
        files=['Transaction_Team3_{{ ds_nodash }}.csv'],
        file_format="(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1, NULL_IF = ('NULL', 'null', ''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"')",
        pattern=".*[.]csv",
    )

    dag_create_prestage_table >> copy_into_prestg
          
        
    

