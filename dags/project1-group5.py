from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'
SNOWFLAKE_STAGE = 's3://octde2024/airflow_project/AirFlow_project_group5/'
PRESTAGE_TABLE = 'prestage_table_group5'


CREATE_TABLE_SQL = f"""
CREATE OR REPLACE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{PRESTAGE_TABLE} (
    stock_id VARCHAR,
    date DATE,
    open_price FLOAT,
    close_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    volume INT,
    company_name VARCHAR,
    sector VARCHAR,
    market_cap FLOAT
);
"""


with DAG(
    's3_to_snowflake_dag',
    start_date=datetime(2024, 11, 6),
    schedule_interval='@daily',
    catchup=True,
    default_args={
        'snowflake_conn_id': SNOWFLAKE_CONN_ID,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['project1'],
) as dag:

    create_table = SnowflakeOperator(
        task_id='create_prestage_table',
        sql=CREATE_TABLE_SQL,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    copy_from_s3_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id='copy_from_s3_to_snowflake',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        table=PRESTAGE_TABLE,
        stage=SNOWFLAKE_STAGE,
        file_format="(type='CSV', field_delimiter=',', skip_header=1, null_if=('NULL', 'null', ''), empty_field_as_null=True, field_optionally_enclosed_by='\"')",
        pattern=r'.*ThreeDaysData_Group5_{{ ds }}.*.csv',  
    )


    create_table >> copy_from_s3_to_snowflake
