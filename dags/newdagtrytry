from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

# Snowflake
SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'
SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER'
SNOWFLAKE_WAREHOUSE = 'aw_etl'
PRESTAGE_TABLE = 'PRESTAGE_trytry_GROUP5'

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{PRESTAGE_TABLE} (
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
    'dagtrytry',
    start_date=datetime(2024, 11, 6),
    schedule_interval=None,  # 设置为 None 表示只运行一次
    default_args={
        'snowflake_conn_id': SNOWFLAKE_CONN_ID,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['group5'],
) as dag:

    # Task 1: 
    create_table_if_not_exists = SnowflakeOperator(
        task_id='create_table_if_not_exists',
        sql=CREATE_TABLE_SQL,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # Task 2: 
    copy_into_stage = CopyFromExternalStageToSnowflakeOperator(
        task_id='stage_group5_data',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        table=PRESTAGE_TABLE,
        stage=SNOWFLAKE_STAGE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        file_format="(type='CSV', field_delimiter=',', skip_header=1, null_if=('NULL', 'null', ''), empty_field_as_null=True, field_optionally_enclosed_by='\"')",
        pattern=r'.*ThreeDaysData_Group5_.*\.csv',  # 使用正则表达式匹配所有符合条件的文件
    )

    create_table_if_not_exists >> copy_into_stage
