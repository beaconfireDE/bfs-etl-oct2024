from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

# Snowflake
SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'
SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER'
SNOWFLAKE_WAREHOUSE = 'BF_ETL1007'
PRESTAGE_TABLE = 'PRESTAGE_3DaysData_GROUP5'

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
    '3DaysData-Group5',
    start_date=datetime(2024, 11, 6),
    schedule_interval=None,  
    default_args={
        'snowflake_conn_id': SNOWFLAKE_CONN_ID,
    },
    tags=['group5'],
) as dag:

    create_table_if_not_exists = SnowflakeOperator(
        task_id='create_table_if_not_exists',
        sql=CREATE_TABLE_SQL,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    copy_into_stage = CopyFromExternalStageToSnowflakeOperator(
        task_id='stage_group5_data',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        table=PRESTAGE_TABLE,
        stage=SNOWFLAKE_STAGE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        file_format="(type='CSV', field_delimiter=',', skip_header=1, null_if=('NULL', 'null', ''), empty_field_as_null=True, field_optionally_enclosed_by='\"')",
        pattern=r'.*ThreeDaysData_Group5_.*\.csv',  
    )

    create_table_if_not_exists >> copy_into_stage
