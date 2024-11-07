from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

# Snowflake
SNOWFLAKE_CONN_ID = 'snowflake_conn'
TARGET_SNOWFLAKE_DATABASE = 'AIRFLOW1007'
TARGET_SNOWFLAKE_SCHEMA = 'BF_DEV'

SOURCE_SNOWFLAKE_DATABASE = 'US_STOCK_DAILY'
SOURCE_SNOWFLAKE_SCHEMA = 'DCCM'

SNOWFLAKE_TABLE_DIM_STATIC = 'DIM_STOCK_STATIC_GROUP5'
SNOWFLAKE_TABLE_DIM_PROFILE = 'DIM_SCD_COMPANY_PROFILE_GROUP5'
SNOWFLAKE_TABLE_FACT_STOCK = 'FACT_STOCK_HISTORY_GROUP5'

CREATE_TABLE_DIM_STOCK_STATIC = f"""
CREATE TABLE IF NOT EXISTS {TARGET_SNOWFLAKE_DATABASE}.{TARGET_SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE_DIM_STATIC} (
    SYMBOL VARCHAR PRIMARY KEY,
    EXCHANGE VARCHAR
);
"""

CREATE_TABLE_SCD_COMPANY_PROFILE = f"""
CREATE TABLE IF NOT EXISTS {TARGET_SNOWFLAKE_DATABASE}.{TARGET_SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE_DIM_PROFILE} (
    SCD_ID NUMBER PRIMARY KEY,
    SYMBOL VARCHAR,
    NAME VARCHAR,
    COMPANYNAME VARCHAR,
    WEBSITE VARCHAR,
    PRICE FLOAT,
    BETA FLOAT,
    VOLAVG INT,
    MKTCAP FLOAT,
    LASTDIV FLOAT,
    RANGE VARCHAR,
    CHANGES FLOAT,
    CEO VARCHAR,
    INDUSTRY VARCHAR,
    SECTOR VARCHAR,
    DESCRIPTION VARCHAR,
    DCFDIFF FLOAT,
    DCF FLOAT,
    Start_Date DATE,
    End_Date DATE
);
"""

CREATE_TABLE_FACT_STOCK_HISTORY = f"""
CREATE TABLE IF NOT EXISTS {TARGET_SNOWFLAKE_DATABASE}.{TARGET_SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE_FACT_STOCK} (
    SYMBOL VARCHAR,
    DATE DATE,
    OPEN FLOAT,
    HIGH FLOAT,
    LOW FLOAT,
    CLOSE FLOAT,
    VOLUME INT,
    ADJCLOSE FLOAT
);
"""

with DAG(
    'Project2Stock-Group5',
    start_date=datetime(2024, 11, 6),
    schedule_interval='0 15 * * *',  
    default_args={
        'snowflake_conn_id': SNOWFLAKE_CONN_ID,
    },
    tags=['group5'],
) as dag:
    
    create_table_dim_static = SnowflakeOperator(
        task_id='create_table_dim_static',
        sql=CREATE_TABLE_DIM_STOCK_STATIC,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )
    
    create_table_dim_profile = SnowflakeOperator(
        task_id='create_table_dim_profile',
        sql=CREATE_TABLE_SCD_COMPANY_PROFILE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )
    
    create_table_fact_stock = SnowflakeOperator(
        task_id='create_table_fact_stock',
        sql=CREATE_TABLE_FACT_STOCK_HISTORY,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    update_dim_stock_static = SnowflakeOperator(
        task_id='update_dim_stock_static',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE_DIM_STATIC} AS target
        USING (SELECT SYMBOL, EXCHANGE FROM {SOURCE_SNOWFLAKE_DATABASE}.{SOURCE_SNOWFLAKE_SCHEMA}.{SOURCE_TABLE_SYMBOLS}) AS source
        ON target.SYMBOL = source.SYMBOL
        WHEN MATCHED THEN
            UPDATE SET target.EXCHANGE = source.EXCHANGE
        WHEN NOT MATCHED THEN
            INSERT (SYMBOL, EXCHANGE)
            VALUES (source.SYMBOL, source.EXCHANGE);
        """
    )
