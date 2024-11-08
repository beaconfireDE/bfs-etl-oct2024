"""
Project_2_Part_3
"""
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


SNOWFLAKE_CONN_ID = 'snowflake_conn'

SNOWFLAKE_SOURCE_DATABASE = 'US_STOCK_DAIlY'
SNOWFLAKE_SOURCE_SCHEMA = 'DCCM'
SNOWFLAKE_SOURCE_TABLE = 'STOCK_HISTORY'

SNOWFLAKE_TARGET_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_TARGET_SCHEMA = 'BF_DEV'
SNOWFLAKE_TARGET_TABLE = 'fact_stock_history_team1'

DAG_ID = "project2_snowflake_to_snowflake_team1"

# SQL query command for Incremental Loading
INCREAMENTAL_LOAD = f"""
    MERGE INTO {SNOWFLAKE_TARGET_DATABASE}.{SNOWFLAKE_TARGET_SCHEMA}.{SNOWFLAKE_TARGET_TABLE} AS tgt
    USING (
        SELECT SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY SYMBOL, DATE ORDER BY DATE) AS row_rnk
            FROM {SNOWFLAKE_SOURCE_DATABASE}.{SNOWFLAKE_SOURCE_SCHEMA}.{SNOWFLAKE_SOURCE_TABLE}
            ) AS src_no_dup
        WHERE row_rnk = 1
        ) AS src
    ON tgt.date = src.date AND tgt.symbol = src.symbol
    WHEN MATCHED THEN UPDATE SET
        tgt.open = src.open,
        tgt.high = src.high,
        tgt.low = src.low,
        tgt.close = src.close,
        tgt.volume = src.volume,
        tgt.adjclose = src.adjclose
    WHEN NOT MATCHED THEN INSERT (symbol, date, open, high, low, close, volume, adjclose)
    VALUES (src.symbol, src.date, src.open, src.high, src.low, src.close, src.volume, src.adjclose);
"""

# DAG operation starting
with DAG(
    DAG_ID,
    start_date=datetime(2024, 11, 6),
    schedule_interval='0 0 * * *', # Everyday at 12:00am
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['team1_project2'],
    catchup=True,
) as dag:
    
    incremental_load = SnowflakeOperator(
        task_id='incremental_load_stock_history',
        sql= INCREAMENTAL_LOAD,
        snowflake_conn_id= SNOWFLAKE_CONN_ID,
    )

incremental_load