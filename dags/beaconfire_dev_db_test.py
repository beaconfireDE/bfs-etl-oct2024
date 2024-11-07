import os
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Snowflake connection and configuration
SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_WAREHOUSE = 'BF_ETL1007'

# Source and Target Tables
SNOWFLAKE_SCHEMA_SOURCE = 'DCCM'
SNOWFLAKE_DATABASE_SOURCE = 'US_STOCK_DAILY'
SNOWFLAKE_TABLE_SH_SOURCE = 'STOCK_HISTORY'
SNOWFLAKE_TABLE_CP_SOURCE = 'COMPANY_PROFILE'
SNOWFLAKE_TABLE_SYM_SOURCE = 'SYMBOLS'

SNOWFLAKE_DATABASE_TARGET = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA_TARGET = 'BF_DEV'
SNOWFLAKE_TABLE_SH_TARGET = 'fact_Stock_History_TEAM4'
SNOWFLAKE_TABLE_CP_TARGET = 'dim_COMPANY_PROFILE_TEAM4'
SNOWFLAKE_TABLE_SYM_TARGET = 'dim_SYMBOLS_TEAM4'

# SQL MERGE Statements for upserts (insert and update)
SQL_MERGE_STATEMENT_SH = f"""
MERGE INTO {SNOWFLAKE_DATABASE_TARGET}.{SNOWFLAKE_SCHEMA_TARGET}.{SNOWFLAKE_TABLE_SH_TARGET} AS target
    USING (
        SELECT SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE FROM
        (
        SELECT SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE, row_number() OVER (PARTITION BY SYMBOL, DATE ORDER BY DATE) AS row_num 
        FROM {SNOWFLAKE_DATABASE_SOURCE}.{SNOWFLAKE_SCHEMA_SOURCE}.{SNOWFLAKE_TABLE_SH_SOURCE}
        ) 
        AS deduped_source WHERE row_num = 1
        ) AS source
    ON target.SYMBOL = source.SYMBOL AND target.DATE = source.DATE
    WHEN MATCHED THEN
        UPDATE SET
            target.OPEN = source.OPEN,
            target.HIGH = source.HIGH,
            target.LOW = source.LOW,
            target.CLOSE = source.CLOSE,
            target.VOLUME = source.VOLUME,
            target.ADJCLOSE = source.ADJCLOSE
    WHEN NOT MATCHED THEN
        INSERT (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE)
        VALUES (source.SYMBOL, source.DATE, source.OPEN, source.HIGH, source.LOW, source.CLOSE, source.VOLUME, source.ADJCLOSE)
        """

SQL_MERGE_STATEMENT_CP = f"""
    MERGE INTO {SNOWFLAKE_DATABASE_TARGET}.{SNOWFLAKE_SCHEMA_TARGET}.{SNOWFLAKE_TABLE_CP_TARGET} AS target
    USING (
        SELECT ID, SYMBOL, PRICE, BETA, MKTCAP, INDUSTRY, SECTOR
        FROM {SNOWFLAKE_DATABASE_SOURCE}.{SNOWFLAKE_SCHEMA_SOURCE}.{SNOWFLAKE_TABLE_CP_SOURCE}
    ) AS source
    ON target.ID = source.ID
    WHEN MATCHED THEN
        UPDATE SET
            target.SYMBOL = source.SYMBOL,
            target.PRICE = source.PRICE,
            target.BETA = source.BETA,
            target.MKTCAP = source.MKTCAP,
            target.INDUSTRY = source.INDUSTRY,
            target.SECTOR = source.SECTOR
    WHEN NOT MATCHED THEN
        INSERT (ID, SYMBOL, PRICE, BETA, MKTCAP, INDUSTRY, SECTOR)
        VALUES (source.ID, source.SYMBOL, source.PRICE, source.BETA, source.MKTCAP, source.INDUSTRY, source.SECTOR);
"""

SQL_MERGE_STATEMENT_SYM = f"""
    MERGE INTO {SNOWFLAKE_DATABASE_TARGET}.{SNOWFLAKE_SCHEMA_TARGET}.{SNOWFLAKE_TABLE_SYM_TARGET} AS target
    USING (
        SELECT SYMBOL, NAME, EXCHANGE
        FROM {SNOWFLAKE_DATABASE_SOURCE}.{SNOWFLAKE_SCHEMA_SOURCE}.{SNOWFLAKE_TABLE_SYM_SOURCE}
    ) AS source
    ON target.SYMBOL = source.SYMBOL
    WHEN MATCHED THEN
        UPDATE SET
            target.NAME = source.NAME,
            target.EXCHANGE = source.EXCHANGE
    WHEN NOT MATCHED THEN
        INSERT (SYMBOL, NAME, EXCHANGE)
        VALUES (source.SYMBOL, source.NAME, source.EXCHANGE);
"""

# DAG Definition
DAG_ID = "beaconfire_dev_db_test"

with DAG(
    DAG_ID,
    start_date=datetime(2024, 11, 7),
    schedule_interval='@daily',  # daily update
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=False,
) as dag:
    
    # MERGE Tasks for upserts
    snowflake_merge_sh_params = SnowflakeOperator(
        task_id='snowflake_merge_sh_params',
        sql=SQL_MERGE_STATEMENT_SH,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_merge_cp_params = SnowflakeOperator(
        task_id='snowflake_merge_cp_params',
        sql=SQL_MERGE_STATEMENT_CP,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_merge_sym_params = SnowflakeOperator(
        task_id='snowflake_merge_sym_params',
        sql=SQL_MERGE_STATEMENT_SYM,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )

    # Defining task order if sequential execution is required
    [snowflake_merge_sh_params, snowflake_merge_cp_params, snowflake_merge_sym_params]
