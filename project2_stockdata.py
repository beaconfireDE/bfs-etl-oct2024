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
SOURCE_TABLE_PROFILE = 'COMPANY_PROFILE'
SOURCE_TABLE_STOCK = 'STOCK_HISTORY'
SOURCE_TABLE_SYMBOLS = "SYMBOLS"

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
        MERGE INTO {TARGET_SNOWFLAKE_DATABASE}.{TARGET_SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE_DIM_STATIC} AS target
        USING (SELECT SYMBOL, EXCHANGE FROM {SOURCE_SNOWFLAKE_DATABASE}.{SOURCE_SNOWFLAKE_SCHEMA}.{SOURCE_TABLE_SYMBOLS}) AS source
        ON target.SYMBOL = source.SYMBOL
        WHEN MATCHED THEN
            UPDATE SET target.EXCHANGE = source.EXCHANGE
        WHEN NOT MATCHED THEN
            INSERT (SYMBOL, EXCHANGE)
            VALUES (source.SYMBOL, source.EXCHANGE);
        """
    )
    
    update_fact_stock = SnowflakeOperator(
        task_id = 'update_fact_stock',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql = f"""
        MERGE INTO {TARGET_SNOWFLAKE_DATABASE}.{TARGET_SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE_FACT_STOCK} AS target
        USING {SOURCE_SNOWFLAKE_DATABASE}.{SOURCE_SNOWFLAKE_SCHEMA}.{SOURCE_TABLE_STOCK} AS source
        ON target.SYMBOL = source.SYMBOL AND target.DATE = source.DATE
        WHEN MATCHED THEN UPDATE SET
            TARGET.OPEN = SOURCE.OPEN,
            TARGET.HIGH = SOURCE.HIGH,
            TARGET.LOW = SOURCE.LOW,
            TARGET.CLOSE = SOURCE.CLOSE,
            TARGET.VOLUME = SOURCE.VOLUME,
            TARGET.ADJCLOSE = SOURCE.ADJCLOSE
        WHEN NOT MATCHED THEN 
            INSERT (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE) 
            VALUES (SOURCE.SYMBOL, SOURCE.DATE, SOURCE.OPEN, SOURCE.HIGH, SOURCE.LOW, SOURCE.CLOSE, SOURCE.VOLUME, SOURCE.ADJCLOSE);
        """
    )
    
    update_scd_company_profile = SnowflakeOperator(
    task_id='update_scd_company_profile',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""

    MERGE INTO {TARGET_SNOWFLAKE_DATABASE}.{TARGET_SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE_DIM_PROFILE} AS target
    USING (
        SELECT 
            cp.*, 
            s.NAME, 
            CURRENT_DATE AS Start_Date, 
            NULL AS End_Date
        FROM 
            {SOURCE_SNOWFLAKE_DATABASE}.{SOURCE_SNOWFLAKE_SCHEMA}.{SOURCE_TABLE_PROFILE} AS cp
        JOIN 
            {SOURCE_SNOWFLAKE_DATABASE}.{SOURCE_SNOWFLAKE_SCHEMA}.{SOURCE_TABLE_SYMBOLS} AS s
        ON 
            cp.SYMBOL = s.SYMBOL
    ) AS source
    ON target.SYMBOL = source.SYMBOL
    AND target.End_Date IS NULL
    AND (
        target.NAME <> source.NAME OR
        target.COMPANYNAME <> source.COMPANYNAME OR
        target.WEBSITE <> source.WEBSITE OR
        target.PRICE <> source.PRICE OR
        target.BETA <> source.BETA OR
        target.VOLAVG <> source.VOLAVG OR
        target.MKTCAP <> source.MKTCAP OR
        target.LASTDIV <> source.LASTDIV OR
        target.RANGE <> source.RANGE OR
        target.CHANGES <> source.CHANGES OR
        target.CEO <> source.CEO OR
        target.INDUSTRY <> source.INDUSTRY OR
        target.SECTOR <> source.SECTOR OR
        target.DESCRIPTION <> source.DESCRIPTION OR
        target.DCFDIFF <> source.DCFDIFF OR
        target.DCF <> source.DCF
    )
    WHEN MATCHED THEN
        UPDATE SET target.End_Date = CURRENT_DATE;

    INSERT INTO {TARGET_SNOWFLAKE_DATABASE}.{TARGET_SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE_DIM_PROFILE} (
        SCD_ID, SYMBOL, NAME, COMPANYNAME, WEBSITE, PRICE, BETA, VOLAVG, MKTCAP, 
        LASTDIV, RANGE, CHANGES, CEO, INDUSTRY, SECTOR, DESCRIPTION, 
        DCFDIFF, DCF, Start_Date, End_Date
    )
    SELECT
        source.ID,                   
        source.SYMBOL,
        source.NAME,
        source.COMPANYNAME,
        source.WEBSITE,
        source.PRICE,
        source.BETA,
        source.VOLAVG,
        source.MKTCAP,
        source.LASTDIV,
        source.RANGE,
        source.CHANGES,
        source.CEO,
        source.INDUSTRY,
        source.SECTOR,
        source.DESCRIPTION,
        source.DCFDIFF,
        source.DCF,
        CURRENT_DATE AS Start_Date,  
        NULL AS End_Date            
    FROM (
        SELECT 
            cp.*, 
            s.NAME, 
            CURRENT_DATE AS Start_Date, 
            NULL AS End_Date
        FROM 
            {SOURCE_SNOWFLAKE_DATABASE}.{SOURCE_SNOWFLAKE_SCHEMA}.{SOURCE_TABLE_PROFILE} AS cp
        JOIN 
            {SOURCE_SNOWFLAKE_DATABASE}.{SOURCE_SNOWFLAKE_SCHEMA}.{SOURCE_TABLE_SYMBOLS} AS s
        ON 
            cp.SYMBOL = s.SYMBOL
    ) AS source
    LEFT JOIN {TARGET_SNOWFLAKE_DATABASE}.{TARGET_SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE_DIM_PROFILE} AS target
    ON target.SYMBOL = source.SYMBOL
    AND target.End_Date IS NULL
    WHERE target.SYMBOL IS NULL       
    """
)
    create_table_dim_static >> update_dim_stock_static
    create_table_dim_profile >> update_scd_company_profile
    create_table_fact_stock >> update_fact_stock