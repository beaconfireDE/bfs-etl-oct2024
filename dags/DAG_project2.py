from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'
SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_WAREHOUSE = 'BF_ETL1007'

with DAG( ### Perform three tasks concurrenry
    "Team3_s_to_s_incremental_update",
    start_date=datetime(2024, 11, 6),
    schedule_interval='@daily',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    catchup=False,
    tags=['Team3']
) as dag:

    # Task 1: Create Dimension Table
    create_dim_table = SnowflakeOperator(
        task_id="create_dim_Company_Profile",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            CREATE OR REPLACE TABLE AIRFLOW1007.BF_DEV.dim_Company_Profile_Team3 (
                id NUMBER(38, 0) PRIMARY KEY,
                symbol VARCHAR(16),
                price NUMBER(18, 8),
                beta NUMBER(18, 8),
                volavg NUMBER(38, 0),
                mktcap NUMBER(38, 0),
                lastdiv NUMBER(18, 8),
                range VARCHAR(64),
                changes NUMBER(18, 8),
                companyname VARCHAR(512),
                exchange VARCHAR(64),
                industry VARCHAR(64),
                website VARCHAR(64),
                description VARCHAR(2048),
                ceo VARCHAR(64),
                sector VARCHAR(64),
                dcf_diff NUMBER(18, 8),
                dcf NUMBER(18, 8)
            );
        """,
    )

    # Task 2: Create Fact Table
    create_fact_table = SnowflakeOperator(
        task_id="create_fact_Stock_History",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            CREATE OR REPLACE TABLE AIRFLOW1007.BF_DEV.fact_Stock_History_Team3 (
                symbol VARCHAR(16),
                date DATE,
                open_price NUMBER(18, 8),
                high_price NUMBER(18, 8),
                low_price NUMBER(18, 8),
                close_price NUMBER(18, 8),
                volume NUMBER(38, 8),
                adjclose_price NUMBER(18, 8),
                PRIMARY KEY (symbol, date),
                FOREIGN KEY (symbol) REFERENCES AIRFLOW1007.BF_DEV.dim_Company_Profile_Team3(symbol)
            );
        """,
    )

    # Task 3: Incremental Update for Fact Table
    incremental_update_fact = SnowflakeOperator(
        task_id="incremental_update_fact",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            MERGE INTO AIRFLOW1007.BF_DEV.fact_Stock_History_Team3 AS target
            USING US_STOCK_DAILY.DCCM.Stock_History AS source
            ON target.date = source.date AND target.symbol = source.symbol
            WHEN MATCHED THEN UPDATE SET
                target.open_price = source.open,
                target.high_price = source.high,
                target.low_price = source.low,
                target.close_price = source.close,
                target.volume = source.volume,
                target.adjclose_price = source.adjclose
            WHEN NOT MATCHED THEN INSERT (symbol, date, open_price, high_price, low_price, close_price, volume, adjclose_price)
            VALUES (source.symbol, source.date, source.open, source.high, source.low, source.close, source.volume, source.adjclose);
        """,
    )

    ### dependencies in order
    create_dim_table >> create_fact_table >> incremental_update_fact
