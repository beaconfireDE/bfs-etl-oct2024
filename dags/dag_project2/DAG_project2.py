from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'
SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_WAREHOUSE = 'BF_ETL1007'

with DAG( ### Perform three tasks concurrenry
    "Team3_s_to_s_incremental_DAG",
    start_date=datetime(2024, 11, 4),
    schedule_interval='0 1 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    catchup=True,
    tags=['Team3']
) as dag:

    # Task 1: Create Dimension Table
    create_dim_table = SnowflakeOperator(
        task_id="create_dim_Company_Profile",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS AIRFLOW1007.BF_DEV.dim_Company_Profile_Team3 (
                id NUMBER(38, 0) PRIMARY KEY,
                symbol VARCHAR(16) UNIQUE,
                price NUMBER(18, 8),
                beta NUMBER(18, 8),
                lastdiv NUMBER(18, 8),
                range VARCHAR(64),
                companyname VARCHAR(512),
                exchange VARCHAR(64),
                industry VARCHAR(64),
                website VARCHAR(64),
                description VARCHAR(2048),
                ceo VARCHAR(64),
                sector VARCHAR(64)
            );
        """,
    )

    # Task 2: Initially load the data into the dim table. (Not sure why the catch=True doesnt work in this
    # case so we need to initially load it when the table is empty)
    incremental_update_dim = SnowflakeOperator(
        task_id="incremental_update_dim",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            MERGE INTO AIRFLOW1007.BF_DEV.dim_Company_Profile_Team3 AS target
            USING (
                SELECT DISTINCT id, symbol, price, beta, lastdiv, range, companyname, 
                                exchange, industry, website, description, ceo, sector
                FROM US_STOCK_DAILY.DCCM.Company_Profile
            ) AS source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET
                symbol = source.symbol,
                price = source.price,
                beta = source.beta,
                lastdiv = source.lastdiv,
                range = source.range,
                companyname = source.companyname,
                exchange = source.exchange,
                industry = source.industry,
                website = source.website,
                description = source.description,
                ceo = source.ceo,
                sector = source.sector
            WHEN NOT MATCHED THEN INSERT (
                id, symbol, price, beta, lastdiv, range, companyname, 
                exchange, industry, website, description, ceo, sector
            ) VALUES (
                source.id, source.symbol, source.price, source.beta, source.lastdiv, source.range,
                source.companyname, source.exchange, source.industry, source.website,
                source.description, source.ceo, source.sector
            );
        """,
    )

    # Task 3: Create Fact Table
    create_fact_table = SnowflakeOperator(
        task_id="create_fact_Stock_History",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            CREATE IF NOT EXISTS TABLE AIRFLOW1007.BF_DEV.fact_Stock_History_Team3 (
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

    # Task 4: Incremental Update for Fact Table
    incremental_update_fact = SnowflakeOperator(
        task_id="incremental_update_fact",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
                MERGE INTO AIRFLOW1007.BF_DEV.fact_Stock_History_Team3 AS target
                USING (
                    SELECT symbol, date, open AS open_price, high AS high_price, low AS low_price, close AS close_price, volume, adjclose AS adjclose_price
                    FROM (
                        SELECT symbol, date, open, high, low, close, volume, adjclose,
                            ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY symbol, date) AS row_num
                        FROM US_STOCK_DAILY.DCCM.Stock_History
                    ) AS deduped_source
                    WHERE row_num = 1 
                ) AS source
                ON target.date = source.date AND target.symbol = source.symbol
                WHEN MATCHED THEN UPDATE SET
                    target.open_price = source.open_price,
                    target.high_price = source.high_price,
                    target.low_price = source.low_price,
                    target.close_price = source.close_price,
                    target.volume = source.volume,
                    target.adjclose_price = source.adjclose_price
                WHEN NOT MATCHED THEN INSERT (symbol, date, open_price, high_price, low_price, close_price, volume, adjclose_price)
                VALUES (source.symbol, source.date, source.open_price, source.high_price, source.low_price, source.close_price, source.volume, source.adjclose_price);

        """,
    )

    create_time_dim_table = SnowflakeOperator(
        task_id="create_dim_Time",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS AIRFLOW1007.BF_DEV.dim_Time_Team3 (
                date DATE PRIMARY KEY,
                year INT,
                quarter INT,
                month INT,
                day INT,
                day_of_week VARCHAR(10)
            );
        """,
    )

    # Populate Time Dimension Table
    populate_time_dim_table = SnowflakeOperator(
        task_id="populate_dim_Time",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            INSERT INTO AIRFLOW1007.BF_DEV.dim_Time_Team3
            SELECT
                date,
                YEAR(date) AS year,
                QUARTER(date) AS quarter,
                MONTH(date) AS month,
                DAY(date) AS day,
                DAYNAME(date) AS day_of_week
            FROM (
                SELECT DISTINCT date
                FROM US_STOCK_DAILY.DCCM.Stock_History
            );
        """,
    )

    ### dependencies in order
    create_time_dim_table >> populate_time_dim_table >> create_dim_table >> incremental_update_dim >> create_fact_table >> incremental_update_fact
