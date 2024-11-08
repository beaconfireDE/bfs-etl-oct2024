from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from datetime import datetime

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'
SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER'
SOURCE_DATABASE = 'US_STOCK_DAILY'
SOURCE_SCHEMA = 'DCCM'
target_dict = {
    "company":"dim_company_team2",
    "exchange":"dim_exchange_team2",
    "industry":"dim_industry_team2",
    "sector":"dim_sector_team2",
    "date":"dim_date_team2",
    "stock_history":"update_dim_exchange_table"
}
source_dict = {
    "company":"company_profile",
    "symbol":"symbols",
    "stock_history":"stock_history"
}

filename = 'AQ_Team2_' + '{{ ds_nodash }}' + '.csv'

with DAG(
    's3_to_snowflake_team2_test',
    start_date=datetime(2022, 11, 1),
    end_date=datetime(2022, 11, 5),
    schedule_interval='55 23 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire', 'team2'],
    catchup=True,
) as dag:
    create_table_dim_company = SnowflakeOperator(
        task_id='create_dim_company_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=
        f'''
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["company"]} (
                COMPANY_ID NUMBER(38,0) NOT NULL PRIMARY KEY,
                SYMBOL VARCHAR(16),
                COMPANYNAME VARCHAR(512),
                CEO VARCHAR(64),
                DESCRIPTION VARCHAR(2048),
                WEBSITE VARCHAR(64)
            );
        '''
    )

    create_table_dim_symbol = SnowflakeOperator(
        task_id='create_dim_exchange_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=
        f'''
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["exchange"]} (
                EXCHANGE_ID INT IDENTITY NOT NULL PRIMARY KEY,
                EXCHANGE VARCHAR(64)
            );
        '''
    )

    create_table_dim_industry = SnowflakeOperator(
        task_id='create_dim_industry_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=
        f'''
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["industry"]} (
                INDUSTRY_ID INT IDENTITY NOT NULL PRIMARY KEY,
                INDUSTRY VARCHAR(64)
            );
        '''
    )

    create_table_dim_sector = SnowflakeOperator(
        task_id='create_dim_sector_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=
        f'''
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["sector"]} (
                SECTOR_ID INT IDENTITY NOT NULL PRIMARY KEY,
                SECTOR VARCHAR(64)
            );
        '''
    )

    create_table_dim_date = SnowflakeOperator(
        task_id='create_dim_date_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=
        f'''
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["date"]} (
                DATE_KEY NUMBER(38,0) NOT NULL PRIMARY KEY,
                DATE DATE,
                YEAR NUMBER(38,0),
                QUARTER NUMBER(38,0),
                MONTH NUMBER(38,0),
                DAY NUMBER(38,0),
                WEEKDAY NUMBER(38,0)
            );
        '''
    )

    create_table_fact_history = SnowflakeOperator(
        task_id='create_fact_history_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=
        f'''
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["stock_history"]} (
                FACT_ID NUMBER(38,0) IDENTITY NOT NULL PRIMARY KEY,
                COMPANY_ID INT,
                EXCHANGE_ID INT,
                INDUSTRY_ID INT,
                SECTOR_ID INT,
                DATE_KEY INT,
                OPEN NUMBER(18,8),
                HIGH NUMBER(18,8),
                LOW NUMBER(18,8),
                CLOSE NUMBER(18,8),
                VOLUME NUMBER(38,8),
                ADJCLOSE NUMBER(18,8)
            );
        '''
    )

    update_table_dim_company = SnowflakeOperator(
        task_id='update_dim_company_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=
        f'''
            MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["company"]} AS t1
            USING (
                    SELECT
                        id,
                        symbol,
                        companyname,
                        ceo,
                        description,
                        website
                    FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{source_dict["company"]}
                    EXCEPT
                    SELECT
                        company_id,
                        symbol,
                        companyname,
                        ceo,
                        description,
                        website
                    FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["company"]}
            ) AS t2
            ON t1.company_id = t2.id
            WHEN MATCHED THEN
                UPDATE SET t1.symbol = t2.symbol,
                    t1.companyname = t2.companyname,
                    t1.ceo = t2.ceo,
                    t1.description = t2.description,
                    t1.website = t2.website
            WHEN NOT MATCHED THEN
                INSERT(company_id, symbol, companyname, ceo, description, website)
                VALUES(t2.id, t2.symbol, t2.companyname, t2.ceo, t2.description, t2.website);
        '''
    )

    update_table_dim_symbol = SnowflakeOperator(
        task_id='update_dim_symbol_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=
        f'''
            MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["exchange"]} AS t1
            USING (
                SELECT distinct exchange
                FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{source_dict["symbol"]}
                EXCEPT
                SELECT exchange
                FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["exchange"]}
            ) AS t2
            ON t1.exchange = t2.exchange

            WHEN NOT MATCHED THEN
                INSERT(exchange)
                VALUES(t2.exchange);
        '''
    )

    update_table_dim_industry = SnowflakeOperator(
        task_id='update_dim_industry_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=
        f'''
            MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["industry"]} AS t1
            USING (
                SELECT distinct sector
                FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{source_dict["company"]}
                EXCEPT
                SELECT distinct sector
                FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["industry"]}
            ) AS t2
            ON t1.sector = t2.sector
        '''
    )

    update_table_dim_sector = SnowflakeOperator(
        task_id='update_dim_sector_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=
        f'''
            MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["sector"]} AS t1
            USING (
                SELECT distinct sector
                FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{source_dict["company"]}
                EXCEPT
                SELECT distinct sector
                FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["sector"]}
            ) AS t2
            ON t1.sector = t2.sector

            WHEN NOT MATCHED THEN
                INSERT(sector)
                VALUES(t2.sector);
        '''
    )

    update_table_dim_date = SnowflakeOperator(
        task_id='update_dim_date_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=
        f'''
            MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["date"]} AS t1
            USING (
                SELECT TO_DATE('{{ ds_nodash }}','YYYYMMDD') as date
            ) AS t2
            ON t1.date_key = {{ ds_nodash }}
            WHEN NOT MATCHED THEN
                INSERT(date_key, date, year, quarter, month, day, weekday)
                VALUES({{ ds_nodash }}, t2.date,YEAR(t2.date),QUARTER(t2.date),MONTH(t2.date),DAY(t2.date),DAYOFWEEK(t2.date));
                
        '''
    )

    update_table_fact_history = SnowflakeOperator(
        task_id='update_fact_history_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=
        f'''
            INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.fact_stock_history_team2(
                company_id, 
                exchange_id,
                industry_id,
                sector_id,
                date_key,
                open,
                high,
                low,
                close,
                volume,
                adjclose)
            SELECT s25.company_id, s5.exchange_id, s3.industry_id, s4.sector_id, 20200619, s1.open, s1.high, s1.low, s1.close, s1.volume, s1.adjclose
            FROM (
                SELECT * 
                FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{source_dict["stock_history"]}
                WHERE date = TO_DATE('{{ ds_nodash }}', 'YYYYMMDD')
            ) s1
                JOIN {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{source_dict["company"]} s2 ON s1.symbol = s2.symbol
                JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["company"]} s25 ON s1.symbol = s25.symbol
                JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["industry"]} s3 ON s2.industry = s3.industry
                JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["sector"]} s4 ON s2.sector = s4.sector
                JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_dict["exchange"]} s5 ON s2.exchange = s5.exchange;
        '''
    )

    clean_fact_history = SnowflakeOperator(
        task_id='clean_repeat_fact_history',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=
        f'''
            DELETE FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.fact_stock_history_team2
            WHERE date_key={{ ds_nodash }}
            '''
    )

    (
        [
            create_table_dim_company >> update_table_dim_company,
            create_table_dim_date >> update_table_dim_date,
            create_table_dim_industry >> update_table_dim_industry,
            create_table_dim_sector >> update_table_dim_sector,
            create_table_dim_symbol >> update_table_dim_symbol,
            create_table_fact_history
        ] >> clean_fact_history >> update_table_fact_history
        
    )

