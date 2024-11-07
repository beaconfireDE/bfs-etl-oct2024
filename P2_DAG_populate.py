from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime 


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 7),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='create_and_populate_dim_fact_tables',
    default_args=default_args,
    schedule_interval='@daily',  # Run daily
    catchup=False,
) as dag_populate:
    
    # Task to populate the dim company profile table with data from the source table
    populate_dim_company_table = SnowflakeOperator(
        task_id='populate_dim_company_table',
        snowflake_conn_id='snowflake_conn',
        sql="""
        INSERT INTO AIRFLOW1007.BF_DEV.DIM_COMPANY_PROFILE_TEAM1 (ID, SYMBOL, PRICE, BETA, VOLAVG, MKTCAP, LASTDIV, RANGE, CHANGES, COMPANYNAME, EXCHANGE, INDUSTRY, WEBSITE, DESCRIPTION, CEO, SECTOR, DCFDIFF, DCF)
        SELECT
            ID,
            SYMBOL,
            PRICE,
            BETA,
            VOLAVG,
            MKTCAP,
            LASTDIV,
            RANGE,
            CHANGES,
            COMPANYNAME,
            EXCHANGE,
            INDUSTRY,
            WEBSITE,
            DESCRIPTION,
            CEO,
            SECTOR,
            DCFDIFF,
            DCF
        FROM US_STOCK_DAILY.DCCM.Company_Profile
        WHERE SYMBOL NOT IN (SELECT SYMBOL FROM AIRFLOW1007.BF_DEV.DIM_COMPANY_PROFILE_TEAM1);
        """
    )
    
    # Task to populate the dim symbol table with data from the source table
    populate_dim_symbol_table = SnowflakeOperator(
        task_id='populate_dim_symbol_table',
        snowflake_conn_id='snowflake_conn',
        sql="""
        INSERT INTO AIRFLOW1007.BF_DEV.DIM_SYMBOLS_TEAM1 (SYMBOL, NAME, EXCHANGE)
        SELECT
            SYMBOL,
            NAME,
            EXCHANGE
        FROM US_STOCK_DAILY.DCCM.Symbols
        WHERE SYMBOL NOT IN (SELECT SYMBOL FROM AIRFLOW1007.BF_DEV.DIM_SYMBOLS_TEAM1);
        """
    )

    # Task to populate the fact table with data from the source table
    populate_fact_table = SnowflakeOperator(
        task_id='populate_fact_table',
        snowflake_conn_id='snowflake_conn',
        sql="""
        INSERT INTO AIRFLOW1007.BF_DEV.FACT_STOCK_HISTORY_TEAM1 (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE)
        SELECT
            sh.SYMBOL,
            sh.DATE,
            sh.OPEN,
            sh.HIGH,
            sh.LOW,
            sh.CLOSE,
            sh.VOLUME,
            sh.ADJCLOSE
        FROM US_STOCK_DAILY.DCCM.Stock_History sh
        WHERE (sh.DATE, sh.SYMBOL) NOT IN (SELECT DATE, SYMBOL FROM AIRFLOW1007.BF_DEV.FACT_STOCK_HISTORY_TEAM1);
        """
    )
    
    # Define the task dependencies
    populate_dim_symbol_table >> populate_dim_company_table >> populate_fact_table
