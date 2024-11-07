from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime 


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='create_dim_fact_tables',
    default_args=default_args,
    schedule_interval='@once',  # Run once on demand
    catchup=False,
) as dag_create:

    # Task to create the dim company table in the BF_DEV schema
    create_dim_company_table = SnowflakeOperator(
        task_id='create_dim_company_table',
        snowflake_conn_id='snowflake_conn',
        sql="""
        create or replace TABLE AIRFLOW1007.BF_DEV.DIM_COMPANY_PROFILE_TEAM1 (
	        ID NUMBER(38,0) NOT NULL,
	        SYMBOL VARCHAR(16),
	        PRICE NUMBER(18,8),
            BETA NUMBER(18,8),
            VOLAVG NUMBER(38,0),
            MKTCAP NUMBER(38,0),
            LASTDIV NUMBER(18,8),
            RANGE VARCHAR(64),
            CHANGES NUMBER(18,8),
            COMPANYNAME VARCHAR(512),
            EXCHANGE VARCHAR(64),
            INDUSTRY VARCHAR(64),
            WEBSITE VARCHAR(64),
            DESCRIPTION VARCHAR(2048),
            CEO VARCHAR(64),
            SECTOR VARCHAR(64),
            DCFDIFF NUMBER(18,8),
            DCF NUMBER(18,8),
            primary key (ID),
	        foreign key (SYMBOL) references AIRFLOW1007.BF_DEV.DIM_SYMBOLS_TEAM1(SYMBOL));
        """
    )
    
    # Task to create the dim symbol table in the BF_DEV schema
    create_dim_symbol_table = SnowflakeOperator(
        task_id='create_dim_symbol_table',
        snowflake_conn_id='snowflake_conn',
        sql="""
        create or replace TABLE AIRFLOW1007.BF_DEV.DIM_SYMBOLS_TEAM1 (
	        SYMBOL VARCHAR(16) NOT NULL,
	        NAME VARCHAR(256),
	        EXCHANGE VARCHAR(64),
	        primary key (SYMBOL));
        """
    )

    # Task to create the fact table in the BF_DEV schema
    create_fact_table = SnowflakeOperator(
        task_id='create_fact_table',
        snowflake_conn_id='snowflake_conn',
        sql="""
        create or replace TABLE AIRFLOW1007.BF_DEV.FACT_STOCK_HISTORY_TEAM1 (
	        SYMBOL VARCHAR(16) NOT NULL,
	        DATE DATE NOT NULL,
            OPEN NUMBER(18,8),
            HIGH NUMBER(18,8),
            LOW NUMBER(18,8),
            CLOSE NUMBER(18,8),
            VOLUME NUMBER(38,8),
            ADJCLOSE NUMBER(18,8),
	        primary key (SYMBOL, DATE),
	        foreign key (SYMBOL) references AIRFLOW1007.BF_DEV.DIM_SYMBOLS_TEAM1(SYMBOL));
        """
    )
    
    # Define the task dependencies
    # Symbol table has to be created before company and fact tables
    # Because these two tables have foreign keys referecing to symbol tables 
    create_dim_symbol_table >> create_dim_company_table >> create_fact_table