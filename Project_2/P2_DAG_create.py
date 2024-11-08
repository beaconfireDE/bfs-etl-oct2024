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
    dag_id='create_dim_fact_tables',
    default_args=default_args,
    schedule_interval='@once',  # Run once on demand
    catchup=False,
) as dag_create:
    

    # Task to create the dim symbol table in the BF_DEV schema
    create_dim_symbol_table = SnowflakeOperator(
        task_id='create_dim_symbol_table',
        snowflake_conn_id='snowflake_conn',
        sql="""
        CREATE OR REPLACE TABLE AIRFLOW1007.BF_DEV.dim_SYMBOLS_Team1 (
            SYMBOL VARCHAR(16) PRIMARY KEY, 
            NAME VARCHAR(256),
            EXCHANGE VARCHAR(64));
        """
    )
    
    # Task to create the company profile table in the BF_DEV schema
    create_dim_company_profile_table = SnowflakeOperator(
        task_id='create_dim_company_table',
        snowflake_conn_id='snowflake_conn',
        sql="""
        CREATE OR REPLACE TABLE AIRFLOW1007.BF_DEV.dim_Company_Profile_Team1 (
            SYMBOL VARCHAR(16) PRIMARY KEY,  
            COMPANYNAME VARCHAR(512),
            SECTOR VARCHAR(64),
            INDUSTRY VARCHAR(64),
            WEBSITE VARCHAR(64),
            FOREIGN KEY (SYMBOL) REFERENCES AIRFLOW1007.BF_DEV.dim_SYMBOLS_Team1(SYMBOL));
        """
    )

    # Task to create the company financials table in the BF_DEV schema
    create_dim_company_financials_table = SnowflakeOperator(
        task_id='create_dim_symbol_table',
        snowflake_conn_id='snowflake_conn',
        sql="""
        CREATE OR REPLACE TABLE AIRFLOW1007.BF_DEV.dim_Company_Financials_Team1 (
            ID NUMBER(38,0) PRIMARY KEY,
            SYMBOL VARCHAR(16),
            PRICE NUMBER(18,8),
            BETA NUMBER(18,8),
            VOLAVG NUMBER(38,0),
            MKTCAP NUMBER(38,0),
            LASTDIV NUMBER(18,8),
            DCFDIFF NUMBER(18,8),
            DCF NUMBER(18,8),
            FOREIGN KEY (SYMBOL) REFERENCES AIRFLOW1007.BF_DEV.dim_SYMBOLS_Team1(SYMBOL));
        """
    )


    # Task to create the fact table in the BF_DEV schema
    create_fact_table = SnowflakeOperator(
        task_id='create_fact_table',
        snowflake_conn_id='snowflake_conn',
        sql="""
        CREATE OR REPLACE TABLE AIRFLOW1007.BF_DEV.fact_Stock_History_Team1 (
            SYMBOL VARCHAR(16),        
            DATE DATE,                 
            OPEN NUMBER(18,8),
            HIGH NUMBER(18,8),
            LOW NUMBER(18,8),
            CLOSE NUMBER(18,8),
            VOLUME NUMBER(38,0),
            ADJCLOSE NUMBER(18,8),
            PRIMARY KEY (SYMBOL, DATE),
            FOREIGN KEY (SYMBOL) REFERENCES AIRFLOW1007.BF_DEV.dim_SYMBOLS_Team1(SYMBOL));
        """
    )
    
    # Define the task dependencies
    # Symbol table has to be created before company and fact tables
    # Because these two tables have foreign keys referecing to symbol tables 
    create_dim_symbol_table >> create_dim_company_profile_table >> create_dim_company_financials_table >> create_fact_table