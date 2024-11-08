"""
Project_2_Part_3
"""
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


SNOWFLAKE_CONN_ID = 'snowflake_conn'

SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_WAREHOUSE = 'BF_ETL1007'
SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'

SQL_FILE_FACT = "P2_load_stock_history.sql"
SQL_FILE_DIM1 = "P2_load_dim_symbols.sql"
SQL_FILE_DIM2 = "P2_load_dim_company_profile.sql"
SQL_FILE_DIM3 = "P2_load_dim_company_financials.sql"

DAG_ID = "project2_snowflake_to_snowflake_team1"

# DAG operation starting
with DAG(
    DAG_ID,
    start_date=datetime(2024, 11, 6),
    schedule_interval='0 0 * * *', # Everyday at 12:00am
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['team1_project2'],
    catchup=True,
) as dag:
    
    incremental_load_stock_history = SnowflakeOperator(
        task_id='incremental_load_stock_history',
        sql= SQL_FILE_FACT,
        snowflake_conn_id= SNOWFLAKE_CONN_ID,
    )

    incremental_load_symbols = SnowflakeOperator(
        task_id='incremental_load_symbols',
        sql= SQL_FILE_DIM1,
        snowflake_conn_id= SNOWFLAKE_CONN_ID,
    )

    incremental_load_company_profile = SnowflakeOperator(
        task_id='incremental_load_company_profile',
        sql= SQL_FILE_DIM2,
        snowflake_conn_id= SNOWFLAKE_CONN_ID,
    )

    incremental_load_company_financials = SnowflakeOperator(
        task_id='incremental_load_company_financials',
        sql= SQL_FILE_DIM3,
        snowflake_conn_id= SNOWFLAKE_CONN_ID,
    )

    (
        incremental_load_stock_history,
        incremental_load_symbols,
        incremental_load_company_profile,
        incremental_load_company_financials,
    )

