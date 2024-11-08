from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_WAREHOUSE = 'BF_ETL1007'

SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'

DAG_ID = "project2_snowflake_to_snowflake"
SQL_FILE_SYMBOLS = "./sql_symbols.sql"
SQL_FILE_FACT = "./sql_fact.sql"
SQL_FILE_COMPANY_PROFILE = "./sql_company_profile.sql"

# DAG operator
with DAG(
    DAG_ID,
    start_date=datetime(2024, 11, 8),
    schedule_interval='0 0 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['team6project2'],
    catchup=True,
) as dag:

    snowflake_op_sql_merge_fact = SnowflakeOperator(
        task_id='snowflake_op_sql_merge_fact',
        sql=SQL_FILE_FACT
    )

    snowflake_op_sql_merge_company_profile = SnowflakeOperator(
        task_id='snowflake_op_sql_merge_company_profile',
        sql=SQL_FILE_COMPANY_PROFILE
    )
    snowflake_op_sql_merge_symbols = SnowflakeOperator(
        task_id='snowflake_op_sql_merge_symbols',
        sql=SQL_FILE_SYMBOLS
    )

    (
        snowflake_op_sql_merge_fact,
        snowflake_op_sql_merge_company_profile,
        snowflake_op_sql_merge_symbols

    )
