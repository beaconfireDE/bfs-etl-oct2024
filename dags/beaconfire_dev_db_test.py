"""
Example use of Snowflake related operators.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


SNOWFLAKE_CONN_ID = 'snowflake_conn'#
SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_WAREHOUSE = 'BF_ETL1007'
SNOWFLAKE_STAGE = 'beaconfire_stage'

# source
SNOWFLAKE_SCHEMA_SOURCE = 'DCCM'
SNOWFLAKE_DATABASE_SOURCE = 'US_STOCK_DAILY'

SNOWFLAKE_TABLE_SH_SOURCE = 'STOCK_HISTORY'
SNOWFLAKE_TABLE_CP_SOURCE = 'COMPANY_PROFILE'
SNOWFLAKE_TABLE_SYM_SOURCE = 'SYMBOLS'

# target
SNOWFLAKE_DATABASE_TARGET = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA_TARGET = 'BF_DEV'

SNOWFLAKE_TABLE_SH_TARGET = 'fact_Stock_History_TEAM4'
SNOWFLAKE_TABLE_CP_TARGET = 'dim_COMPANY_PROFILE_TEAM4'
SNOWFLAKE_TABLE_SYM_TARGET = 'dim_SYMBOLS_TEAM4'

# SQL commands
# CREATE_TABLE_SQL_STRING = (
#     f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SAMPLE_TABLE} (name VARCHAR(250), id INT);"
# )

SQL_INSERT_STATEMENT_SH = f"INSERT INTO {SNOWFLAKE_DATABASE_TARGET}.{SNOWFLAKE_SCHEMA_TARGET}.{SNOWFLAKE_TABLE_SH_TARGET} (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE) SELECT SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE FROM {SNOWFLAKE_DATABASE_SOURCE}.{SNOWFLAKE_SCHEMA_SOURCE}.{SNOWFLAKE_TABLE_SH_SOURCE}"
SQL_INSERT_STATEMENT_CP = f"INSERT INTO {SNOWFLAKE_DATABASE_TARGET}.{SNOWFLAKE_SCHEMA_TARGET}.{SNOWFLAKE_TABLE_CP_TARGET} (ID, SYMBOL, PRICE, BETA, MKTCAP, INDUSTRY, SECTOR) SELECT ID, SYMBOL, PRICE, BETA, MKTCAP, INDUSTRY, SECTOR FROM {SNOWFLAKE_DATABASE_SOURCE}.{SNOWFLAKE_SCHEMA_SOURCE}.{SNOWFLAKE_TABLE_CP_SOURCE}"
SQL_INSERT_STATEMENT_SYM = f"INSERT INTO {SNOWFLAKE_DATABASE_TARGET}.{SNOWFLAKE_SCHEMA_TARGET}.{SNOWFLAKE_TABLE_SYM_TARGET} (SYMBOL, NAME, EXCHANGE) SELECT SYMBOL, NAME, EXCHANGE FROM {SNOWFLAKE_DATABASE_SOURCE}.{SNOWFLAKE_SCHEMA_SOURCE}.{SNOWFLAKE_TABLE_SYM_SOURCE}"

# SQL_LIST = [SQL_INSERT_STATEMENT % {"id": n} for n in range(0, 10)]
# SQL_MULTIPLE_STMTS = "; ".join(SQL_LIST)
DAG_ID = "beaconfire_dev_db_test"

# [START howto_operator_snowflake]

with DAG(
    DAG_ID,
    start_date=datetime(2024, 11, 7),
    schedule_interval='0 11 * * *', # at 11 everyday
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=True,
) as dag:
    
    # [START snowflake_example_dag]
    # snowflake_op_sql_str = SnowflakeOperator(
    #     task_id='snowflake_op_sql_str',
    #     sql=CREATE_TABLE_SQL_STRING,
    #     warehouse=SNOWFLAKE_WAREHOUSE,
    #     database=SNOWFLAKE_DATABASE,
    #     schema=SNOWFLAKE_SCHEMA,
    #     role=SNOWFLAKE_ROLE,
    # )

    # initial copying of data
    snowflake_insert_sh_params = SnowflakeOperator(
        task_id='snowflake_insert_sh_params',
        sql=SQL_INSERT_STATEMENT_SH,
        # parameters={"id": 5},
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_insert_cp_params = SnowflakeOperator(
        task_id='snowflake_insert_cp_params',
        sql=SQL_INSERT_STATEMENT_CP,
        # parameters={"id": 5},
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_insert_sym_params = SnowflakeOperator(
        task_id='snowflake_insert_sym_params',
        sql=SQL_INSERT_STATEMENT_SYM,
        # parameters={"id": 5},
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )

    # snowflake_op_sql_list = SnowflakeOperator(task_id='snowflake_op_sql_list', sql=SQL_LIST)

    # snowflake_op_sql_multiple_stmts = SnowflakeOperator(
    #     task_id='snowflake_op_sql_multiple_stmts',
    #     sql=SQL_MULTIPLE_STMTS,
    # )

    # snowflake_op_template_file = SnowflakeOperator(
    #    task_id='snowflake_op_template_file',
    #    sql='./beaconfire_dev_db_test.sql',
    # )

    # [END howto_operator_snowflake]

    # (
    #     snowflake_op_sql_str
    #     >> [
    #         snowflake_op_with_params,
    #         snowflake_op_sql_list,
    #         snowflake_op_template_file,
    #         snowflake_op_sql_multiple_stmts,
    #     ]
        
    # )
    [  
        snowflake_insert_sh_params,
        snowflake_insert_cp_params,
        snowflake_insert_sym_params
    ]
    

    # [END snowflake_example_dag]

