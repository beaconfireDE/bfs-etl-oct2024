import sys
import os
import logging

from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from config import SNOWFLAKE_CONN_ID, SNOWFLAKE_STAGE, SNOWFLAKE_SCHEMA, SNOWFLAKE_DATABASE

SQL_CREATE_PRESTAGE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.PRESTAGE_TRANSACTION_TEAM3 (
                TRANSACTIONID NUMBER(10,0),
                DATE DATE,
                CUSTOMERID NUMBER(10,0),
                PRODUCTID NUMBER(10,0),
                QUANTITY NUMBER(5,0),
                PRICE NUMBER(10,2),
                TOTALAMOUNT NUMBER(15,2),
                PAYMENTMETHOD VARCHAR(20),
                STORELOCATION VARCHAR(50),
                EMPLOYEEID NUMBER(10,0)
            );
        """

def create_prestage_table(task_id: str) -> SnowflakeOperator:
    
    return SnowflakeOperator(
        task_id = task_id,
        snowflake_conn_id = SNOWFLAKE_CONN_ID,
        sql = SQL_CREATE_PRESTAGE_TABLE
    )




def list_files(task_id: str) -> SnowflakeOperator:
    list_files_sql = f"""
    LIST @{SNOWFLAKE_STAGE} PATTERN='Transaction_Team3_\\d{{8}}.csv';
    """
    return SnowflakeOperator(
        task_id=task_id,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=list_files_sql,
        do_xcom_push=True
    )


def load_files(task_id: str) -> SnowflakeOperator:
    sql_copy_into_template = """
    {% for file in ti.xcom_pull(task_ids='list_files') %}
    COPY INTO {{ params.database }}.{{ params.schema }}.PRESTAGE_TRANSACTION_TEAM3
    FROM @{% raw %}{{{% endraw %} params.stage }}/{{ file }}
    FILE_FORMAT = (
        TYPE = 'CSV',
        FIELD_DELIMITER = ',',
        SKIP_HEADER = 1,
        NULL_IF = ('NULL', 'null', ''),
        EMPTY_FIELD_AS_NULL = TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
    );
    {% endfor %}
    """

    return SnowflakeOperator(
        task_id=task_id,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=sql_copy_into_template,
        params={
            'database': SNOWFLAKE_DATABASE,
            'schema': SNOWFLAKE_SCHEMA,
            'stage': SNOWFLAKE_STAGE
        }
    )

