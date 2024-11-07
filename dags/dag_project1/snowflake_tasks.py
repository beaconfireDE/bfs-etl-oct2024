from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

from config import SNOWFLAKE_CONN_ID, SNOWFLAKE_STAGE, SNOWFLAKE_SCHEMA, SNOWFLAKE_DATABASE

SQL_CREATE_PRESTAGE_TABLE = """
create or replace TABLE AIRFLOW1007.BF_DEV.PRESTAGE_TRANSACTION_TEAM3 (
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




def load_data_to_snowflake(task_id: str) -> SnowflakeOperator:
    sql_copy_into = f"""
    COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.PRESTAGE_TRANSACTION_TEAM3
    FROM @{SNOWFLAKE_STAGE}/Transaction_Team3_{{{{ ds_nodash }}}}.csv
    FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = ',', SKIP_HEADER = 1, NULL_IF = ('NULL', 'null', ''), EMPTY_FIELD_AS_NULL = TRUE, FIELD_OPTIONALLY_ENCLOSED_BY = '\"')
    """
    return SnowflakeOperator(
        task_id=task_id,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=sql_copy_into
    )

