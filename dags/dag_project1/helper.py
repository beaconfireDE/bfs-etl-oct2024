from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator


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

def create_prestage_table(task_id: str, snowflake_conn_id: str, sql: str = SQL_CREATE_PRESTAGE_TABLE) -> SnowflakeOperator:
    task_create_prestage_table = SnowflakeOperator(
        task_id = task_id,
        snowflake_conn_id = snowflake_conn_id,
        sql = sql
    )

    return task_create_prestage_table


def load_data_to_snowflake(
        task_id: str, 
        snowflake_conn_id: str, 
        snowflake_stage: str,
        table: str,
        schema: str,
        files: list,
        file_format: str = "(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1, NULL_IF = ('NULL', 'null', ''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"')",
        pattern: str = ".*[.]csv"
    ) -> SnowflakeOperator:
    
    task_load_data = SnowflakeOperator(
        copy_into_prestg = CopyFromExternalStageToSnowflakeOperator(
        task_id=task_id,
        snowflake_conn_id=snowflake_conn_id,
        stage=snowflake_stage,
        table=table,
        schema=schema,
        files=files,
        file_format=file_format,
        pattern=pattern,
    )
    )

    return task_load_data

