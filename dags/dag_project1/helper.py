from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


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
    create_prestage_table = SnowflakeOperator(
        task_id = task_id,
        snowflake_conn_id = snowflake_conn_id,
        sql = sql
    )

    return create_prestage_table