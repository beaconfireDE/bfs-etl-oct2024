"""
Example use of Snowflake related operators.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import S3ToSnowflakeOperator



SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'

SNOWFLAKE_ROLE = 'AW_developer'
SNOWFLAKE_WAREHOUSE = 'aw_etl'
SNOWFLAKE_STAGE = 's3_stage_trans_order'

S3_FILE_PATH_TEMPLATE = 's3://octde2024/airflow_project/Transaction_Team3_{{ ds_nodash }}.csv'
SNOWFLAKE_SAMPLE_TABLE = 'prestage_Transaction_Team3'



with DAG(
    "s3_to_snowflake_incremental_load",
    start_date=datetime(2024, 11, 6),
    end_date = datetime(2024, 11, 8),
    schedule_interval='0 1 * * *',# UTC timezone, everyday at 1am
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    catchup=True,
    tags=['Team3']
) as dag:
    
    create_prestage_table = SnowflakeOperator(
        task_id="create_prestage_transaction_team3",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            create or replace TABLE AIRFLOW1007.BF_DEV.PRESTAGE_TRANSACTIONs_TEAM3 (
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
        """,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    copy_into_prestg = S3ToSnowflakeOperator(
        task_id="copy_into_table",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        s3_keys=[S3_FILE_PATH_TEMPLATE],
        table=SNOWFLAKE_SAMPLE_TABLE,
        stage=SNOWFLAKE_STAGE,
        file_format="(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1, NULL_IF = ('NULL', 'null', ''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"')",
        pattern=".*[.]csv",  
    )

    create_prestage_table >> copy_into_prestg
          
        
    

