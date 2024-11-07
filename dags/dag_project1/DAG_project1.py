from datetime import datetime

from airflow import DAG

from config import SNOWFLAKE_CONN_ID, SNOWFLAKE_STAGE, SNOWFLAKE_SCHEMA
from helper import create_prestage_table, load_data_to_snowflake


with DAG(
    "Team3_s3_to_s_incremental_DAG",
    start_date=datetime(2024, 11, 6),
    end_date = datetime(2024, 11, 8),
    schedule_interval='0 1 * * *',# UTC timezone, everyday at 1am
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    catchup=True,
    tags=['Team3']
) as dag:
    
    task_create_prestage_table = create_prestage_table(
        task_id='create_prestage_table', snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    task_load_data_into_snowflake = load_data_to_snowflake(
        task_id='copy_into_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        snowflake_stage=SNOWFLAKE_STAGE,
        table='prestage_Transaction_Team3',
        schema=SNOWFLAKE_SCHEMA
    )

    task_create_prestage_table >> task_load_data_into_snowflake
          
        
    

