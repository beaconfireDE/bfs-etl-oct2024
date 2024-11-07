from datetime import datetime

from airflow import DAG

from .snowflake_tasks import create_prestage_table, load_data_to_snowflake


with DAG(
    "Team3_s3_to_s_incremental_DAG",
    start_date=datetime(2024, 11, 6),
    end_date = datetime(2024, 11, 8),
    schedule_interval='0 1 * * *',# UTC timezone, everyday at 1am
    catchup=True,
    tags=['Team3']
) as dag:
    
    task_create_prestage_table = create_prestage_table(
        task_id='create_prestage_table'
    )

    task_load_data_into_snowflake = load_data_to_snowflake(
        task_id='copy_into_table'
    )

    task_create_prestage_table >> task_load_data_into_snowflake
          
        
    

