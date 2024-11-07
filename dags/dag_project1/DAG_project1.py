from datetime import datetime
import sys
import os

from airflow import DAG
# Add the current directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from snowflake_tasks import create_prestage_table, list_files, load_files


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

    task_list_files = list_files(
        task_id='list_files'
    )

    task_load_files = load_files(
        task_id='load_files_to_snowflake'
    )

    task_create_prestage_table >> task_list_files >> task_load_files
          
        
    

