from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define constants
S3_BUCKET = 's3://octde2024/aiflow_project/'
SNOWFLAKE_CONN_ID = 'snowflake_conn'
DATABASE = 'AIRFLOW1007'
SCHEMA = 'BF_DEV'
TABLE_NAME = 'prestage_AQ20241106_Team2'
S3_FILE_NAME = 'AQ_Team2_20241106.csv'
STAGE_NAME = 'S3_STAGE_TRANS_ORDER'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    's3_to_snowflake_load',
    default_args=default_args,
    description='Load data from S3 to Snowflake',
    schedule_interval='@daily',  # Trigger manually or set specific timing
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task to load data from S3 to Snowflake
    load_data_task = S3ToSnowflakeOperator(
        task_id='load_data_to_snowflake',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        s3_keys=[f"{S3_BUCKET}{S3_FILE_NAME}"],
        table=TABLE_NAME,
        schema=SCHEMA,
        stage=f"{DATABASE}.{SCHEMA}.{STAGE_NAME}",
        file_format='(type = "CSV", skip_header = 1, FIELD_OPTIONALLY_ENCLOSED_BY = \'"\' )',
        warehouse='compute_wh',
        role='accountadmin',
    )

    load_data_task
