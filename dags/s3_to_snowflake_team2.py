from airflow import DAG
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from datetime import datetime

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'
SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER'

with DAG(
    's3_to_snowflake_team2',
    start_date=datetime(2024, 11, 7),
    end_date=datetime(2024, 11, 7),
    schedule_interval='0 8 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire', 'team2'],
    catchup=True,
) as dag:

    copy_into_prestg = CopyFromExternalStageToSnowflakeOperator(
        task_id='copy_csv_into_snowflake',
        table='prestage_AirQuality_Team2',
        schema=SNOWFLAKE_SCHEMA,
        database=SNOWFLAKE_DATABASE,
        stage=SNOWFLAKE_STAGE,
        role=SNOWFLAKE_ROLE,
        pattern='AQ_Team2_20241107.csv',
        file_format='csv_format_team2',
    )

    copy_into_prestg
