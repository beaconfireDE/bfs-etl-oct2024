SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'

SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_WAREHOUSE = 'BF_ETL1007'
SNOWFLAKE_STAGE = 's3_stage_trans_order'

S3_FILE_PATH_TEMPLATE = 's3://octde2024/airflow_project/Transaction_Team3_{{ ds_nodash }}.csv'