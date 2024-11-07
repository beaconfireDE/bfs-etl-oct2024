"""
Example use of Snowflake related operators.
"""
import os
from datetime import datetime, date

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator


# SNOWFLAKE_CONN_ID = 'snowflake_conn'
# SNOWFLAKE_DATABASE = 'beaconfire'
# SNOWFLAKE_SCHEMA = 'dev_db'

# SNOWFLAKE_ROLE = 'AW_developer'
# SNOWFLAKE_WAREHOUSE = 'aw_etl'
# SNOWFLAKE_STAGE = 's3_stage_trans_order'

# with DAG(
#     "s3_data_copy_test",
#     start_date=datetime(2022, 7, 13),
#     end_date = datetime(2022, 7, 16),
#     schedule_interval='0 7 * * *',
#     default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
#     tags=['beaconfire'],
#     catchup=True,
# ) as dag:

#     copy_into_prestg = CopyFromExternalStageToSnowflakeOperator(
#         task_id='prestg_product_order_trans',
#         files=['product_order_trans_{{ ds[5:7]+ds[8:10]+ds[0:4] }}.csv'],
#         table='prestg_product_order_trans',
#         schema=SNOWFLAKE_SCHEMA,
#         stage=SNOWFLAKE_STAGE,
#         file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
#             NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
#             ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
#     )

#     copy_into_prestg
          
        
SNOWFLAKE_CONN_ID = 'snowflake_conn'#
SNOWFLAKE_DATABASE = 'AIRFLOW1007'#
SNOWFLAKE_SCHEMA = 'BF_DEV'#

SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_WAREHOUSE = 'BF_ETL1007'
SNOWFLAKE_STAGE = 's3_stage_trans_order'

with DAG(
    "s3_data_copy_test",
    start_date=datetime(2024, 11, 6),
    end_date = datetime(2024, 11, 8),
    schedule_interval='0 8 * * *', # every day at 8
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=True,
) as dag:

    copy_into_prestg = CopyFromExternalStageToSnowflakeOperator(
        task_id='prestg_product_order_trans',
        files=['Labevents_4_{{ ds_nodash }}.csv'],
        table='PRESTAGE_LABEVENTS_4',
        schema=SNOWFLAKE_SCHEMA,
        database=SNOWFLAKE_DATABASE,
        stage=SNOWFLAKE_STAGE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
            NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
            ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    )

    # check_context = SnowflakeOperator(
    #     task_id='check_database_context',
    #     sql='SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();',
    #     database=SNOWFLAKE_DATABASE,
    #     schema=SNOWFLAKE_SCHEMA,
    #     snowflake_conn_id=SNOWFLAKE_CONN_ID,
    # )

    # check_context >> copy_into_prestg
    copy_into_prestg