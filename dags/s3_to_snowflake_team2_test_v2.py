from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from datetime import datetime

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'
SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER'

filename = 'AQ_Team2_' + '{{ ds_nodash }}' + '.csv'

with DAG(
    's3_to_snowflake_team2_test2',
    start_date=datetime(2024, 11, 5),
    end_date=datetime(2024, 11, 8),
    schedule_interval='0 1 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire', 'team2'],
    catchup=True,
) as dag:
    create_table = SnowflakeOperator(
        task_id='create_snowflake_table2',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=
        '''
        CREATE TABLE IF NOT EXISTS AIRFLOW1007.BF_DEV.prestage_AirQuality_Team2_test2 (
        Date DATE,
        Time TIME,
        "CO(GT)" FLOAT,
        "PT08.S1(CO)" INT,
        "NMHC(GT)" INT,
        "C6H6(GT)" FLOAT,
        "PT08.S2(NMHC)" INT,
        "NOx(GT)" INT,
        "PT08.S3(NOx)" INT,
        "NO2(GT)" INT,
        "PT08.S4(NO2)" INT,
        "PT08.S5(O3)" INT,
        T FLOAT,
        RH FLOAT,
        AH FLOAT)
        '''
    )

    clean_table = SnowflakeOperator(
        task_id='clean_repeat_data2',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=
        '''
        DELETE FROM AIRFLOW1007.BF_DEV.prestage_AirQuality_Team2_test2 
        WHERE DATE='{{ ds }}'
        '''
    )

    copy_into_prestg = SnowflakeOperator(
        task_id='write_snowflake_table2',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=
        '''
        COPY INTO AIRFLOW1007.BF_DEV.prestage_AirQuality_Team2_test2
        FROM @AIRFLOW1007.BF_DEV.S3_STAGE_TRANS_ORDER
        FILES = ('{filename}')
        FILE_FORMAT = csv_format
        ON_ERROR = 'ABORT_STATEMENT';
        '''.format(filename=filename)
    )

    create_table >> clean_table >> copy_into_prestg
