import datetime as dt
import time
# from airflow.operators import S3KeySensor
# from airflow.operators.redshift_load_plugin import S3ToRedshiftOperator
# from airflow.operators.s3_key_rename_plugin import S3KeyRenameOperator
from airflow.operators.looker_schedule_run_plugin import LookerScheduleRunOperator

from airflow import DAG

default_args = {
    'owner': 'Jesse Carah',
    'start_date': dt.datetime(2018, 11, 20),
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=5)
}

dag = DAG('looker_test',
    default_args=default_args,
    schedule_interval='@once'
)

get_dashboard = LookerScheduleRunOperator(
    task_id='testy_run',
    looker_conn_id='looker_api',
    table='history',
    dag=dag
)

get_dashboard
