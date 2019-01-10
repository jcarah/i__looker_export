import datetime as dt
import time
# from airflow.operators import S3KeySensor
# from airflow.operators.redshift_load_plugin import S3ToRedshiftOperator
# from airflow.operators.s3_key_rename_plugin import S3KeyRenameOperator
from airflow.operators.looker_schedule_run_plugin import LookerScheduleRunOperator

from airflow import DAG

since = "{{ yesterday_ds }}".replace('-','/')
until = "{{ ds }}".replace('-','/')


default_args = {
    'owner': 'Jesse Carah',
    'start_date': dt.datetime(2018, 11, 20),
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=5)
}

dag = DAG('looker_test',
    default_args=default_args,
    schedule_interval='@daily'
)

get_dashboard = LookerScheduleRunOperator(
    task_id='testy_run',
    looker_conn_id='looker_api',
    table='merge_query_source_query',
    load_type='append',
    since=since,
    until=until,
    dag=dag
)

# table = 'user'
# rename = S3KeyRenameOperator(
#     task_id='{0}_rename'.format(table),
#     s3_conn_id='s3',
#     s3_bucket='jessecarah',
#     # s3_key='{0}/{0}.csv'.format(table),
#     table=table,
#     dag=dag
# )

# rename
get_dashboard
