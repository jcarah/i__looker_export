import datetime as dt
import time
from airflow.operators.s3_key_sensor_plugin import S3KeySensor
from airflow.operators.redshift_load_plugin import S3ToRedshiftOperator
from airflow.operators.s3_key_rename_plugin import S3KeyRenameOperator
from airflow.operators.looker_schedule_run_plugin import LookerScheduleRunOperator
from airflow.operators.s3_cleanup_plugin import S3CleanupOperator

from airflow import DAG

default_args = {
    'owner': 'Jesse Carah',
    'start_date': dt.datetime(2019, 1, 1),
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=5)
}

dag = DAG('i__looker-to-redshift',
    default_args=default_args,
    schedule_interval='@once',
    catchup=True
)

tables = [
          {
            "name": "history",
            "replication": "upsert"
          },
          {
            "name": "look",
            "replication": "rebuild"
          },
          {
            "name": "node",
            "replication": "rebuild"
          },
          {
            "name": "user_facts",
            "replication": "rebuild"
          },
          {
            "name": "merge_query",
            "replication": "upsert"
          },
          # {
          #   "name": "query",
          #   "replication": "upsert"
          # },
          {
            "name": "source_query",
            "replication": "upsert"
          },
          {
            "name": "user",
            "replication": "rebuild"
          },
          {
            "name": "merge_query_source_query",
            "replication": "upsert"
          },
          {
            "name": "result_maker",
            "replication": "rebuild"
          },
          {
            "name": "sql_runner_query",
            "replication": "upsert"
          }
         ]

# tables = [{
#           "name": "user",
#           "replication": "rebuild"
#          }]

since = "{{ yesterday_ds }}".replace('-','/')
until = "{{ ds }}".replace('-','/')

for table in tables:
    s3_cleanup = S3CleanupOperator(
        task_id='{0}_cleanup'.format(table['name']),
        s3_conn_id='s3',
        s3_bucket='jessecarah', # refactor to use meta data from connection
        table=table['name'],
        dag=dag
        )

    build_schedule = LookerScheduleRunOperator(
        task_id='{0}_schedule_build'.format(table['name']),
        looker_conn_id='looker_api',
        load_type=table['replication'],
        table=table['name'],
        since=since,
        until=until,
        dag=dag
        )

    sense_s3_key =  S3KeySensor(
        task_id='{0}_sense'.format(table['name']),
        verify=False,
        wildcard_match=True,
        aws_conn_id='s3',
        bucket_name='jessecarah', # refactor to use meta data from connection
        bucket_key='{0}/{0}_{1}*'.format(table['name'], time.strftime('%Y-%m-%d')),
        timeout=18*60*60,
        poke_interval=15,
        dag=dag
        )

    rename = S3KeyRenameOperator(
        task_id='{0}_rename'.format(table['name']),
        s3_conn_id='s3',
        s3_bucket='jessecarah', # refactor to use meta data from connection
        table=table['name'],
        dag=dag
        )

    load = S3ToRedshiftOperator(
        task_id='{0}_load'.format(table['name']),
        s3_conn_id='s3',
        s3_bucket='jessecarah',
        s3_key='{0}/{0}.csv'.format(table['name']),
        load_type=table['replication'],
        redshift_conn_id='redshift',
        redshift_schema='airflow',
        table=table['name'],
        primary_key='id',
        copy_params=["COMPUPDATE OFF",
                      "STATUPDATE OFF",
                      "FORMAT as CSV",
                      "TIMEFORMAT 'auto'",
                      "BLANKSASNULL",
                      "TRUNCATECOLUMNS",
                      "region as 'us-east-1'",
                      "IGNOREHEADER 1"],
        origin_schema='../templates/{0}_schema.json'.format(table['name']),
        schema_location='local',
        incremental_key='id' if table['replication'] == 'upsert' else None,
        dag=dag
        )

    s3_cleanup >> build_schedule >> sense_s3_key >> rename >> load
