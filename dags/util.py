import logging
from datetime import datetime, timedelta
import sqlite3

from airflow.hooks.sqlite_hook import SqliteHook

def get_num_active_dagruns(dag_id, conn_id='sqlite_default'):
    # if you've opted for a different backend for airflow, you will need to
    # refactor the two lines below. For a Postgres example, please refer to
    # https://github.com/Nextdoor/airflow_examples/blob/master/dags/util.py#L8
    airflow_db = SqliteHook(sqlite_conn_id=conn_id)
    conn = airflow_db.get_conn()
    cursor = conn.cursor()
    sql = """
         select count(*)
         from dag_run
         where dag_id = '{dag_id}'
         and state in ('running', 'queued', 'up_for_retry')
          """.format(dag_id=dag_id)
    cursor.execute(sql)
    num_active_dagruns = cursor.fetchone()[0]
    return num_active_dagruns

def is_latest_active_dagrun(**kwargs):
    """Ensure that there are no runs currently in progress and this is the most recent run."""
    num_active_dagruns = get_num_active_dagruns(kwargs['dag'].dag_id)
    logging.info('Number of active dag runs: {0}'.format(num_active_dagruns))

    expected_run_execution_date = datetime.now().date()
    logging.info('expected execution date: {}'.format(expected_run_execution_date))
    execution_date = kwargs['execution_date'].date()
    logging.info('Execution date: {}'.format(execution_date))
    is_latest_dagrun = execution_date == expected_run_execution_date
    logging.info("Is latest dagrun: {}".format(is_latest_dagrun))
    logging.info("Num dag runs active: {}".format(str(num_active_dagruns)))
    is_latest_active_dagrun = (is_latest_dagrun and num_active_dagruns == 1)
    return is_latest_active_dagrun

def dont_skip(table, **kwargs):
    if is_latest_active_dagrun(**kwargs) is False:
        if table['replication'] == 'rebuild':
            return False
        else:
            return True
    else:
        return True
