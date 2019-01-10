from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from looker_hook import LookerHook
from airflow.hooks.base_hook import BaseHook

import json
import os

class LookerScheduleRunOperator(BaseOperator):
    """
    LookerScheuleRun
    :param looker_conn_id:              The source s3 connection id.
    :type looker_conn_id:               string
    :param table                        The name of the destination table.
    :type                               string
    :param load_type:                   The method of loading into Redshift that
                                        should occur. Options:
                                            - "append"
                                            - "rebuild"
                                            - "truncate"
                                            - "upsert"
                                        Defaults to "append."
    :type load_type:                    string
    """


    template_fields = ('since',
                       'until')

    @apply_defaults
    def __init__(self,
                since,
                until,
                looker_conn_id,
                table,
                load_type,
                *args,
                **kwargs):
        super(LookerScheduleRunOperator, self).__init__(*args, **kwargs)
        self.since=since
        self.until=until
        self.looker_conn_id = looker_conn_id
        self.table = table
        self.load_type=load_type


    # create a query to run later
    def create_looker_query(self,LookerHook, query_body):
        looker_hook = LookerHook(self.looker_conn_id)
        query_body = looker_hook.create_query(self,query_body)
        return query_body

    def load_query(self):
        try:
            dirname = os.path.dirname(__file__)
            filepath = os.path.join(dirname,'../templates/{}_query.json'
                                .format(self.table))
            file = open(filepath)
            query = file.read()
            file.close()
        except IOError:
            print('Error: File, {} does not exist.'.format(filepath))
        return query

    def apply_filters(self,query):
        if self.load_type == 'append':
            query_body = json.loads(query)
            query_body['filters'] = {"{0}.created_date".format(self.table):
                                             "{0} to {1}".format(self.since,
                                                                 self.until)}
        return json.dumps(query_body)


    def load_s3_creds(self,BaseHook):
        try:
            connection = BaseHook.get_connection('s3')
        except AirflowException:
            pass
        s3_creds = connection.extra_dejson
        s3_creds['bucket'] = connection.host
        return s3_creds


    def build_schedule(self,query_id,table):
        try:
            dirname = os.path.dirname(__file__)
            filepath = os.path.join(dirname,'../templates/schedule_template.json')
            file = open(filepath)
        except IOError:
            print('Error: File, {} does not exist.'.format(filepath))

        template = json.loads(file.read())
        file.close()
        s3_creds = self.load_s3_creds(BaseHook)
        template['name'] = table
        template['query_id'] = query_id
        template['scheduled_plan_destination'][0]['address'] = '{}/{}/'.format(
            s3_creds['bucket'],
            table)
        template['scheduled_plan_destination'][0]['parameters'] = str(json.dumps({
                "region":s3_creds['region'],
                "access_key_id":s3_creds['aws_access_key_id']
                }))
        template['scheduled_plan_destination'][0]['secret_parameters'] = str(json.dumps({
            "secret_access_key":s3_creds['aws_secret_access_key']
            }))
        if self.load_type == 'append':
            template['filters'] = {"{0}.created_date".format(self.table):
                                             "{0} to {1}".format(self.since,
                                                                 self.until)}
        return json.dumps(template)


    def execute(self,context):
        looker_hook = LookerHook(self.looker_conn_id)
        query = self.apply_filters(self.load_query())
        r = looker_hook.create_query(query)
        query_id = r['id']
        schedule_body = self.build_schedule(query_id,self.table)
        r = looker_hook.run_scheduled_plan_once(schedule_body)


class LookerScheduleRunOperatorPlugin(AirflowPlugin):
    name = "looker_schedule_run_plugin"
    operators = [LookerScheduleRunOperator]
