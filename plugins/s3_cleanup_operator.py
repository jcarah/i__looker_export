from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from S3_hook import S3Hook


class S3CleanupOperator(BaseOperator):
    """
    S3 Cleanup
    :param s3_conn_id:              The source s3 connection id.
    :type s3_conn_id:               string
    :param s3_bucket:               The source s3 bucket.
    :type s3_bucket:                string
    :param table:                   The base table name.
    :type table:                    string
    """

    template_fields = ('since',)

    @apply_defaults
    def __init__(self,
                 s3_conn_id,
                 s3_bucket,
                 table,
                 since,
                 *args,
                 **kwargs):
        super(S3CleanupOperator, self).__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.table = table
        self.since = since

    def execute(self, context):
        key_prefix = self.table + '_2' # this reflects Looker's naming convention
        s3_hook = S3Hook(self.s3_conn_id)
        try:
            print('{0}/{1}/{2}'.format(self.table,
                                       self.since,
                                       key_prefix))
            keys_to_delete = s3_hook.list_keys(
                bucket_name=self.s3_bucket,
                prefix='{0}/{1}/{2}'.format(self.table,
                                            self.since,
                                            key_prefix)
            )

            print('Removing {0}.'.format(', '.join(keys_to_delete)))
            s3_hook.delete_objects(
                bucket=self.s3_bucket,
                keys=keys_to_delete
            )
        except:
            print('Nothing to cleanup. Moving on.')
            pass
        return

class S3CleanupOperatorPlugin(AirflowPlugin):
    name = "s3_cleanup_plugin"
    operators = [S3CleanupOperator]
