from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from S3_hook import S3Hook
from airflow.hooks.base_hook import BaseHook
import logging

class S3KeyRenameOperator(BaseOperator):
    """
    S3 Key Rename
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
        super(S3KeyRenameOperator, self).__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.table = table
        self.since = since

    def execute(self, context):
        key_prefix = self.table + '_2' # this reflects Looker's naming convention
        s3_hook = S3Hook(self.s3_conn_id)
        # evaluate if the export exists in the specified s3 bucket
        try:
            target_key = sorted(s3_hook.list_keys(
                bucket_name=self.s3_bucket,
                prefix='{0}/{1}/{2}'.format(self.table,
                                            self.since,
                                            key_prefix)))[0]
        except:
            logging.info('Error: File does not exist in specified S3 bucket.')
        # strips Looker metadata and returns a stable filename
        renamed_key = '{0}/{1}/{0}.csv'.format(self.table,
                                               self.since)
        # replace the base key with the newest version of the table
        s3_hook.copy_object(target_key,
                            renamed_key,
                            self.s3_bucket,
                            self.s3_bucket)
        # create a separate copy for archival puposes
        archived_key = '{0}/{1}/archive_{2}'.format(self.table,
                                                    self.since,
                                                    target_key.split('/')[2]
                                                    )
        logging.info('Creating archive of {}'.format(archived_key))
        s3_hook.copy_object(target_key,
                            archived_key,
                            self.s3_bucket,
                            self.s3_bucket)
        s3_hook.delete_objects(self.s3_bucket,
                               target_key)
        # prune old archives
        archives = sorted(s3_hook.list_keys(bucket_name=self.s3_bucket,
                         prefix='{0}/{1}/archive_{2}'.format(self.table,
                                                             self.since,
                                                             key_prefix)))
        if len(archives) >= 5:
            logging.info('Dropping old archive, {}'.format(archives[0]))
            s3_hook.delete_objects(self.s3_bucket, archives[0])


class S3KeyRenameOperatorPlugin(AirflowPlugin):
    name = "s3_key_rename_plugin"
    operators = [S3KeyRenameOperator]
