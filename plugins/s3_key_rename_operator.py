from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from S3_hook import S3Hook

class S3KeyRenameOperator(BaseOperator):
    """
    S3 Key Rename
    :param s3_conn_id:              The source s3 connection id.
    :type s3_conn_id:               string
    :param s3_bucket:               The source s3 bucket.
    :type s3_bucket:                string
    :param s3_key:                  The source s3 key.
    :type s3_key:                   string
    :param table:                   The base table name.
    :type table:                    string
    """

    @apply_defaults
    def __init__(self,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 # table,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        # self.table = table

    def execute(self, context):
        key_prefix = self.s3_key.split('.')[0]
        key_extension = self.s3_key.split('.')[1]
        hook = S3Hook(self.s3_conn_id)
        # grab all keys and sort them chronologically (based off name)
        # the 0th positioned key will by our base key
        keys = sorted([key for key in hook.list_keys(bucket_name=self.s3_bucket,
                                                prefix=key_prefix)
                                            if  key.endswith(key_extension)])
        print(keys)
        newest_key = keys[len(keys) - 1]
        if len(keys) >= 5:
            # prune bucket of oldest csv
            hook.delete_objects(self.s3_bucket, keys[1])
        # replace the base key with the newest version of the table
        hook.copy_object(newest_key, self.s3_key, self.s3_bucket, self.s3_bucket)

class S3KeyRenameOperatorPlugin(AirflowPlugin):
    name = "s3_key_rename_plugin"
    operators = [S3KeyRenameOperator]
