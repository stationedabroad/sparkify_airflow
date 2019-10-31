from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

from operators.redshift_connector import S3RedshiftConnector

class StageToRedshiftOperator(S3RedshiftConnector):
    """
    This class executes generic COPY FROM to aws redshift from S3 buckets, it writes to staging tables
    in the entry layer for the redshift DWH.
    This class inherits from a generic AWS connector class which manages connections.

    params:
        exec_date - execution date of DAG
        table - table in redshift to write to
        s3_bucket - AWS S3 bucket
        s3_key - AWS S3 key
        json_method - formats into COPY FROM (using columns mappings or auto)
        redshift_conn_id - redshift connection details
        aws_conn_id - AWS connection login
    """
    template_fields = ('execution_date',)
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region 'us-west-2' compupdate off
        json '{}';
    """

    @apply_defaults
    def __init__(self,
                 exec_date,
                 table,
                 s3_bucket,
                 s3_key,
                 json_method,
                 redshift_conn_id,
                 aws_conn_id,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(redshift_conn_id=redshift_conn_id, aws_conn_id=aws_conn_id, *args, **kwargs)

        self.table = table
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.json_method = json_method
        self.execution_date = exec_date

    def execute(self, context):
        self.log.info(f'Execution of task on execution date {self.execution_date}')
        self.log.info('Starting to copy data from s3 to redshift')

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        copy_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.credentials.access_key,
            self.credentials.secret_key,
            self.json_method
        )

        self.redshift.run(copy_sql)