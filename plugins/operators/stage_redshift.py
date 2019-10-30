from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
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

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.json_method = json_method
        self.execution_date = exec_date

    def execute(self, context):
        self.log.info(f'Execution of task on execution date {self.execution_date}')

        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()

        self.log.info('Making connection to redshift')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        # self.log.info('clearing data from Redshift table for new data')
        # redshift.run("DELETE FROM {}".format(self.table))

        self.log.info('Starting to copy data from s3 to redshift')
        # rendered_data_source_key = self.s3_key.format(**context)
        #data_source_s3_path = "s3://{}/{}/".format(self.s3_bucket, rendered_data_source_key)
        data_source_s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        copy_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            data_source_s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_method
        )

        redshift.run(copy_sql)