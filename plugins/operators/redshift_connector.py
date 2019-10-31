from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class S3RedshiftConnector(BaseOperator):
	"""
	This class manages basic AWS connections for AWS artifacts (S3 buckets, redshift)
	"""

	@apply_defaults
	def __init__(self,
                 redshift_conn_id,
                 aws_conn_id,
                 *args, **kwargs):

		super(S3RedshiftConnector, self).__init__(*args, **kwargs)
		self.redshift_conn_id = redshift_conn_id
		self.aws_conn_id = aws_conn_id

		aws_hook = AwsHook(self.aws_conn_id)
		self.credentials = aws_hook.get_credentials()
		self.redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

	def execute(self):
		pass