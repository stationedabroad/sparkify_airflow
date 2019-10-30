from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from operators.redshift_connector import S3RedshiftConnector

class LoadDimensionOperator(S3RedshiftConnector):

    ui_color = '#80BD9E'
    insert_sql = """
        insert into {}({})
        {};
    """

    @apply_defaults
    def __init__(self,
                 table,
                 columns,
                 sql_insert,
                 redshift_conn_id,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(redshift_conn_id=redshift_conn_id, aws_conn_id='aws_credential', *args, **kwargs)

        self.table = table
        self.columns = columns
        self.sql_insert = sql_insert


    def execute(self, context):
        self.log.info('LoadDimensionOperator ... about to load')

        insert_sql = LoadDimensionOperator.insert_sql.format(
                self.table, 
                self.columns,
                self.sql_insert
            )
        print(insert_sql)
        print(self.redshift)
        self.redshift.run(insert_sql)
