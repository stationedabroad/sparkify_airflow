from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from operators.redshift_connector import S3RedshiftConnector

class LoadFactDimensionOperator(S3RedshiftConnector):
    """
    Class to load fact and dimension tables (using insert into sql string).  It does this
    by executing sql directly in redshift based on the connection details and passed sql.
    This class inherits from a generic S3 and Redshift parent class which abstracts away 
    connection details.

    params:
        table - name of table to insert into
        columns - columns of table to insert into (comma seperated string)
        sql_insert - body of sql insert
        redshift_conn_id - redshift connection id
    """

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

        super(LoadFactDimensionOperator, self).__init__(redshift_conn_id=redshift_conn_id, aws_conn_id='aws_credential', *args, **kwargs)

        self.table = table
        self.columns = columns
        self.sql_insert = sql_insert


    def execute(self, context):
        self.log.info('LoadFactDimensionOperator ... about to load')
        insert_sql = LoadFactDimensionOperator.insert_sql.format(
                self.table, 
                self.columns,
                self.sql_insert
            )

        self.redshift.run(insert_sql)