from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        insert into {}({})
        {}
    """

    @apply_defaults
    def __init__(self,
                 table,
                 columns,
                 sql_insert,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.table = table
        self.columns = columns
        self.sql_insert = sql_insert


    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')




        insert_sql = insert_sql.format(
                self.table, 
                self.columns,
                self.sql_insert
            )
