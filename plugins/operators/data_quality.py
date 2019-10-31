from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from operators.redshift_connector import S3RedshiftConnector

class DataQualityOperator(S3RedshiftConnector):
    """
    Run Data Quality Checks against AWS redhsift

    self.tests is a nested dictionary of tests passed in and has the form:

    {"test_1": {"sql": "your_test_1_sql", "result": "your_test_1_sql_expected_result" },
     "test_2": {"sql": "your_test_2_sql", "result": "your_test_2_sql_expected_result" }
    ...
    }

    The execute method it carries out the passed tests, comparing the return DB values to the
    'result' value key in the self.tests dict.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tests,
                 redshift_conn_id,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(redshift_conn_id=redshift_conn_id, aws_conn_id='aws_credential', *args, **kwargs)
        self.tests = tests

    def execute(self, context):
        self.log.info(f'Begin data quality checks, total checks {len(self.tests)}')

        failed_tests = []

        for test in self.tests:
            self.log.info(f'Running {test}')
            running_test = self.tests[test]
            records = self.redshift.get_records(running_test['sql'])
            self.log.info(f'output received test {test}: {records}')
            
            if len(records) < 1 or records[0][0] != running_test['result']:
                failed_tests.append(test)
            if failed_tests:
                raise ValueError(f'Date Quality check failed in these tests {failed_tests}')
            self.log.info(f'All tests passed {self.tests.keys()}')                