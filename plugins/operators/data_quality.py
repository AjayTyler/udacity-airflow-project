from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id='',
        tests={},
        *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tests=tests

    def execute(self, context):
        data_quality_tests = {
            'empty_table_check': {
                'test_type': 'table',
                'pass_type': 'pass_fail',
                'sql': 'SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END as record_count FROM {}',
                'pass_if': 1
            },
            'freshness_check': {
                'test_type': 'column',
                'pass_type': 'pass_warn',
                'sql': 'SELECT DATEDIFF(day, MAX({}), current_date) as days_old FROM {}',
                'pass_if': 1,
                'warning_message': 'Data is old and may be out of date.'
            },
            'null_columns_check': {
                'test_type': 'column',
                'pass_type': 'pass_fail',
                'sql': 'SELECT NVL2({}, 0, 1) as null_count FROM {}',
                'pass_if': 0
            }
        }

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test in self.tests.keys():
            self.log.info(f'Beginning {test}')
            test_info = data_quality_tests.get(test)

            # If a test type was entered wrong or doesn't exist, we stop
            # everything here.
            self.log.info(f'Checking to make sure the test exists.')
            if test_info is None:
                raise NameError(f'{test} is not a defined test.')

            # We start by fetching the info we need for the test specified
            # in our DAG file.
            test_type = test_info.get('test_type')
            pass_type = test_info.get('pass_type')
            test_pass_condition = test_info.get('pass_if')
            test_sql = test_info.get('sql')

            for check in self.tests.get(test):
                self.log.info(f'Formatting query...')
                # Depending on the test, we can either start immediately or we'll
                # need to get a few pieces of info to format things correctly.
                if test_type == 'table':
                    test_sql = test_sql.format(check)
                    table = check
                elif test_type == 'column':
                    self.log.info(f'Looking for data in {check}')
                    column = check.get('target_column')
                    table = check.get('table')
                    test_sql = test_sql.format(column, table)
                else:
                    raise NameError(f'{test_type} is not defined, cannot format SQL')

                self.log.info(f'Running {test} on {table}')
                result = redshift.get_records(test_sql)
                if result[0][0] == test_pass_condition:
                    self.log.info(f'{test} test passed.')
                else:
                    if pass_type == 'pass_warn':
                        self.log.info(f'{test_info.get("warning_message")}')
                    else:
                        raise ValueError(f'{test} failed for {table}: did not meet pass condition')
