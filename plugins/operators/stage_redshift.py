from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id='',
            aws_credentials_id='',
            source_data='',
            destination_table='',
            json_format='auto',
            region='',
            *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.source_data = source_data
        self.destination_table = destination_table
        self.json_format = json_format
        self.region = region

    def execute(self, context):
        # We need some info for out queries, so we'll go ahead
        # and pull in our connection details.
        aws = MetastoreBackend().get_connection(self.aws_credentials_id)

        # We'll use the role ARN since it's recommended; you'll
        # have to use the extra field in the Airflow connection
        # entry, and we access it using `.extra_dejson.get` to
        # access it.
        role_arn = aws.extra_dejson.get('role_arn')

        # Clear the staging table prior to loading.
        truncate_redshift_staging_table_sql = ("""
            truncate {}
        """).format(self.destination_table)

        if (self.json_format == 'auto'):
            json_formatting = f"json '{self.json_format}'"
        elif (self.json_format is not None):
            json_formatting = f"format as json '{self.json_format}'"


        # Since we're just moving things to a staging table,
        # we'll turn off the compupdate and statupdate.
        s3_to_staging_sql = ("""
            copy {}
            from 's3://{}'
            credentials 'aws_iam_role={}'
            {}
            region '{}'
            blanksasnull emptyasnull
            compupdate off statupdate off
        """).format(self.destination_table, self.source_data, role_arn, json_formatting, self.region)

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear the staging table
        self.log.info(f'Truncating destination table {self.destination_table}')
        redshift.run(truncate_redshift_staging_table_sql)

        # Load the staging table
        self.log.info(f'Moving S3 data into Redshift table {self.destination_table}')
        redshift.run(s3_to_staging_sql)
