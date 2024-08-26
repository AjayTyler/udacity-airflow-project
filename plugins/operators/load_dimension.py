from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id='',
            destination_table='',
            primary_key='',
            insert_mode='merge',
            sql='',
            *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.primary_key = primary_key
        self.insert_mode = insert_mode
        self.sql = sql


    def execute(self, context):
        # We'll want our dimensional load operations to be able to
        # run in parallel, so we'll make sure the temporary tables
        # have a unique name.
        temp_table_name = f'public.temp_{self.destination_table.split(".")[-1]}_staging'

        # We want to leverage Redshift's MERGE operations, but we
        # can't use views or subqueries if we do that. So, we'll
        # create a table temporarily.
        temp_table_sql = (f'''
            DROP TABLE IF EXISTS {temp_table_name};
            CREATE TABLE {temp_table_name} (like {self.destination_table});
        ''')

        # Here we populate the temp table using a query that pulls
        # in data from the appropriate staging table.
        populate_temp_table_sql = ('''
            INSERT INTO {}
            {}
        ''').format(temp_table_name, self.sql)

        # We have two options to perform our incremental load: merge,
        # which is useful for revising data, or just a classic insert,
        # which presumes we do not need to check for duplicates.
        merge_into_destination_table_sql = (f'''
            MERGE INTO {self.destination_table}
            USING {temp_table_name} as st
                ON {self.destination_table}.{self.primary_key} = st.{self.primary_key}
            REMOVE DUPLICATES
        ''')

        insert_into_destination_table_sql = (f'''
            INSERT INTO {self.destination_table}
            SELECT * FROM {temp_table_name}
        ''')

        append_new_into_destination_table_sql = (f'''
            INSERT INTO {self.destination_table}
            SELECT *
            FROM {temp_table_name}
            WHERE {self.primary_key} NOT IN (SELECT sub.{self.primary_key} FROM {self.destination_table} as sub)
        ''')

        # Just for convenience below, we'll choose the appropriate SQL
        # command and assign it to a variable so the run command below
        # is easily delineated.
        if (self.insert_mode == 'merge'):
            sql_command = merge_into_destination_table_sql
        elif (self.insert_mode == 'insert'):
            sql_command = insert_into_destination_table_sql
        elif(self.insert_mode == 'append_new'):
            sql_command = append_new_into_destination_table_sql
        else:
            raise NameError(f'"{self.insert_mode}" supplied as insert mode')

        # After the insert, we'll use this to remove the staging table.
        # It'll automatically drop after the session is closed, but I
        # like to be specific.
        drop_temp_table = f'DROP TABLE IF EXISTS {temp_table_name}'

        # The actual operations begin here: we fetch the creds we need.
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Creating `{temp_table_name}`')
        redshift.run(temp_table_sql)

        self.log.info(f'Populating `{temp_table_name}`')
        redshift.run(populate_temp_table_sql)

        self.log.info(f'Adding new data to {self.destination_table}')
        redshift.run(sql_command)

        self.log.info(f'Removing `{temp_table_name}`')
        redshift.run(drop_temp_table)
