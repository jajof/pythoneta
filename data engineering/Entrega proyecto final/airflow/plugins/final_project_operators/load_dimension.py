from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 sql_query = '',
                 destination_table = '',
                 delete_insert = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.destination_table = destination_table
        self.delete_insert = delete_insert

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.delete_insert:
            self.log.info(f"Clearing data from {self.destination_table} Redshift table")
            redshift_hook.run("DELETE FROM {}".format(self.destination_table))
        self.log.info('Loading data into dimension table...')
        redshift_hook.run(self.sql_query)