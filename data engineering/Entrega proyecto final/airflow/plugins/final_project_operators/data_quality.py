from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    template = """
        SELECT count(*)
        FROM {table}
        where {table}.{column} is NULL 
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id = '',
                tables_with_rows = [],
                columns_without_nulls = [()], # list of tuples (table, column)
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_with_rows = tables_with_rows
        self.columns_without_nulls = columns_without_nulls

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # check that tables have rows
        self.log.info('Checking that all tables have rows.')
        for table in self.tables_with_rows:
            self.log.info(f'Checking table {table}')
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")

        # Check if columns have nulls. 
        self.log.info('Checking if specific columns have null values.')
        for table, column in self.columns_without_nulls:
            formated_sql = DataQualityOperator.template.format(table = table, column=column)
            records = redshift_hook.get_records(formated_sql)
            num_records = records[0][0]
            if num_records > 0:
                raise ValueError(f"Data quality check failed. {table}.{column} contained {num_records} NULL values")
            self.log.info(f"Data quality on {table}.{column} check passed")
            
