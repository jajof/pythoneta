from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {destination_table}
        FROM '{origin_s3}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        json 'auto ignorecase'
    """

    @apply_defaults
    def __init__(self,
                destination_table = '',
                redshift_conn_id = '',
                aws_credentials_id='',
                s3_bucket='',
                s3_key='',
                delete_insert=False,
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.destination_table = destination_table
        self.redshift_conn_id = redshift_conn_id,
        self.aws_credentials_id= aws_credentials_id
        self.s3_bucket= s3_bucket
        self.s3_key= s3_key
        self.delete_insert = delete_insert

    def execute(self, context):
        # get the credentials to connect to AWS
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # We build the S3 path to the file that matches the execution date.
        # Just for staging_events.
        if self.destination_table == 'staging_events':
            year = context['logical_date'].year
            month = context['logical_date'].month
            day = context['logical_date'].day
            # the dates between 1 and 9 must have a 0 at the left (1 -> 01) to match de path in S3
            if day <= 9:
                day = '0' + str(day)
            else:
                day = str(day)
            s3_key_formatted = self.s3_key.format(year=year, month=month, day=day)
        else:
            s3_key_formatted = self.s3_key
        # Final S3_path
        s3_path = f's3://{self.s3_bucket}/{s3_key_formatted}'
        self.log.info(f'Getting data from {s3_path}')
        # We format the SQL template.
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            destination_table=self.destination_table,
            origin_s3 = s3_path,
            access_key = aws_connection.login,
            secret_key = aws_connection.password,)

        if self.destination_table == 'staging_songs' and context['logical_date'].day > 1:
            # the songs table must only be filled the first day.
            self.log.info(f"Table {self.destination_table} is already filled. No need to reprocess")
        else:
            if self.delete_insert:
                self.log.info(f"Clearing data from {self.destination_table} Redshift table")
                redshift_hook.run("DELETE FROM {}".format(self.destination_table))
            redshift_hook.run(formatted_sql)