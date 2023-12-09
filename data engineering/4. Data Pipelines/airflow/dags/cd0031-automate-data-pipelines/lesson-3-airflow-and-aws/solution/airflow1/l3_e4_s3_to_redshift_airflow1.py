import datetime
import logging

from airflow import DAG
from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from udacity.common import sql_statements


def load_data_to_redshift(*args, **kwargs):
    metastoreBackend = MetastoreBackend()
    aws_connection=metastoreBackend.get_connection("aws_credentials")
    logging.info(vars(aws_connection))
    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(sql_statements.COPY_ALL_TRIPS_SQL.format(aws_connection.login, aws_connection.password))


dag = DAG(
    's3_to_redshift_legacy',
    start_date=datetime.datetime.now()
)

create_table_task = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL
)

load_task = PythonOperator(
    task_id='load_from_s3_to_redshift',
    dag=dag,
    python_callable=load_data_to_redshift
)

location_traffic_task = PostgresOperator(
    task_id="calculate_location_traffic",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.LOCATION_TRAFFIC_SQL
)

create_table_task >> load_task
load_task >> location_traffic_task
