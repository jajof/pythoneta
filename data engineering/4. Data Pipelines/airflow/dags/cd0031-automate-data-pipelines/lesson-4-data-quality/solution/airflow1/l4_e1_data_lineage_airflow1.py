import pendulum

from airflow import DAG
from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from udacity.common import sql_statements


def load_trip_data_to_redshift(*args, **kwargs):
    metastoreBackend = MetastoreBackend()

    redshift_hook = PostgresHook("redshift")
    aws_connection=metastoreBackend.get_connection("aws_credentials")

    sql_stmt = sql_statements.COPY_ALL_TRIPS_SQL.format(
        aws_connection.login,
        aws_connection.password,
    )
    redshift_hook.run(sql_stmt)


def load_station_data_to_redshift(*args, **kwargs):
    metastoreBackend = MetastoreBackend()

    redshift_hook = PostgresHook("redshift")
    aws_connection=metastoreBackend.get_connection("aws_credentials")
    sql_stmt = sql_statements.COPY_STATIONS_SQL.format(
        aws_connection.login,
        aws_connection.password,
    )
    redshift_hook.run(sql_stmt)


dag = DAG(
    'data_pipeline_schedules_legacy',
    start_date=pendulum.now()
)

create_trips_table = PostgresOperator(
    task_id="create_trips_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL
)

load_trips_task = PythonOperator(
    task_id='load_trips_from_s3_to_redshift',
    dag=dag,
    python_callable=load_trip_data_to_redshift,
)

create_stations_table = PostgresOperator(
    task_id="create_stations_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
)

load_stations_task = PythonOperator(
    task_id='load_stations_from_s3_to_redshift',
    dag=dag,
    python_callable=load_station_data_to_redshift,
)

calculate_traffic_task = PostgresOperator(
    task_id='calculate_location_traffic',
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.LOCATION_TRAFFIC_SQL,
)

create_trips_table >> load_trips_task >> calculate_traffic_task

