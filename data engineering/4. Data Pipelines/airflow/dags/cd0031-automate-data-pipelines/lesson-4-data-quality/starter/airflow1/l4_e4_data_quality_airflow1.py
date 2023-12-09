#Instructions
#1 - Set an SLA on the max amount of time it should take to load trip data from s3 to redshift, say an hour
#2 - Add data verification step after the load trip data from s3 to redshift
#3 - Add data verification step after we load station data

# Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.


import pendulum
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from udacity.common import sql_statements


def load_trip_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    execution_date = kwargs["execution_date"]
    sql_stmt = sql_statements.COPY_MONTHLY_TRIPS_SQL.format(
        credentials.access_key,
        credentials.secret_key,
        year=execution_date.year,
        month=execution_date.month
    )
    redshift_hook.run(sql_stmt)


def load_station_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    sql_stmt = sql_statements.COPY_STATIONS_SQL.format(
        credentials.access_key,
        credentials.secret_key,
    )
    redshift_hook.run(sql_stmt)


def check_greater_than_zero(*args, **kwargs):
    table = kwargs["params"]["table"]
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
    if len(records) < 1 or len(records[0]) < 1:
        raise ValueError(f"Data quality check failed. {table} returned no results")

    logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")


dag = DAG(
    'data_quality_legacy',
    start_date=pendulum.now(),
    schedule_interval='@monthly',
    max_active_runs=1
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
    provide_context=True,
)

check_trips = PythonOperator(
    task_id='check_trips_data',
    dag=dag,
    python_callable=check_greater_than_zero,
    provide_context=True,
    params={
        'table': 'trips',
    }
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

check_stations = PythonOperator(
    task_id='check_stations_data',
    dag=dag,
    python_callable=check_greater_than_zero,
    provide_context=True,
    params={
        'table': 'stations',
    }
)

create_trips_table >> load_trips_task
create_stations_table >> load_stations_task

#
# TODO: Set the task dependencies for the stations and trips check tasks
#


