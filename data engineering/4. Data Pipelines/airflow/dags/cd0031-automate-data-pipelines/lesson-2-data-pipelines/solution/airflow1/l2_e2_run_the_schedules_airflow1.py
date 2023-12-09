import pendulum
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def hello_world():
    logging.info("Hello World")

# dag is the instance of the DAG
# with a start date of now
# and a schedule to run daily once enabled in Airflow
dag = DAG(
        "greet_flow_dag_legacy",
        start_date=pendulum.now(),
        schedule_interval='@daily')

# 
# task is the only task in this DAG, so it will run by itself, without any sequence before or after another task
# it is returned from the PythonOperator
# note task is not a function, and cannot be invoked as a function, but will be run by the DAG automatically
task = PythonOperator(
        task_id="hello_world_task",
        python_callable=hello_world,
        dag=dag)
