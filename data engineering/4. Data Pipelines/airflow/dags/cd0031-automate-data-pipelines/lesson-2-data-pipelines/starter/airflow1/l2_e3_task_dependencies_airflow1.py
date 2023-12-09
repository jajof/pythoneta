# Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import pendulum
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def hello_world():
    logging.info("Hello World")


def addition():
    logging.info(f"2 + 2 = {2+2}")


def subtraction():
    logging.info(f"6 -2 = {6-2}")


def division():
    logging.info(f"10 / 2 = {int(10/2)}")


dag = DAG(
    "task_dependencies_dag",
    schedule_interval='@hourly',
    start_date=pendulum.now()
)
hello_world_task = PythonOperator(
    task_id="hello_world",
    python_callable=hello_world,
    dag=dag)

#
# TODO: Define an addition task that calls the `addition` function above
#

#
# TODO: Define a subtraction task that calls the `subtraction` function above
#

#
# TODO: Define a division task that calls the `division` function above
#

#
# TODO: Configure the task dependencies such that the graph looks like the following:
#
#                    ->  addition_task
#                   /                 \
#   hello_world_task                   -> division_task
#                   \                 /
#                    ->subtraction_task
