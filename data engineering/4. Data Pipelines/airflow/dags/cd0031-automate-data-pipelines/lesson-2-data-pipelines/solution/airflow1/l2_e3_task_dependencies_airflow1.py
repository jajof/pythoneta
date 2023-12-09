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

addition_task = PythonOperator(
    task_id="addition",
    python_callable=addition,
    dag=dag)

subtraction_task = PythonOperator(
    task_id="subtraction",
    python_callable=subtraction,
    dag=dag)

division_task = PythonOperator(
    task_id="division",
    python_callable=division,
    dag=dag)

# Configure Task Dependencies
# Notice, addition and subtraction can run at the same time
hello_world_task >> addition_task
hello_world_task >> subtraction_task

# Notice division waits for subtraction and addition to run
subtraction_task >> division_task
addition_task >> division_task
