# Instructions
# Complete the TODOs in this DAG so that it runs once a day. Once you’ve done that, run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn the previous exercise off, then turn this exercise on. Wait a moment and refresh the UI to see Airflow automatically run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import pendulum
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def hello_world():
    logging.info("Hello World")

#
# TODO: Add a daily `schedule_interval` argument to the following DAG
#
# dag is the instance of the DAG, with a start date of now, and a schedule to run daily
dag = DAG(
        "greet_flow_dag_legacy",
        start_date=pendulum.now()
)

# 
# task is the only task in this DAG, so it will run by itself, without any sequence before or after another task
# it is returned from the PythonOperator
# note task is not a function, and cannot be invoked as a function, but will be run by the DAG automatically
task = PythonOperator(
        task_id="hello_world_task",
        python_callable=hello_world,
        dag=dag)
