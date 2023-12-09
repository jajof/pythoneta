# Instructions
# Use the Airflow context in the pythonoperator to complete the TODOs below. Once you are done, run your DAG and check the logs to see the context in use.

# Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import pendulum
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook


def log_details(*args, **kwargs):
    #
    # TODO: Extract ds, run_id, prev_ds, and next_ds from the kwargs, and log them
    # NOTE: Look here for context variables passed in on kwargs:
    #       https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html
    #
    ds = '' # kwargs[]
    run_id = '' # kwargs[]
    previous_ds = '' # kwargs.get('')
    next_ds = '' # kwargs.get('')

    logging.info(f"Execution date is {ds}")
    logging.info(f"My run id is {run_id}")
    if previous_ds:
        logging.info(f"My previous run was on {previous_ds}")
    if next_ds:
        logging.info(f"My next run will be {next_ds}")

dag = DAG(
    'log_details_dag_legacy',
    schedule_interval="@daily",
    start_date=pendulum.now()
)

list_task = PythonOperator(
    task_id="log_details",
    python_callable=log_details,
    provide_context=True,
    dag=dag
)
