# Instructions
# Define a function that uses the python logger to log a function. Then finish filling in the details of the DAG down below. Once you’ve done that, run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import pendulum
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


#
# TODO: Define a function for the PythonOperator to call and have it log something
#
# def my_function():
#     logging.info(<REPLACE>)


dag = DAG(
        'greet_flow_dag_legacy',
        start_date=pendulum.now())

#
# TODO: Uncomment the operator below and replace the arguments labeled <REPLACE> below
#

#greet_task = PythonOperator(
#    task_id="<REPLACE>",
#    python_callable=<REPLACE>,
#    dag=<REPLACE>
#)

