# Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import pendulum
import logging


from airflow.decorators import dag,task

from custom_operators.facts_calculator import FactsCalculatorOperator
from custom_operators.has_rows import HasRowsOperator
from custom_operators.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.empty import EmptyOperator


@dag(start_date=pendulum.now())
def prueba_chota():
    
    for i in range(10):
        @task
        def hello_world_task():
            logging.info("Hello World!")

        @task # Luego, en vez de ser python operators. Los transformaremos en custom operators.
        def bye_world_task():
            logging.info('Bye World!')


        # hello_world represents a discrete invocation of the hello_world_task
        hello_world=hello_world_task()
        bye_world=bye_world_task()

        hello_world >> bye_world


prueba_chota_dag = prueba_chota()