# Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import datetime

from airflow import DAG

from airflow.operators.postgres_operator import PostgresOperator
from custom_operators.s3_to_redshift import S3ToRedshiftOperator
from custom_operators.has_rows import HasRowsOperator

#
# TODO: Create a DAG which performs the following functions:
#
#       1. Loads Trip data from S3 to RedShift
#       2. Performs a data quality check on the Trips table in RedShift
#       3. Uses the FactsCalculatorOperator to create a Facts table in Redshift
#           a. **NOTE**: to complete this step you must complete the FactsCalcuatorOperator
#              skeleton defined in plugins/operators/facts_calculator.py
#
dag = DAG("build_a_full_dag_legacy", start_date=datetime.datetime.utcnow())

#
# TODO: Load trips data from S3 to RedShift. Use the s3_key
#       "data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
#       and YOUR s3 bucket
#
#copy_trips_task = S3ToRedshiftOperator(...)

#
# TODO: Perform a data quality check on the Trips table
#
#check_trips = HasRowsOperator(...)

#
# TODO: Use the FactsCalculatorOperator to create a Facts table in RedShift. The fact column should
#       be `tripduration` and the groupby_column should be `bikeid`
#
#calculate_facts = FactsCalculatorOperator(...)

#
# TODO: Define task ordering for the DAG tasks you defined
#
