from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements
from airflow.secrets.metastore import MetastoreBackend


default_args = {
    'owner': 'jh',
    'depends_on_past':True ,
    'wait_for_downstream':True,
    'schedule_interval':'@hourly',
    'start_date': pendulum.datetime(2018, 11, 2, 0, 0, 0, 0),
    'end_date': pendulum.datetime(2018, 11, 30, 0, 0, 0, 0),
    'retries':1,
    'retry_delay': timedelta(seconds=10),
    'catchup':True,
    'max_active_runs':1    
}

@dag(
    # schedule_interval='@hourly',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow')
 
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    # Load events data
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        destination_table = 'staging_events',
        redshift_conn_id = 'redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='jh-bucket',
        s3_key='log-data/{year}/{month}/{year}-{month}-{day}-events.json',
        delete_insert = False
    )

    # Load songs data.
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        destination_table = 'staging_songs',
        redshift_conn_id = 'redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='jh-bucket',
        s3_key='song-data',
        delete_insert = False
    )

    # Build songplays table
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id = 'redshift',
        sql_query = final_project_sql_statements.SqlQueries.songplay_table_insert,
        destination_table = 'fact_songplay',
        delete_insert = False)

    # Load user table
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id = 'redshift',
        sql_query = final_project_sql_statements.SqlQueries.user_table_insert,
        destination_table = 'dim_users',
        delete_insert = False)

    # Load song table
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id = 'redshift',
        sql_query = final_project_sql_statements.SqlQueries.song_table_insert,
        destination_table = 'dim_songs',
        delete_insert = False)

    # Load artists table
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id = 'redshift',
        sql_query = final_project_sql_statements.SqlQueries.artist_table_insert,
        destination_table = 'dim_artists',
        delete_insert = False)

    # Load time table
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id = 'redshift',
        sql_query = final_project_sql_statements.SqlQueries.time_table_insert,
        destination_table = 'dim_time',
        delete_insert = False)

    # Check data quality
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id = 'redshift',
        tables_with_rows = ['staging_events',
                            'staging_songs',
                            'fact_songplay',
                            'dim_users',
                            'dim_songs',
                            'dim_artists',
                            'dim_time'],
        columns_without_nulls = [('dim_artists', 'name'),
                                 ('fact_songplay', 'session_id'),
                                 ('dim_users', 'user_id')], # list of tuples (table, column)
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> stage_events_to_redshift >> load_songplays_table
    start_operator >> stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_user_dimension_table  >> run_quality_checks
    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator
    
final_project_dag = final_project()