import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from source files into staging tables in a data warehouse.

    This function iterates through a list of SQL queries (`copy_table_queries`) that specify the data loading process for
    staging tables. It executes each query using the provided database cursor `cur` and commits the changes to the database
    connection `conn`.

    Args:
        cur (psycopg2.extensions.cursor): The database cursor for executing SQL queries.
        conn (psycopg2.extensions.connection): The database connection object.

    Returns:
        None
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Execute and print SQL queries to insert data from staging tables into target tables in a data warehouse.

    This function iterates through a list of SQL queries (`insert_table_queries`) that specify the data insertion process from
    staging tables into target tables. It executes each query using the provided database cursor `cur`, prints the query for
    debugging purposes, and commits the changes to the database connection `conn`.

    Args:
        cur (psycopg2.extensions.cursor): The database cursor for executing SQL queries.
        conn (psycopg2.extensions.connection): The database connection object.

    Returns:
        None
    """
    for query in insert_table_queries:
        print(query, '\n')
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function to orchestrate the ETL process for a data warehouse.

    This function performs the following steps:
    1. Reads database configuration from a configuration file (dwh-2.cfg).
    2. Establishes a connection to the Amazon Redshift cluster using the configuration parameters.
    3. Loads data into staging tables by calling the `load_staging_tables` function and prints progress.
    4. Inserts data into definitive tables by calling the `insert_tables` function and prints progress.
    5. Closes the database connection.

    Args:
        None

    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('configs/dwh-2.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Loading data into staging tables...')
    load_staging_tables(cur, conn)
    
    print('Inserting data into definitive tables...')
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()