import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop tables defined in a list of SQL queries using the provided database cursor and connection.

    Args:
        cur (psycopg2.extensions.cursor): The database cursor for executing SQL queries.
        conn (psycopg2.extensions.connection): The database connection object.

    Returns:
        None
    """
    for query in drop_table_queries:
        print(query, '\n')
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Create tables defined in a list of SQL queries using the provided database cursor and connection.

    Args:
        cur (psycopg2.extensions.cursor): The database cursor for executing SQL queries.
        conn (psycopg2.extensions.connection): The database connection object.

    Returns:
        None
    """
    for query in create_table_queries:
        print(query, '\n')
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function to set up an Amazon Redshift data warehouse by reading configuration, creating and dropping tables.

    This function performs the following steps:
    1. Reads database configuration from a configuration file (dwh-2.cfg).
    2. Establishes a connection to the Amazon Redshift cluster using the configuration parameters.
    3. Drops existing tables (if they exist) by calling the `drop_tables` function.
    4. Creates necessary tables by calling the `create_tables` function.
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()