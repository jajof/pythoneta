import configparser
import psycopg2
from sql_queries import count_rows_queries, analysis_queries

def number_of_rows(cur, conn):
    """
    Execute SQL queries to count the number of rows in database tables and print the results.

    This function iterates through a list of SQL queries (`count_rows_queries`) that count the number of rows in various
    database tables. It executes each query using the provided database cursor `cur`, prints the query, and commits the changes
    to the database connection `conn`. It then prints the query results.

    Args:
        cur (psycopg2.extensions.cursor): The database cursor for executing SQL queries.
        conn (psycopg2.extensions.connection): The database connection object.

    Returns:
        None
    """
    for query in count_rows_queries:
        print(query)
        cur.execute(query)
        # conn.commit()
        print(cur.fetchall())
        print()

def analysis(cur, conn):
    """
    Execute SQL queries to perform data analysis tasks and print the results along with associated questions.

    This function iterates through a list of tuples `analysis_queries`, each containing a question and its corresponding SQL
    query for data analysis. It executes each query using the provided database cursor `cur`, prints the question, the query,
    and the query results, and increments the index for each question.

    Args:
        cur (psycopg2.extensions.cursor): The database cursor for executing SQL queries.
        conn (psycopg2.extensions.connection): The database connection object.

    Returns:
        None
    """
    for i, (question, query) in enumerate(analysis_queries):
        cur.execute(query)
        #conn.commit()
        print(f'{i+1}. {question}')
        print(query)
        response = cur.fetchall()
        for row in response:
            print(row)
        print()
        

def main():
    """
    Main function to perform data analysis tasks on a data warehouse.

    This function performs the following steps:
    1. Reads database configuration from a configuration file (dwh-2.cfg).
    2. Establishes a connection to the Amazon Redshift cluster using the configuration parameters.
    3. Checks the number of rows in database tables by calling the `number_of_rows` function and prints the results.
    4. Performs data analysis tasks by calling the `analysis` function and prints progress.
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
    
    print('Checking the number of rows of each table...')
    number_of_rows(cur, conn)
    
    print('Solving some example questions...')
    analysis(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()