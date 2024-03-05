import mysql.connector
import yaml
import pathlib
import logging
from sys import stdout
import csv
import datetime

logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(funcName)s() - %(levelname)s - %(message)s')
handler = logging.StreamHandler(stdout)
logger.setLevel(logging.INFO)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger.addHandler(handler)


def get_db_connection(secret_path, database):
    '''
    Function that gets a connection to the mysql database

    Args:
        secret_path (str) : A string of the path to the secrets yml file
        database (str)    : Database name to connect to

    Returns:
        MySQLConnection object: Connection to the local database specified
    '''
    dir = pathlib.Path.cwd()
    config = dir / secret_path

    connection = None
    with config.open('r') as file:
        secrets = yaml.safe_load(file)

    username = secrets['mysql_user']
    password = secrets['mysql_pass']

    try:
        connection = mysql.connector.connect(user=username,
                                             password=password,
                                             host='localhost',
                                             port='3306',
                                             database=database)
    except Exception as error:
        logging.warning('Error while connecting to database for job tracker', exc_info=error)
    
    return connection


def load_third_party(connection, file_path_csv):
    cursor = connection.cursor()
    
    with open(file_path_csv, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            query = ('''INSERT INTO ticket_sales (ticket_id, trans_date, event_id, event_name, event_date, event_type, event_city, customer_id, price, num_tickets)
            VALUES(%s, '%s', %s, '%s', '%s', '%s', '%s', %s, %s, %s)''') %tuple(row)
            try:
                cursor.execute(query)
            except Exception as e:
                logger.error('An error occurred while inserting the row', exc_info=e)

    connection.commit()
    cursor.close()
    return

def query_popular_tickets(connection):
    today = datetime.date.today()
    # Get the most popular ticket in the past month
    sql_statement = '''
    SELECT event_name
    FROM ticket_sales
    WHERE DATE_FORMAT(event_date,'%Y-%m-01') = DATE('{}-01')
    ORDER BY num_tickets DESC
    LIMIT 3
    '''.format(today.strftime('%Y-%m'))
    logger.info(sql_statement)
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    return records


if __name__ == '__main__':
    conn = get_db_connection('secrets.yml', 'pipeline_mini')
    load_third_party(conn, 'Data/third_party_sales_1.csv')
    results = query_popular_tickets(conn)
    results_tuple = tuple(row[0] for row in results)
    print(results_tuple)