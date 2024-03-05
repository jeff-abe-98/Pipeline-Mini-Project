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
            query = ('''
            INSERT INTO ticket_sales
            WITH check_row AS (
                SELECT 
                    %s AS ticket_id, 
                    '%s' AS trans_date, 
                    %s AS event_id, 
                    '%s' AS event_name, 
                    '%s' AS event_date, 
                    '%s' AS event_type, 
                    '%s' AS event_city, 
                    %s AS customer_id,
                    %s AS price,
                    %s AS num_tickets
                )
            SELECT * FROM check_row t1
                WHERE NOT EXISTS(
                     SELECT 1 FROM ticket_sales t2 WHERE t1.ticket_id = t2.ticket_id
                )
            ''') %tuple(row)
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
    WITH popular_events AS(
    SELECT event_id
    FROM ticket_sales
    WHERE DATE_FORMAT(event_date,'%Y-%m-01') = DATE('{}-01')
    GROUP BY event_id
    ORDER BY SUM(num_tickets) DESC)

    SELECT event_name FROM ticket_sales WHERE event_id IN (SELECT * FROM popular_events)
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
    results_list = [row[0] for row in results]
    display = 'Here are the most popular tickets in the past month:'
    for item in results_list:
        display += f'\n  - {item}'
    print(display)