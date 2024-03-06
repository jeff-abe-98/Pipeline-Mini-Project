import mysql.connector
import yaml
import logging
from sys import stdout
import csv
import datetime
from datetime import timedelta

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

    connection = None
    try:
        with open(secret_path, 'r') as file:
            secrets = yaml.safe_load(file)

        username = secrets['mysql_user']
        password = secrets['mysql_pass']

        connection = mysql.connector.connect(user=username,
                                             password=password,
                                             host='localhost',
                                             port='3306',
                                             database=database)
    except Exception as error:
        logging.warning('Error while connecting to database for job tracker', exc_info=error)
    
    return connection


def load_third_party(connection, file_path_csv):
    '''
    Function to load data from a csv

    Args:
        connection (object) : MySQLConnection Object
        file_path_csv (str) : Relative path to the csv file 

    Returns:
        None
    '''
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
    '''
    Function to return an ordered list of event names with the highest number of tickets sold
    
    Args:
        connection (object) : MySQLConnection Object 
    '''
    today = datetime.date.today()
    # Get the most popular ticket in the past month
    sql_statement = '''SELECT event_name
                        FROM ticket_sales
                        WHERE DATE_FORMAT(event_date,'%Y-%m-01') = DATE_ADD('{}-01', INTERVAL -1 MONTH)
                        GROUP BY event_name
                        ORDER BY SUM(num_tickets) DESC'''.format(today.strftime('%Y-%m'))
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    return records


if __name__ == '__main__':
    conn = get_db_connection('secrets.yml', 'pipeline_mini')
    load_third_party(conn, 'Data/third_party_sales_1.csv')
    results = query_popular_tickets(None)
    results_list = [row[0] for row in results]
    display = 'Here are the most popular tickets in the past month:'
    for item in results_list:
        display += f'\n  - {item}'
    print(display)