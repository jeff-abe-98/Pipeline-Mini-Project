import mysql.connector
import yaml
import logging
from sys import stdout
import csv
import datetime
from datetime import timedelta

logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(funcName)s() - %(levelname)s - %(message)s')
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
        logger.info('Connection with database established')
    except Exception as error:
        logger.warning('Error while connecting to database for job tracker', exc_info=error)
    
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
    cursor.execute('SELECT ticket_id FROM ticket_sales')
    keys = {str(row[0]) for row in cursor.fetchall()}
    rows_loaded = 0
    with open(file_path_csv, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if row[0] in keys:
                continue
            query = ('''
            INSERT INTO ticket_sales (ticket_id, trans_date, event_id, event_name, event_date, event_type, event_city, customer_id, price,num_tickets)
            VALUES(%s , '%s', %s, '%s' ,'%s' ,'%s' ,'%s' ,%s ,%s ,%s)
            ''') %tuple(row)
            try:
                cursor.execute(query)
                rows_loaded +=1
            except Exception as e:
                logger.error('An error occurred while inserting the row', exc_info=e)
    logger.info(f'{rows_loaded} rows loaded into table ticket_sales')

    connection.commit()
    cursor.close()
    return

def query_popular_tickets(connection):
    '''
    Function to return an ordered list of event names with the highest number of tickets sold
    
    Args:
        connection (object) : MySQLConnection Object 
    '''
    #today = datetime.date.today()
    today = datetime.date(2020,9,15)
    # Get the most popular ticket in the past month
    sql_statement = '''SELECT event_name
                        FROM ticket_sales
                        WHERE DATE_FORMAT(trans_date,'%Y-%m-01') = DATE_ADD('{}-01', INTERVAL -1 MONTH)
                        GROUP BY event_name
                        ORDER BY SUM(num_tickets) DESC
                        LIMIT 3'''.format(today.strftime('%Y-%m'))
    
    cursor = connection.cursor()
    try:
        cursor.execute(sql_statement)
        records = cursor.fetchall()
        cursor.close()
        logger.info('Query executed')
    except Exception as e:
        logger.error('Query execution failed', exc_info=e)
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