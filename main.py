import mysql.connector
import yaml
import pathlib
import logging
from sys import stdout

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
