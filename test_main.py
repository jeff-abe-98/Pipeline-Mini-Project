from main import get_db_connection, load_third_party, query_popular_tickets
from mysql.connector import MySQLConnection
import pytest

@pytest.fixture(scope='session', autouse=True)
def init():
    conn = get_db_connection('secrets.yml', 'pipeline_mini')
    load_third_party(conn, 'Data/third_party_sales_1.csv')

def test_get_db_connection():
    assert isinstance(get_db_connection('secrets.yml', 'pipeline_mini'), MySQLConnection)
    assert get_db_connection('secret.yml', 'pipeline_mini') == None
    assert get_db_connection('secrets.yml', 'pipeline_min') == None


def test_query_popular_tickets():
    conn = get_db_connection('secrets.yml', 'pipeline_mini')
    assert query_popular_tickets(conn) == [('Washington Spirits vs Sky Blue FC',), ('Christmas Spectacular',), ('The North American International Auto Show',)]
    with pytest.raises(Exception) as exc_info:
        query_popular_tickets(None)
    assert exc_info.value.args[0] == "'NoneType' object has no attribute 'cursor'"
