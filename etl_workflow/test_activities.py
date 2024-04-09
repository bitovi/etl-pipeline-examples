import pytest
from activities import extract_data
from test_static_data import PRODUCTS, ORDERS

@pytest.fixture
def mock_psycopg2(mocker):
    fetchall_responses = [
        PRODUCTS,
        ORDERS
    ]

    mock_cursor = mocker.Mock()

    mock_cursor.fetchall.side_effect = fetchall_responses

    mock_conn = mocker.Mock()
    mock_conn.cursor.return_value = mock_cursor

    mock_connect = mocker.patch('psycopg2.connect')
    mock_connect.return_value = mock_conn

    return mock_connect

@pytest.mark.asyncio
async def test_extract_data(mock_psycopg2):
    data = await extract_data()

    mock_psycopg2.return_value.cursor.assert_called_with()
    assert mock_psycopg2.return_value.cursor.return_value.execute.call_count == 2
    mock_psycopg2.return_value.cursor.return_value.execute.assert_any_call("select product_id, product_name, price from product order by product_id asc;")
    mock_psycopg2.return_value.cursor.return_value.execute.assert_any_call("select order_id, products, total from \"order\" order by order_id asc;")

    assert data == {'products': PRODUCTS, 'orders': ORDERS}
