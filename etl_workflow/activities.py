import json
import os
import psycopg2

from temporalio import activity
from urllib.parse import urlparse

SOURCE_DB_URL = os.environ.get('SOURCE_DB_URL')
DATA_WAREHOUSE_DB_URL = os.environ.get('DATA_WAREHOUSE_DB_URL')

@activity.defn
async def extract_data():
    get_all_products = "SELECT product_id, product_name, price FROM product order by product_id asc;"
    get_all_orders = "select order_id, products, total from \"order\" order by order_id asc;"

    url = urlparse(SOURCE_DB_URL)

    conn = psycopg2.connect(
        dbname=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
    )

    cursor = conn.cursor()

    cursor.execute(get_all_products)
    products = cursor.fetchall()

    cursor.execute(get_all_orders)
    orders = cursor.fetchall()

    return {'products': products, 'orders': orders}

@activity.defn
async def transform_data(data):
	products = data['products']
	orders = data['orders']

	denormalized_orders = []

	products_map = {}
	for product in products:
		product_map = {}
		product_id = product[0]
		product_map["product_id"] = product_id
		product_map["product_name"] = product[1]
		product_map["price"] = product[2]

		products_map[product_id] = product_map

	for order in orders:
		denormalized_order = {}
		denormalized_order["order_id"] = order[0]
		denormalized_order["products"] = []
		denormalized_order["price"] = 0

		order_products = order[1]
		for product_id in order_products:
			product_map = products_map[product_id]
			denormalized_order["products"].append(product_map)
			denormalized_order["price"] = (
				denormalized_order["price"] + product_map["price"]
			)

		denormalized_orders.append(denormalized_order)

	return denormalized_orders

@activity.defn
async def load_data(orders):
    url = urlparse(DATA_WAREHOUSE_DB_URL)

    conn = psycopg2.connect(
        dbname=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
    )

    cursor = conn.cursor()

    insert_statement = '\n'.join(f"insert into orders values ({order['order_id']}, '{json.dumps(order)}');" for order in orders)

    try:
        cursor.execute("BEGIN;")

        cursor.execute(insert_statement)

        cursor.execute("COMMIT;")
    except Exception as e:
        cursor.execute("ROLLBACK;")
        raise e
    finally:
        cursor.close()
        conn.close()

    return True
