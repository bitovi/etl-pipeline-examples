import json
import time
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "me",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def aaa_etl_pipeline():
    @task()
    def extract_data():
        time.sleep(10)
        postgres_hook = PostgresHook(postgres_conn_id='source_db')

        get_all_products = "SELECT product_id, product_name, price FROM product order by product_id asc;"
        get_all_orders = "select order_id, products, total from \"order\" order by order_id asc;"

        connection = postgres_hook.get_conn()
        cursor = connection.cursor()

        cursor.execute(get_all_products)
        products = cursor.fetchall()

        cursor.execute(get_all_orders)
        orders = cursor.fetchall()

        return {'products': products, 'orders': orders}
    
    @task
    def foo_bar():
        print("This is the foo bar function!")

    @task()
    def transform_data(**kwargs):
        data = kwargs["task_instance"].xcom_pull(task_ids='extract_data')
        time.sleep(10)
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
            denormalized_order["total"] = 0

            order_products = order[1]
            for product_id in order_products:
                product_map = products_map[product_id]
                denormalized_order["products"].append(product_map)
                denormalized_order["total"] = (
                    denormalized_order["total"] + product_map["price"]
                )

            denormalized_orders.append(denormalized_order)

        return denormalized_orders
        # return { denormalized_orders: denormalized_orders }

    @task()
    def load_data(**kwargs):
        orders = kwargs["task_instance"].xcom_pull(task_ids='transform_data')
        # Switch to this to break compatibility with transform_data
        # data = kwargs["task_instance"].xcom_pull(task_ids='transform_data')
        # orders = data.denormalized_orders
        time.sleep(10)
        postgres_hook = PostgresHook(postgres_conn_id='data_warehouse_db')

        connection = postgres_hook.get_conn()
        cursor = connection.cursor()

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
            connection.close()

        return True
    # start with this and wait until load_data starts
    extract_data() >> transform_data() >> load_data() 
    # then switch to this
    # extract_data() >> foo_bar() >> transform_data() >> load_data()
    # switch back to see the task disappear from all executions in the UI

dag_run = aaa_etl_pipeline()
