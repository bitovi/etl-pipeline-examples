import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
    def transform_data(orders, products):
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

    @task()
    def generate_sql(data):
        return '\n'.join(f"insert into orders values ({row['order_id']}, '{json.dumps(row)}');" for row in data)

    get_all_products = PostgresOperator(
        task_id="get_all_products",
        postgres_conn_id="source_db",
        sql="SELECT product_id, product_name, price FROM product order by product_id asc;",
    )

    get_all_orders = PostgresOperator(
        task_id="get_all_orders",
        postgres_conn_id="source_db",
        sql="select order_id, products, total from \"order\" order by order_id asc;",
    )

    transformed_data = transform_data(get_all_orders.output, get_all_products.output)

    insert_statement = generate_sql(transformed_data)

    PostgresOperator(
        task_id="load_all_orders",
        postgres_conn_id="data_warehouse_db",
        sql=insert_statement,
    )

dag_run = aaa_etl_pipeline()
