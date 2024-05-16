import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "me",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

list_of_items_to_execute_on=["one", "two", "three"]

@dag(
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def aaa_etl_pipeline():
    @task(
        do_xcom_push=True
    )
    def extract_data():
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

    @task()
    def transform_data(**kwargs):
        data = kwargs["task_instance"].xcom_pull(task_ids='extract_data')
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

        # return [] # Uncomment to trigger send_alert
        return denormalized_orders
    
    @task.branch()
    def branch(task_instance):
        try:
            result = task_instance.xcom_pull(task_ids="transform_data")
            if len(result) > 0:
                return ['load_data', 'send_alert']
            else:
                return 'send_alert'
        except Exception as error:
            # Stop execution if any errors happen
            return None


    @task()
    def load_data(**kwargs):
        orders = kwargs["task_instance"].xcom_pull(task_ids='transform_data')
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
    
    @task()
    def send_alert():
        print("Send email here!")

    @task()
    def foo(item):
        print(f"Something else could happen here! {item}")

    @task()
    def bar(item):
        print(f"Another thing could happen here! {item}")

    @task(
        trigger_rule=TriggerRule.ONE_FAILED
    )
    def notify_admin():
        print("Let an admin know something went wrong here")

    extract_data_task = extract_data()
    transform_data_task = transform_data()
    branch_task = branch()
    load_data_task = load_data()
    send_alert_task = send_alert()
    foo_tasks = foo.expand(item=list_of_items_to_execute_on)
    bar_tasks = [bar(i) for i in list_of_items_to_execute_on]

    extract_data_task >> transform_data_task >> branch_task
    branch_task >> [load_data_task, send_alert_task] >> foo_tasks
    foo_tasks >> bar_tasks >> notify_admin()

dag_run = aaa_etl_pipeline()
