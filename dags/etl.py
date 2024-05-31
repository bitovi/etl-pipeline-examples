import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import TaskInstance

default_args = {
    "owner": "me",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def etl_pipeline():
    @task()
    def hold_funds(**kwargs):
        print("Collecting funds")
        # Implementation for getting funds
        pass

    @task()
    def revert_hold_funds(**kwargs):
        dag_instance = kwargs['dag']
        execution_date = kwargs['execution_date']
        operator_instance = dag_instance.get_task("hold_funds")
        previous_task_state = TaskInstance(operator_instance, execution_date).current_state()
        print(f"previous_task_state: {previous_task_state}")
        if previous_task_state == 'success':
            print("Returning funds")
            # Implementation for returning funds

    @task()
    def hold_inventory(**kwargs):
        print("Hold inventory")
        # Implementation for holding inventory
        pass

    @task()
    def revert_hold_inventory(**kwargs):
        dag_instance = kwargs['dag']
        execution_date = kwargs['execution_date']
        operator_instance = dag_instance.get_task("hold_inventory")
        previous_task_state = TaskInstance(operator_instance, execution_date).current_state()
        if previous_task_state == 'success':
            print("Reverting inventory")
            # Implementation for returning inventory

    @task(
        retries=1,
        retry_delay=timedelta(seconds=10),
    )
    def create_order(**kwargs):
        print("Creating order")
        # Implementation for creating order record

        # raise Exception("Oh no!") # Uncomment to see the rollback happen
        pass

    @task()
    def revert_create_order(**kwargs):
        dag_instance = kwargs['dag']
        execution_date = kwargs['execution_date']
        operator_instance = dag_instance.get_task("create_order")
        previous_task_state = TaskInstance(operator_instance, execution_date).current_state()
        if previous_task_state == 'success':
            print("Reverting order")
            # Implementation for reverting order record


    hold_funds() >> hold_inventory() >> create_order() >> [revert_hold_funds().as_teardown(), revert_hold_inventory().as_teardown(), revert_create_order().as_teardown()]

dag_run = etl_pipeline()
