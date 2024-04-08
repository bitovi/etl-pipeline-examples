import pytest
import uuid
from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.testing import WorkflowEnvironment

# Import your Activity Definition and real implementation
from etl_workflow import ETLWorkflow
from activities import extract_data, transform_data, load_data

PRODUCTS = [
    [1, 'product 1', 1],
    [2, 'product 2', 2],
    [3, 'product 3', 3],
    [4, 'product 4', 4],
    [5, 'product 5', 5],
]
ORDERS = [
    [1, [1, 2], 3],
    [2, [3, 4, 5], 0],
]
EXTRACTED_DATA = { 'products': PRODUCTS, 'orders': ORDERS }
TRANSFORMED_ORDERS = [
    {
        'order_id': 1,
        'products': [
            {'product_id': 1, 'product_name': 'product 1', 'price': 1},
            {'product_id': 2, 'product_name': 'product 2', 'price': 2},
        ],
        'total': 3
    },
    {
        'order_id': 1,
        'products': [
            {'product_id': 3, 'product_name': 'product 3', 'price': 3},
            {'product_id': 4, 'product_name': 'product 4', 'price': 4},
            {'product_id': 5, 'product_name': 'product 5', 'price': 5},
        ],
        'total': 12
    }
]

# Define your mocked Activity implementations
@activity.defn(name='extract_data')
async def extract_data_mocked():
    return EXTRACTED_DATA

@activity.defn(name='transform_data')
async def transform_data_mocked(data):
    assert data == EXTRACTED_DATA
    return TRANSFORMED_ORDERS

@activity.defn(name='load_data')
async def load_data_mocked(orders):
    assert orders == TRANSFORMED_ORDERS
    return True

@pytest.mark.asyncio
async def test_etl_workflow():
    task_queue_name = str(uuid.uuid4())

    # Create the test environment
    async with await WorkflowEnvironment.start_local() as env:

        # Provide the mocked Activity implementation to the Worker
        async with Worker(
            env.client,
            task_queue=task_queue_name,
            workflows=[ETLWorkflow],
            activities=[extract_data_mocked, transform_data_mocked, load_data_mocked],
        ):
            # Execute your Workflow as usual
            assert True == await env.client.execute_workflow(
                ETLWorkflow.run,
                id=str(uuid.uuid4()),
                task_queue=task_queue_name,
            )
