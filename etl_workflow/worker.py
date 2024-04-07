import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from etl_workflow import ETLWorkflow
from activities import extract_data, transform_data, load_data

async def main():
    client = await Client.connect("temporal-dev-server:7233")
    worker = Worker(
        client,
        task_queue='etl-task-queue',
        workflows=[ETLWorkflow],
        activities=[extract_data, transform_data, load_data],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
