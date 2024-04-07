import asyncio

from temporalio.client import Client

from etl_workflow import ETLWorkflow


async def main():
    client = await Client.connect("localhost:7233")
    await client.execute_workflow(
        ETLWorkflow.run,
        id="etl-workflow",
        task_queue='etl-task-queue',
    )


if __name__ == "__main__":
    asyncio.run(main())

