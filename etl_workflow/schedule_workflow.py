import asyncio
from datetime import timedelta

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleSpec,
    ScheduleState,
)

from etl_workflow import ETLWorkflow


async def main():
    client = await Client.connect("localhost:7233")
    await client.create_schedule(
        "etl-workflow-every-5-minutes",
        Schedule(
            action=ScheduleActionStartWorkflow(
                ETLWorkflow.run,
                id="etl-workflow",
                task_queue='etl-task-queue',
            ),
            spec=ScheduleSpec(
                intervals=[ScheduleIntervalSpec(every=timedelta(minutes=5))]
            ),
        ),
    )


if __name__ == "__main__":
    asyncio.run(main())

