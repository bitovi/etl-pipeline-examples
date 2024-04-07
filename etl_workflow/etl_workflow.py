from datetime import timedelta
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
        from activities import extract_data, transform_data, load_data

@workflow.defn
class ETLWorkflow:
    @workflow.run
    async def run(self):
        data = await workflow.execute_activity(
            extract_data,
            start_to_close_timeout=timedelta(seconds=15),
        )
        transformed_data = await workflow.execute_activity(
            transform_data,
            data,
            start_to_close_timeout=timedelta(seconds=15),
        )
        await workflow.execute_activity(
            load_data,
            transformed_data,
            start_to_close_timeout=timedelta(seconds=15),
        )
        return True
