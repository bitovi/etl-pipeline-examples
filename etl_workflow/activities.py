from temporalio import activity

@activity.defn
async def extract_data():
    return False

@activity.defn
async def transform_data(data):
    return True

@activity.defn
async def load_data(orders):
    return True
