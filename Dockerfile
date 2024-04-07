FROM python:3.9-slim

WORKDIR /app

COPY ./etl_workflow /app

RUN pip install --no-cache-dir asyncio \
  && pip install temporalio

CMD ["python", "etl_workflow/worker.py"]

