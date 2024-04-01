
# ETL Pipeline Examples

## Starting application

```
docker compose -f docker-compose.yml -f docker-compose-airflow.yml up --build -d
```

## Restarting the worker (after a code change)

```
docker compose -f docker-compose.yml -f docker-compose-airflow.yml restart etl-pipeline-examples-airflow-worker-1
```

## Shutting down appliction

```
docker compose down -v
```
