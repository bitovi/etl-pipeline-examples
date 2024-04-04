
# ETL Pipeline Examples

## Starting application

```
docker compose -f docker-compose.yml -f docker-compose-airflow.yml up --build -d
```

## Restarting the worker (after a code change)

```
docker compose -f docker-compose.yml -f docker-compose-airflow.yml restart airflow-worker
```

## Shutting down appliction

```
docker compose -f docker-compose.yml -f docker-compose-airflow.yml down -v --remove-orphans
```
