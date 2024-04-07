
# ETL Pipeline Examples

## Starting application

```
docker compose -f docker-compose.yml -f docker-compose-temporal.yml up --build -d
```

## Restarting the worker (after a code change)

```
docker compose -f docker-compose.yml -f docker-compose-temporal.yml restart temporal-worker
```

## Shutting down appliction

```
docker compose -f docker-compose.yml -f docker-compose-temporal.yml down -v --remove-orphans
```
