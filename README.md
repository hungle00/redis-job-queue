# Redis Job Queue
A simple job queue implemented with Python and Redis Streams.

## Overall Flow
```
producer.py
    │
    │ enqueue()
    ▼
 JobQueue
    │
    ▼
  Redis
    │
    │ XREADGROUP
    ▼
 worker.py
    │
    ├── process job
    ├── retry on failure
    ├── reclaim stale jobs
    └── dead-letter queue
```

The main idea is to keep the responsibilities separated:
- `producer.py` → create and enqueue jobs
- `job_queue.py` → queue abstraction, retry, recovery and DLQ
- `worker.py` → consume and process jobs

## Redis Data Model
- Stream → job queue and dead-letter queue
- String → store job data
- Set → index jobs by status

## Run Redis on Docker

Start Redis on Docker conainer
```
docker run --name ok-redis -d -p 6379:6379 redis
```
Or using docker compose
```
docker compose up
```
Access to redis CLI
```
docker exec -it ok-redis redis-cli
```
