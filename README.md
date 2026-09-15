# Redis Streams Queue
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
  │
  ├──────────────► worker.py
                     │
                     └── process message
                     │
                     ├── retry
                     └── dead-letter queue
```

The main idea is to keep the responsibilities separated:
- `producer.py` → enqueue job
- `job_queue.py` → queue abstraction
- `worker.py` → process jobs

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
