# Redis Job Queue
A lightweight, reliable job queue implemented in Python using Redis Streams, Hash/KV, and Sorted Sets.

Modelled with a strong separation of concerns:
- **`JobQueue`**: Manages data persistence, status indexing, retry logic, and Redis communication.
- **`Worker`**: Serves as a pure execution engine handling process isolation, graceful shutdowns, signal handling, and retry/reclaim triggers

## Overall Flow
```
```text
producer.py
    │
    │ queue.enqueue()
    ▼
 JobQueue
    │
    ▼
  Redis ─── [ Stream: jobs ]
               │
               │ fetch_jobs() / XREADGROUP
               ▼
           worker.py
               │
               ├── process job (via os.fork)
               │     ├── Success ──► queue.ack() ──► update_status('completed')
               │     └── Failure ──► queue.retry_job()
               │                         ├── Exceeds MAX_ATTEMPTS ──► [ DLQ Stream: jobs:dead ]
               │                         └── Backoff Delay ────────► [ ZSET: job-queue:delayed ]
               │                                                            │
               │   
               │   ▼ (periodic enqueue_scheduled_jobs)
               ├── 
               └── Promote delayed jobs back to Stream
```

## Redis Data Model
- Stream → Main queue for job processing and dead-letter queue
- String → Stores full job metadata, payload, attempt counts, errors,... as JSON.
- Set → Indexes job IDs by their current lifecycle state (queued, processing, completed, retrying, dead_letter).
- Sorted Set → Manages exponential backoff retries using Unix timestamps as scores.

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
