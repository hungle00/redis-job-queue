# RQueue
A lightweight, reliable job queue implemented in Python using Redis Streams, Hash/KV, and Sorted Sets.

Modelled with a strong separation of concerns:
- **`JobQueue`**: Manages data persistence, status indexing, retry logic, and Redis communication.
- **`Worker`**: Serves as a pure execution engine handling process isolation, graceful shutdowns, signal handling, and retry/reclaim triggers

This project is inspired by the [RQ](https://python-rq.org/) - famous Python Task queue library.

## How to use

Install the CLI in editable mode and start Redis:

```bash
pip install -e .
docker compose up -d
```

In one terminal, start a worker listening to the default and `emails` queues:

```bash
rqueue worker --queues default,emails --name local-worker --threads 5
```

The default worker command listens to the `default` queue:

```bash
rqueue worker
```

Use these commands to inspect queue state and jobs:

```bash
rqueue list --status processing
rqueue job inspect <job-id>
rqueue failed list
rqueue delayed list
```

The `failed` and `delayed` lists accept an optional `--limit` value, for example `rqueue failed list --limit 10`.

In your producer application, enqueue work on the default queue or choose a named queue explicitly:

```python
import redis
from rqueue.job_queue import JobQueue

def send_email(data):
    print(f"Sending email to {data['to']}")

client = redis.Redis(host="localhost", port=6379, decode_responses=True)
jobs = JobQueue(redis_client=client)
data = {"to": "user@example.com", "subject": "Welcome!"}

jobs.enqueue(send_email, data)  # default queue
jobs.enqueue_to("emails", send_email, data)
```

Run the producer code in another terminal. To schedule work for later, use `enqueue_at` for the default queue or `enqueue_at_to` for a named queue:

```python
from datetime import datetime, timedelta

jobs.enqueue_at_to(
    "emails", datetime.now() + timedelta(minutes=2), send_email, data
)
```

Workers promote scheduled jobs when they become due and retry failed jobs with backoff. By default, jobs use the `default` queue and its existing `jobs` Redis stream; named queues use streams such as `jobs:emails`.

## Overall Flow
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
