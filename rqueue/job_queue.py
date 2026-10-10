import redis
import uuid
import time
from datetime import datetime
from rqueue.job import Job, JobStatus
from rqueue.delayed_queue import DelayedQueue
from rqueue.dead_letter_queue import DeadLetterQueue

class JobQueue:
    STREAM = "jobs"
    GROUP = "workers"
    DEFAULT_QUEUE = "default"
    MAX_ATTEMPS = 3
    STATUS_PREFIX = "job-queue:status"
    JOB_PREFIX = "job-queue:job"

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.delayed_queue = DelayedQueue(redis_client)
        self.dead_letter_queue = DeadLetterQueue(redis_client)
        self._create_consumer_group(self.DEFAULT_QUEUE)

    def enqueue(self, func, *args, **kwargs):
        return self.enqueue_to(self.DEFAULT_QUEUE, func, *args, **kwargs)

    def enqueue_to(self, queue_name, /, func, *args, **kwargs):
        job = self._new_job(queue_name, func, *args, **kwargs)
        self._save_job(job)

        self.redis.sadd(self._status_key(job.status), job.id)

        message_id = self.redis.xadd(
            self._stream_for_queue(queue_name), {"job_id": job.id }
        )
        self.update_status(job, JobStatus.QUEUED)

        print(f"Enqueued job={job.id}, queue={queue_name}, message={message_id}")
        return job.id

    def enqueue_at(self, time_to_execute, func, *args, **kwargs):
        return self.enqueue_at_to(
            self.DEFAULT_QUEUE, time_to_execute, func, *args, **kwargs
        )

    def enqueue_at_to(self, queue_name, /, time_to_execute, func, *args, **kwargs):
        if isinstance(time_to_execute, datetime):
            execute_timestamp = time_to_execute.timestamp()
        else:
            execute_timestamp = float(time_to_execute)

        now = time.time()
        if execute_timestamp <= now:
            return self.enqueue_to(queue_name, func, *args, **kwargs)

        job = self._new_job(queue_name, func, *args, **kwargs)
        job.status = JobStatus.SCHEDULED
        self._save_job(job)
        self.redis.sadd(self._status_key(JobStatus.SCHEDULED), job.id)
        self.delayed_queue.push(job.id, execute_timestamp)

        print(f"Job {job.id} will be execute at {execute_timestamp}")
        return job.id

    def get_job(self, job_id):
        data = self.redis.get(self._job_key(job_id))
        if not data:
            return

        return Job.deserialize(data)

    def fetch_jobs(
        self, consumer_name: str, count: int = 1, block_ms: int = 2000, queues=None
    ):
        """
        Read messages from Redis Stream.
        Return list of tuple: [(message_id, job_id), ...]
        """
        queues = queues or [self.DEFAULT_QUEUE]
        streams = {}
        for queue_name in queues:
            self._create_consumer_group(queue_name)
            streams[self._stream_for_queue(queue_name)] = ">"

        messages = self.redis.xreadgroup(
            self.GROUP, consumer_name, streams, count=count, block=block_ms
        )
        return [
            (message_id, data["job_id"], self._queue_for_stream(stream))
            for stream, entries in messages
            for message_id, data in entries
        ]

    def retry_job(self, job, message_id, error=None, base_delay=10):
        attempts = job.attempts + 1
        job.attempts = attempts
        job.error = error or "max retries exceeded"

        if attempts >= self.MAX_ATTEMPS:
            self.update_status(job, JobStatus.DEAD_LETTER)
            self.dead_letter_queue.push(job.id, job.error, attempts)
            self.ack(message_id, job.queue_name)
            print(f"Job {job.id} moved to DLQ")
            return

        # Delayed retry using exponential backoff
        delay_seconds = base_delay * (2 ** (attempts - 1))
        execute_at = time.time() + delay_seconds

        self.update_status(job, JobStatus.RETRYING)
        # Saving delayed job into Redis ZSET.
        self.delayed_queue.push(job.id, execute_at)
        self.ack(message_id, job.queue_name)

        print(f"Retry job={job.id}, attempt={attempts}")

    def enqueue_scheduled_jobs(self):
        ready_jobs = self.delayed_queue.pop_ready_jobs()
        if not ready_jobs:
            return 0

        p = self.redis.pipeline()
        for job_id in ready_jobs:
            job = self.get_job(job_id)
            if job is None:
                continue

            self.update_status(job, JobStatus.QUEUED)

            p.xadd(self._stream_for_queue(job.queue_name), {"job_id": job_id})

        p.execute()
        return len(ready_jobs)

    def get_jobs_by_status(self, status):
        ids = self.redis.smembers(self._status_key(status))
        jobs = []
        for job_id in ids:
            job = self.get_job(job_id)
            if job is not None:
                jobs.append(job)
        return jobs

    def reclaim_stale(self, consumer_name: str, min_idle_time=10000, queues=None):
        """
        Reclaim messages that have been pending
        for longer than min_idle_time milliseconds.
        """
        queues = queues or [self.DEFAULT_QUEUE]
        reclaimed = []
        for queue_name in queues:
            self._create_consumer_group(queue_name)
            result = self.redis.xautoclaim(
                self._stream_for_queue(queue_name), self.GROUP, consumer_name,
                min_idle_time, "0-0", count=10,
            )
            messages = result[1]
            reclaimed.extend(
                (message_id, data["job_id"], queue_name)
                for message_id, data in messages
            )

        if reclaimed:
            print(f"Reclaimed {len(reclaimed)} stale messages")
        return reclaimed
   
    def ack(self, message_id: str, queue_name=DEFAULT_QUEUE):
        """Acknowledge that the worker has finished processing a message."""
        self.redis.xack(self._stream_for_queue(queue_name), self.GROUP, message_id)

    def update_status(self, job, new_status):
        old_status = job.status
        self.redis.srem(
            self._status_key(old_status), job.id,
        )
        self.redis.sadd(
            self._status_key(new_status), job.id,
        )
        job.status = new_status
        self._save_job(job)

    def _new_job(self, queue_name, /, func, *args, **kwargs):
        payload_data = {
            "func": func,
            "args": args,
            "kwargs": kwargs
        }
        job = Job(id=str(uuid.uuid4()), payload=payload_data, queue_name=queue_name)
        return job

    def _create_consumer_group(self, queue_name):
        try:
            self.redis.xgroup_create(
                self._stream_for_queue(queue_name), self.GROUP, id="0", mkstream=True,
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def _stream_for_queue(self, queue_name):
        if not queue_name or "," in queue_name:
            raise ValueError("queue_name must be a non-empty queue name")
        if queue_name == self.DEFAULT_QUEUE:
            return self.STREAM
        return f"{self.STREAM}:{queue_name}"

    def _queue_for_stream(self, stream):
        if isinstance(stream, bytes):
            stream = stream.decode()
        prefix = f"{self.STREAM}:"
        if stream == self.STREAM:
            return self.DEFAULT_QUEUE
        return stream[len(prefix):]

    def _job_key(self, job_id):
        return f"{self.JOB_PREFIX}:{job_id}"

    def _status_key(self, status):
        return f"{self.STATUS_PREFIX}:{status}"

    def _save_job(self, job):
        self.redis.set(
            self._job_key(job.id), job.serialize()
        )

