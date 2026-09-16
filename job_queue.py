import redis
import json
import time
from job import Job, JobStatus

class JobQueue:
    STREAM = "jobs"
    GROUP = "workers"
    DL_STREAM = "jobs:dead"
    MAX_ATTEMPS = 3
    STATUS_PREFIX = "job-queue:status"
    JOB_PREFIX = "job-queue:job"

    def __init__(self, redis_client: redis.Redis, consumer_name: str):
        self.redis = redis_client
        self.consumer = consumer_name

    def enqueue(self, job):
        self._save_job(job)

        self.redis.sadd(
            self._status_key(job.status), job.id
        )

        message_id = self.redis.xadd(
            self.STREAM, {"job_id": job.id }
        )

        print(f"Enqueued job={job.id}, message={message_id}")
        return message_id

    def get_job(self, job_id):
        data = self.redis.get(self._job_key(job_id))
        if not data:
            return

        return Job(**json.loads(data)) 

    def worker(self, process_job):
        # process_job is function that consumer want to process
        self._create_consumer_group()

        while(True):
            messages = self.redis.xreadgroup(
                self.GROUP, self.consumer, {self.STREAM: '>'}, count=1, block=5000
            )

            if not messages:
                print("No new messages. Waiting...")
                continue

            for _, entries in messages:
                for message_id, data in entries:
                    self._process_message(message_id, data, process_job)

    def _process_message(self, message_id, data, process_job):
        job_id = data["job_id"]
        job = self.get_job(job_id)

        if not job:
            print(f"Job {job_id} not found")
            self.redis.xack(
                self.STREAM, self.GROUP, message_id
            )
            return

        self.update_status(job, JobStatus.PROCESSING)

        try:
            process_job(job.id, job.payload)

            self.update_status(job, JobStatus.COMPLETED)

            self.redis.xack(
                self.STREAM, self.GROUP, message_id
            )
            print(f"ACK job={job_id} with message={message_id}")

        except Exception as e:
            print(f"Job {job_id} failed: {e}")
            self.retry_job(
                job.id, message_id, error=str(e),
            )

    def retry_job(self, job_id, message_id, error=None):
        job = self.get_job(job_id)
        print(job.attempts)

        attempts = job.attempts + 1

        if attempts >= self.MAX_ATTEMPS:
            # Move to dead letter queue
            self.redis.xadd(
                self.DL_STREAM, 
                {
                    "job_id": job_id,
                    "payload": job.to_json(),
                    "attempts": attempts,
                    "error": error or "max retries exceeded",
                }
            )
            # Remove from PEL
            self.redis.xack(
                self.STREAM, self.GROUP, message_id
            )
            print(f"Job {job_id} moved to DLQ")
            return

        self.redis.xadd(
            self.STREAM, 
            {
                "job_id": job_id,
                "payload": job.to_json(),
                "attempts": attempts,
            }
        )
        # ACK the old message
        self.redis.xack(
            self.STREAM, self.GROUP, message_id,
        )

        print(f"Retry job={job_id}, attempt={attempts}")


    def reclaim_stale(self, process_job, min_idle_time=10000):
        """
        Reclaim messages that have been pending
        for longer than min_idle_time milliseconds.
        """
        result = self.redis.xautoclaim(
            self.STREAM, self.GROUP, self.consumer, 
            min_idle_time, "0-0", count=10,
        )
        next_id, messages = result[0], result[1]

        if not messages:
            return

        print(f"Reclaimed {len(messages)} stale messages")

        for message_id, data in messages:
            print(f"Processing {message_id}")
            self._process_message(message_id, data, process_job)

    def _create_consumer_group(self):
        try:
            self.redis.xgroup_create(
                self.STREAM, self.GROUP, id="0", mkstream=True,
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def update_status(self, job, new_status):
        self.redis.srem(
            self._status_key(job.status), job.id,
        )
        # Add to new status
        self.redis.sadd(
            self._status_key(new_status), job.id,
        )
        job.status = new_status
        self._save_job(job)

    def _job_key(self, job_id):
        return f"{self.JOB_PREFIX}:{job_id}"

    def _status_key(self, status):
        return f"{self.STATUS_PREFIX}:{status}"

    def _save_job(self, job):
        self.redis.set(
            self._job_key(job.id), job.to_json()
        )

