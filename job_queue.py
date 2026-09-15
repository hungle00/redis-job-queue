import redis
import json
import time

class JobQueue:
    STREAM = "jobs"
    GROUP = "workers"
    DL_QUEUE = "jobs:dead"
    MAX_RETRIES = 3

    def __init__(self, redis_client: redis.Redis, consumer_name: str):
        self.redis = redis_client
        self.consumer = consumer_name

    def enqueue(self, job_id, payload):
        message_id = self.redis.xadd(
            self.STREAM, 
            {
                "job_id": job_id,
                "payload": json.dumps(payload),
                "attempts": 0,
            }
        )
        print(f"Enqueued job={job_id}, message={message_id}")

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
        payload = json.loads(data["payload"])

        try:
            process_job(job_id, payload)

            self.redis.xack(
                self.STREAM, self.GROUP, message_id
            )
            print(f"ACK job={job_id} with message={message_id}")

        except Exception as e:
            print(f"Job {job_id} failed: {e}")
            self.retry_job(
                job_id, payload, message_id, data, error=str(e),
            )

    def retry_job(self, job_id, payload, message_id, data, error=None):
        attempts = int(data.get("attempts", 0)) + 1

        if attempts >= self.MAX_RETRIES:
            # Move to dead letter queue
            self.redis.xadd(
                self.DL_QUEUE, 
                {
                    "job_id": job_id,
                    "payload": json.dumps(payload),
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
                "payload": json.dumps(payload),
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

