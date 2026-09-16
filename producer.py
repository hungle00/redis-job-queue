import redis
import uuid
import time
from job_queue import Job, JobQueue

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
queue = JobQueue(redis_client=r, consumer_name="mailer")

def enqueue_job():
    for i in range(1, 11):
        # generate mail data
        payload = {
            "type": "send_email",
            "to": f"user{i}@example.com",
            "subject": "Welcome!",
            "body": "Welcome to our application!",
        }
        job = Job(
            id = str(uuid.uuid4()),
            payload=payload
        )
        queue.enqueue(job)
        time.sleep(1)

if __name__ == "__main__":
    enqueue_job()