import redis
import uuid
import time
from job_queue import JobQueue

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
        queue.enqueue(payload)
        time.sleep(1)

if __name__ == "__main__":
    enqueue_job()