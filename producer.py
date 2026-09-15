import redis
import uuid
import time
from job_queue import JobQueue

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
queue = JobQueue(redis_client=r, consumer_name="mailer")


def produce_data():
    for i in range(1, 11):
        # generate mail data
        job_id = str(uuid.uuid4())
        payload = {
            "type": "send_email",
            "to": f"user{i}@example.com",
            "subject": "Welcome!",
            "body": "Welcome to our application!",
        }
        queue.enqueue(job_id=job_id, payload=payload)
        time.sleep(1)

if __name__ == "__main__":
    produce_data()