import redis
import time
from job_queue import JobQueue

def send_email(job_id, payload):
    # actual email logic
    # email_service.send(...)
    print(f"Sending email to {payload['to']}")
    print(f"Subject: {payload['subject']}")

    if payload['to'] == "user5@example.com":
        raise Exception("Something went wrong")

    time.sleep(1)

    print(f"Email sent successfully for job {job_id}")


r = redis.Redis(host="localhost", port=6379, decode_responses=True)
queue = JobQueue(redis_client=r, consumer_name="mailer")

queue.worker(send_email)
