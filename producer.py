import redis
import uuid
import time
from job_queue import JobQueue

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
jqueue = JobQueue(redis_client=r)

def send_email(data):
    # actual email logic
    # email_service.send(...)
    time.sleep(1)
    print(f"Sending email to {data['to']}")
    print(f"Subject: {data['subject']}")

    if data['to'] == "user5@example.com":
        raise Exception("Something went wrong")


if __name__ == "__main__":
    user_data = {
        "type": "send_email",
        "to": f"user@example.com",
        "subject": "Welcome!",
        "body": "Welcome to our application!",
    }
    # Enqueue normal function
    jqueue.enqueue(send_email, user_data)
    # Enqueue Lambda
    jqueue.enqueue(lambda a, b: f"Hello {a} {b}", "World", "!")
