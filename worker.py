import redis
from job_queue import JobQueue

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
worker = JobQueue(r, consumer_name="mailer")
worker.process()
