import redis
from job_queue import JobQueue

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
queue = JobQueue(redis_client=r, consumer_name="mailer")

completed_jobs = queue.get_jobs_by_status("dead-letter")
for job in completed_jobs:
    print(job)

job = queue.get_job("63a43797-aae0-4e15-b32f-47bc48e3397a")
print(job)
