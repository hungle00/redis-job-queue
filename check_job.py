import redis
from rqueue.job_queue import JobQueue
from rqueue.delayed_queue import DelayedQueue
from rqueue.dead_letter_queue import DeadLetterQueue

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
delay = DelayedQueue(redis_client=r)
dead_letter = DeadLetterQueue(redis_client=r)

print(dead_letter.fetch_dead_jobs())
print(dead_letter.count())
print(delay.get_all_members())
