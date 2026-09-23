import redis
from rqueue.job_queue import JobQueue
from rqueue.worker import Worker

if __name__ == "__main__":
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    jqueue = JobQueue(redis_client=r)
    worker = Worker(queue=jqueue, consumer_name="mailer")
    
    worker.start()