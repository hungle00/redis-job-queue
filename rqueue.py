import argparse
import redis
from rqueue.job_queue import JobQueue
from rqueue.worker import Worker

def main():
    parser = argparse.ArgumentParser(description="Redis job queue worker")
    subparsers = parser.add_subparsers(dest="command", required=True)
    worker_parser = subparsers.add_parser("worker", help="Start a queue worker")
    worker_parser.add_argument("--queues", default="default")
    worker_parser.add_argument("--name", default="worker")
    worker_parser.add_argument("--threads", type=int, default=5)
    args = parser.parse_args()

    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    jqueue = JobQueue(redis_client=r)
    queues = [name.strip() for name in args.queues.split(",") if name.strip()]
    if not queues:
        parser.error("--queues must contain at least one queue name")

    worker = Worker(
        queue=jqueue,
        consumer_name=args.name,
        queues=queues,
        max_threads=args.threads,
    )
    worker.start()


if __name__ == "__main__":
    main()