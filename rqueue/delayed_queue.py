# ==========================================
# 1. DelayedQueue Component (Redis ZSET)
# ==========================================
import time
import redis

class DelayedQueue:
    KEY = "job-queue:delayed"

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    def push(self, job_id: str, execute_at: float):
        self.redis.zadd(self.KEY, {job_id: execute_at})

    def pop_ready_jobs(self, now: float | None = None) -> list[str]:
        if now is None:
            now = time.time()

        ready_jobs = self.redis.zrangebyscore(self.KEY, 0, now)
        if not ready_jobs:
            return []

        p = self.redis.pipeline()
        for job_id in ready_jobs:
            p.zrem(self.KEY, job_id)
        p.execute()

        return ready_jobs

    def count(self) -> int:
        return self.redis.zcard(self.KEY)