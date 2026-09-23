# ==========================================
# 2. DeadLetterQueue Component (Redis Stream)
# ==========================================
import time
import redis

class DeadLetterQueue:
    STREAM = "jobs:dead"

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    def push(self, job_id: str, error: str, attempts: int) -> str:
        return self.redis.xadd(
            self.STREAM,
            {
                "job_id": job_id,
                "error": error,
                "attempts": str(attempts),
            },
        )

    def fetch_dead_jobs(self, count: int = 10, start_id: str = "-") -> list[tuple[str, dict]]:
        """Lấy danh sách các jobs trong DLQ để kiểm tra/monitor từ bên ngoài"""
        entries = self.redis.xrange(self.STREAM, min=start_id, max="+", count=count)
        return [(msg_id, data) for msg_id, data in entries]

    def count(self) -> int:
        """Đếm tổng số tin nhắn trong DLQ Stream"""
        return self.redis.xlen(self.STREAM)
