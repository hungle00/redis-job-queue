import json
from dataclasses import dataclass, asdict

class JobStatus:
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    RETRYING = "retrying"
    DEAD_LETTER = "dead-letter"


@dataclass
class Job:
    id: str
    payload: dict
    status: str = JobStatus.QUEUED
    attempts: int = 0
    error: str | None = None

    def to_dict(self):
        return asdict(self)

    def to_json(self):
        return json.dumps(self.to_dict())
