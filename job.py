import json
import cloudpickle
import base64
from dataclasses import dataclass, asdict
from typing import Optional

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
    error: Optional[str] = None

    def to_dict(self):
        return asdict(self)

    def serialize(self) -> str:
        return base64.b64encode(
            cloudpickle.dumps(self.to_dict())
        ).decode("ascii")

    @classmethod
    def deserialize(cls, data: str):
        raw = base64.b64decode(data)
        return cls(**cloudpickle.loads(raw))

    def unpack_payload(self):
        func = self.payload["func"]

        return (
            func,
            self.payload.get("args", ()),
            self.payload.get("kwargs", {}),
        )
