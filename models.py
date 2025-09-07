from dataclasses import dataclass
from uuid import UUID
from enum import Enum

class MessageState(Enum):
    ENQUEUED = "enqueued"
    PROCESSING = "processing"
    INFLIGHT = "inflight"
    ACKNOWLEDGED = "acknowledged"
    RETRIED = "retried"

@dataclass
class Message:
    id: UUID
    data: object
    enqueued_at: float = None
    retries: int = 0
    state: MessageState = MessageState.ENQUEUED
    
    def to_dict(self):
        return {
            "id": str(self.id),
            "data": self.data,
            "enqueued_at": self.enqueued_at
        }
    
@dataclass
class InflightMessage:
    message: Message
    processing_started_at: float