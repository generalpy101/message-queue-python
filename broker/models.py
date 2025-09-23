from dataclasses import dataclass
from uuid import UUID
from enum import Enum

class MessageState(Enum):
    ENQUEUED = 0      # Message is in queue, not yet processed
    PROCESSING = 1    # Message is currently being processed
    INFLIGHT = 2      # Message has been sent to a consumer but not yet acknowledged
    ACKNOWLEDGED = 3  # Message has been successfully processed
    RETRIED = 4       # Message has been retried after a failure
    
    @classmethod
    def get_name(cls, value):
        """Get human-readable name for an enum value (for debugging/logging)"""
        for state in cls:
            if state.value == value:
                return state.name
        return f"UNKNOWN({value})"

@dataclass
class Message:
    id: UUID
    data: object
    topic: str = "default"
    enqueued_at: float = None
    retries: int = 0
    state: MessageState = MessageState.ENQUEUED.value
    
    def to_dict(self):
        return {
            "id": str(self.id),
            "data": self.data,
            "topic": self.topic,
            "enqueued_at": self.enqueued_at
        }
    
@dataclass
class InflightMessage:
    message: Message
    processing_started_at: float

    def too_many_retries(self, threshold: int) -> bool:
        return self.message.retries >= threshold