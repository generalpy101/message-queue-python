from models import Message, MessageState, InflightMessage
from collections import deque
from typing import Deque, Dict
from uuid import UUID
import time

class MessageStorage:
    def __init__(self):
        self.queue: Deque[Message] = deque()
        self.in_flight: Dict[UUID, InflightMessage] = {}
        
    def enqueue(self, item: Message):
        self.queue.append(item)
        
    def dequeue(self) -> Message | None:
        if self.queue:
            item = self.queue.popleft()
            item.state = MessageState.INFLIGHT
            self.in_flight[item.id] = InflightMessage(message=item, processing_started_at=time.time())
            return item
        return None

    def peek(self) -> Message | None:
        if self.queue:
            return self.queue[0]
        return None
    
    def acknowledge(self, message_id: UUID) -> bool:
        if message_id in self.in_flight:
            msg = self.in_flight.pop(message_id)
            msg.message.state = MessageState.ACKNOWLEDGED
            return True
        return False
    
    def requeue_from_inflight(self, message_id: UUID) -> bool:
        if message_id in self.in_flight:
            inflight = self.in_flight.pop(message_id)
            inflight.message.state = MessageState.RETRIED
            self.enqueue(inflight.message)
            return True
        return False
        