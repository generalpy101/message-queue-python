from message_storage import MessageStorage
from models import Message
from uuid import uuid4, UUID
import time

import threading

class MessageService:
    REQUEUE_TIMEOUT = 30  # seconds

    def __init__(self):
        self.storage = MessageStorage()
        self.requeue_thread = threading.Thread(target=self._requeue_worker, daemon=True)
        self.requeue_thread.start()
        
    def _requeue_worker(self):
        print("Requeue worker started")
        while True:
            self.bg_requeue_inflight()
            time.sleep(5)
    
    def produce(self, data: object) -> UUID:
        message = Message(id=uuid4(), data=data, enqueued_at=time.time())
        self.storage.enqueue(message)
        return message.id
    
    def consume(self):
        return self.storage.dequeue()
    
    def acknowledge(self, message_id: UUID) -> bool:
        return self.storage.acknowledge(message_id)
    
    def bg_requeue_inflight(self, timeout: int = REQUEUE_TIMEOUT):
        current_time = time.time()
        to_requeue = []
        for msg_id, inflight in list(self.storage.in_flight.items()):
            if current_time - inflight.processing_started_at > timeout:
                to_requeue.append(msg_id)
        
        for msg_id in to_requeue:
            self.storage.requeue_from_inflight(msg_id)