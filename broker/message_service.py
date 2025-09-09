from broker.message_storage import MessageStorage
from broker.models import Message
from broker.persistence_service import PersistenceService
from uuid import uuid4, UUID
import time

import threading

class MessageService:
    REQUEUE_TIMEOUT = 30  # seconds
    MAX_RETRIES = 3

    def __init__(self):
        self.storage = MessageStorage()
        self.persistence_service = PersistenceService(is_async=True)
        # Load unacknowledged messages from persistence
        unacknowledged_messages = self.persistence_service.get_unacknowledged_messages()
        for msg in unacknowledged_messages:
            self.storage.enqueue(msg)
        self.requeue_thread = threading.Thread(target=self._requeue_worker, daemon=True)
        self.requeue_thread.start()
        
    def _requeue_worker(self):
        print("Requeue worker started")
        while True:
            self._bg_requeue_inflight()
            self._bg_cleanup_inflight()
            time.sleep(5)
    
    def produce(self, data: object) -> UUID:
        message = Message(id=uuid4(), data=data, enqueued_at=time.time())
        print("Message: ", message.to_dict())
        self.storage.enqueue(message)
        self.persistence_service.log_message(message)
        return message.id
    
    def consume(self):
        message = self.storage.dequeue()
        self.persistence_service.update_message(message)

        return message

    def acknowledge(self, message_id: UUID) -> bool:
        self.persistence_service.ack_message(str(message_id))
        return self.storage.acknowledge(message_id)
    
    def get_dead_letter(self):
        return self.storage.dead_letter
    
    def get_all_messages(self):
        return self.storage.get_all_messages()

    def _bg_requeue_inflight(self, timeout: int = REQUEUE_TIMEOUT):
        current_time = time.time()
        to_requeue = []
        for msg_id, inflight in list(self.storage.in_flight.items()):
            if current_time - inflight.processing_started_at > timeout:
                to_requeue.append(msg_id)
        
        for msg_id in to_requeue:
            self.storage.requeue_from_inflight(msg_id)
            
    def _bg_cleanup_inflight(self):
        '''
        Removes messages which have been retried too many times and are in inflight queue
        '''
        to_remove = []
        for msg_id, inflight in self.storage.in_flight.items():
            if inflight.too_many_retries(self.MAX_RETRIES):
                to_remove.append(msg_id)

        for msg_id in to_remove:
            self.storage.add_to_dead_letter(inflight.message)
