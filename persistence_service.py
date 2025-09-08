import sqlite3
from models import Message, MessageState
import threading

MESSAGE_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS messages ("
    "id TEXT PRIMARY KEY,"
    "data TEXT NOT NULL,"
    "state TEXT NOT NULL,"
    "enqueued_at REAL NOT NULL"
    ")"
)

class PersistenceService:
    def __init__(self, is_async: bool):
        self.db = "./message_queue.db"
        self.conn = self._get_connection()
        self.lock = threading.Lock()
        self._is_async = is_async
        self._setup_db()

    def _get_connection(self):
        conn = sqlite3.connect(self.db, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _setup_db(self):
        '''
        Sets up db if not done already
        '''
        with self.conn:
            self.conn.execute(MESSAGE_SCHEMA)

    def log_message(self, message: Message):
        '''
        Log a message to the database. This will be done asynchronously by default in a new thread.
        Sync version will block until the message is logged. This will be slow.
        '''
        if self._is_async:
            threading.Thread(target=self._log_message, args=(message,)).start()
        else:
            self._log_message(message)
            
    def ack_message(self, message_id: Message):
        if self._is_async:
            threading.Thread(target=self._ack_message, args=(message_id,)).start()
        else:
            self._ack_message(message_id)

    def update_message(self, message: Message):
        if self._is_async:
            threading.Thread(target=self._update_message, args=(message,)).start()
        else:
            self._update_message(message)

    def _update_message(self, message: Message):
        with self.lock:
            with self.conn:
                self.conn.execute(
                    "UPDATE messages SET data = ?, state = ? WHERE id = ?",
                    (message.data, message.state.value, message.id)
                )

    def _log_message(self, message: Message):
        with self.lock:
            with self.conn:
                print(message.id, message.data, message.state.value, message.enqueued_at)
                self.conn.execute(
                    "INSERT INTO messages (id, data, state, enqueued_at) VALUES (?, ?, ?, ?)",
                    (str(message.id), message.data, message.state.value, message.enqueued_at)
                )

    def _ack_message(self, message_id: str):
        with self.lock:
            with self.conn:
                self.conn.execute(
                    "UPDATE messages SET state = ? WHERE id = ?",
                    (MessageState.ACKNOWLEDGED.value, message_id)
                )

    def get_unacknowledged_messages(self):
       '''
       Used to replay unacknowledged messages in case of failure.
       This will run once at startup of the messaging service.
       '''
       with self.lock:
           with self.conn:
               cursor = self.conn.execute("SELECT * FROM messages WHERE state != ? ORDER BY enqueued_at", (MessageState.ACKNOWLEDGED.value,))
               return [Message(**row) for row in cursor.fetchall()]
