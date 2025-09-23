import grpc
from concurrent import futures
import time

from broker.message_service import MessageService
from proto import broker_pb2, broker_pb2_grpc
from broker.models import MessageState


def message_to_proto(msg, topic="default", visibility_timeout=30):
    """Helper to convert internal Message → BrokerMessage (proto)."""
    print(f"Converting message to proto: {msg}")
    return broker_pb2.BrokerMessage(
        message_id=str(msg.id),
        topic=topic,
        data=str(msg.data),
        enqueued_at=msg.enqueued_at or 0.0,
        retries=msg.retries,
        state=int(msg.state),
        visibility_timeout=visibility_timeout,
    )


class BrokerServicer(broker_pb2_grpc.BrokerServicer):
    def __init__(self):
        self.message_service = MessageService()

    # ---- Producer API ----
    def Publish(self, request, context):
        print(f"Publishing message to topic '{request.topic}': {request.payload}")
        msg_id = self.message_service.produce(request.payload)
        return broker_pb2.PublishResponse(message_id=str(msg_id))

    # ---- Streaming API (for consumers / API nodes) ----
    def MessageStream(self, request_iterator, context):
        """
        Bidirectional stream:
          - Receives NodeMessage (acks, heartbeats) from API node
          - Yields BrokerMessage when available
        """
        for node_msg in request_iterator:
            if node_msg.HasField("ack"):
                self.message_service.acknowledge(node_msg.ack.message_id)
            elif node_msg.HasField("heartbeat"):
                # In production: update client liveness
                pass

            # For now: yield one message if available
            message = self.message_service.consume()
            if message:
                yield message_to_proto(message)

    # ---- Ack RPC (for REST /acknowledge) ----
    def Ack(self, request, context):
        success = self.message_service.acknowledge(request.message_id)
        return broker_pb2.AckResponse(success=success)

    # ---- Dead letter queue ----
    def GetDeadLetter(self, request, context):
        dead_msgs = self.message_service.get_dead_letter()
        return broker_pb2.DeadLetterResponse(
            messages=[message_to_proto(m) for m in dead_msgs]
        )

    # ---- Debug all messages ----
    def GetAllMessages(self, request, context):
        all_msgs = self.message_service.get_all_messages()
        return broker_pb2.AllMessagesResponse(
            messages=[message_to_proto(m) for m in all_msgs]
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    broker_pb2_grpc.add_BrokerServicer_to_server(BrokerServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Broker gRPC server started on :50051")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve()
