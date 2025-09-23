from flask import Flask, request, jsonify
import grpc
from uuid import UUID

# Import gRPC stubs
from proto import broker_pb2, broker_pb2_grpc

app = Flask(__name__)

# Create gRPC channel + stub (reuse this across requests)
channel = grpc.insecure_channel("localhost:50051")  # Broker address
stub = broker_pb2_grpc.BrokerStub(channel)


@app.route("/produce", methods=["POST"])
def produce():
    data = request.json.get("data")
    topic = request.json.get("topic")
    if not data or not topic:
        return jsonify({"error": "No data or topic provided"}), 400

    resp = stub.Publish(broker_pb2.PublishRequest(
        topic=topic,
        payload=data
    ))
    return jsonify({"message_id": resp.message_id}), 200


@app.route("/consume", methods=["GET"])
def consume():
    '''
    Consumes `count` number of messages from a `topic`.
    Default `count` is 1.
    '''
    topic = request.args.get("topic", "default")
    count = request.args.get("count", 1, type=int)

    try:
        response_iter = stub.MessageStream(broker_pb2.MessageStreamRequest(
            topic=topic,
            count=count
        ))
        for msg in response_iter:
            yield {
                "message_id": msg.message_id,
                "topic": msg.topic,
                "payload": msg.payload,
                "timestamp": msg.timestamp
            }
    except StopIteration:
        return jsonify({"message": None}), 200


@app.route('/acknowledge', methods=['POST'])
def acknowledge():
    message_id_str = request.json.get('message_id')
    if not message_id_str:
        return jsonify({"error": "No message_id provided"}), 400
    try:
        UUID(message_id_str)  # validate format
    except ValueError:
        return jsonify({"error": "Invalid UUID format"}), 400

    resp = stub.Ack(broker_pb2.AckRequest(message_id=message_id_str))
    if not resp.success:
        return jsonify({"error": "Message ID not found or not in-flight"}), 404
    return jsonify({"status": "acknowledged"}), 200


@app.route('/dead_letter', methods=['GET'])
def dead_letter():
    resp = stub.GetDeadLetter(broker_pb2.Empty())
    messages = [{
        "message_id": m.message_id,
        "topic": m.topic,
        "payload": m.payload,
        "timestamp": m.timestamp
    } for m in resp.messages]
    return jsonify(messages), 200


@app.route("/debug/show_all", methods=['GET'])
def debug_show_all():
    resp = stub.GetAllMessages(broker_pb2.Empty())
    messages = [{
        "message_id": m.message_id,
        "topic": m.topic,
        "payload": m.data,
        "enqueued_at": m.enqueued_at
    } for m in resp.messages]
    return jsonify(messages), 200


if __name__ == '__main__':
    app.run(debug=True)
