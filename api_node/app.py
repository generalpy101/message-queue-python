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
    if not data:
        return jsonify({"error": "No data provided"}), 400

    resp = stub.Publish(broker_pb2.PublishRequest(
        topic="default",   # TODO: add real topics later
        payload=data
    ))
    return jsonify({"message_id": resp.message_id}), 200


@app.route("/consume", methods=["GET"])
def consume():
    # For now, just pull one message off the stream
    def node_messages():
        yield broker_pb2.NodeMessage(
            heartbeat=broker_pb2.Heartbeat(timestamp=0)
        )

    response_iter = stub.MessageStream(node_messages())
    try:
        msg = next(response_iter)   # Get first message
        return jsonify({
            "message_id": msg.message_id,
            "topic": msg.topic,
            "payload": msg.payload,
            "timestamp": msg.timestamp
        }), 200
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
        "payload": m.payload,
        "timestamp": m.timestamp
    } for m in resp.messages]
    return jsonify(messages), 200


if __name__ == '__main__':
    app.run(debug=True)
