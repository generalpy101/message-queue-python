from flask import Flask, request, jsonify
from uuid import UUID
from message_service import MessageService
from models import Message

app = Flask(__name__)

service = MessageService()

@app.route('/produce', methods=['POST'])
def produce():
    data = request.json.get('data')
    if data is None:
        return jsonify({"error": "No data provided"}), 400
    message_id = service.produce(data)
    return jsonify({"message_id": str(message_id)}), 200

@app.route('/consume', methods=['GET'])
def consume():
    message: Message = service.consume()
    if message is None:
        return jsonify({"message": None}), 200
    return jsonify(message.to_dict()), 200

@app.route('/acknowledge', methods=['POST'])
def acknowledge():
    message_id_str = request.json.get('message_id')
    if not message_id_str:
        return jsonify({"error": "No message_id provided"}), 400
    try:
        message_id = UUID(message_id_str)
    except ValueError:
        return jsonify({"error": "Invalid UUID format"}), 400
    success = service.acknowledge(message_id)
    if not success:
        return jsonify({"error": "Message ID not found or not in-flight"}), 404
    return jsonify({"status": "acknowledged"}), 200

@app.route('/dead_letter', methods=['GET'])
def dead_letter():
    messages = service.get_dead_letter()
    return jsonify([msg.to_dict() for msg in messages]), 200

@app.route("/debug/show_all", methods=['GET'])
def debug_show_all():
    all_messages = service.get_all_messages()
    return jsonify([msg.to_dict() for msg in all_messages]), 200

if __name__ == '__main__':
    app.run(debug=True)