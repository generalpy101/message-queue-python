#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH=$(pwd)

# Kill old brokers if any
pkill -f "python -m broker.broker" || true

# Start broker with auto-reload
echo "Starting Broker on :50051 ..."
watchmedo auto-restart --recursive --pattern="*.py" -- \
  python -m broker.broker &
BROKER_PID=$!

trap "kill $BROKER_PID" EXIT

sleep 2

# Start API Node with auto-reload
echo "Starting API Node on :5000 ..."
watchmedo auto-restart --recursive --pattern="*.py" -- \
  python -m api_node.app
