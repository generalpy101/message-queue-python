#!/usr/bin/env bash
set -e

export PYTHONPATH=$(pwd)

echo "Starting Broker on :50051 ..."
python -m broker.broker &

sleep 2

echo "Starting API Node on :5000 ..."
python -m api_node.app
