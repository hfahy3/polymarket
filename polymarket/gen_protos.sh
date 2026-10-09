#!/usr/bin/env bash
# Put Polymarket's combo.proto and rfq.proto (and any protos they import) in ./protos
set -euo pipefail
python -m grpc_tools.protoc -I protos --python_out=. --grpc_python_out=. protos/*.proto
echo "generated: $(ls *_pb2*.py)"
