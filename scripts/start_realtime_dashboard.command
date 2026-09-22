#!/bin/zsh
set -eu

SCRIPT_DIR=${0:A:h}
PROJECT_DIR=${SCRIPT_DIR:h}
cd "$PROJECT_DIR"
exec /usr/bin/env PYTHONDONTWRITEBYTECODE=1 python3 scripts/realtime_server.py serve --host 127.0.0.1 --port 8767 --poll-seconds 1
