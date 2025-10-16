#!/bin/bash
# Start Qdrant Docker container if not running
if ! docker ps --format '{{.Names}}' | grep -q '^qdrant$'; then
  echo "[Qdrant] Container not running. Starting..."
  docker start qdrant >> ~/workspace/logs/qdrant.log 2>&1
else
  echo "[Qdrant] Container already running." >> ~/workspace/logs/qdrant.log
fi
