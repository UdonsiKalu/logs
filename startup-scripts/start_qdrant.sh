#!/bin/bash
# Start Qdrant Docker container, creating it if missing
LOG_FILE=~/workspace/logs/qdrant.log
mkdir -p "$(dirname "$LOG_FILE")"

# If container doesn't exist, create it
if ! docker ps -a --format '{{.Names}}' | grep -q '^qdrant$'; then
  echo "[Qdrant] Container not found. Creating..." >> "$LOG_FILE"
  docker run -d --name qdrant \
    -p 6333:6333 -p 6334:6334 \
    -v $HOME/qdrant/storage:/qdrant/storage \
    qdrant/qdrant:latest >> "$LOG_FILE" 2>&1 || {
      echo "[Qdrant] Failed to create container" >> "$LOG_FILE"
      exit 1
    }
  echo "[Qdrant] Container created." >> "$LOG_FILE"
fi

# Start if not running
if ! docker ps --format '{{.Names}}' | grep -q '^qdrant$'; then
  echo "[Qdrant] Starting container..." >> "$LOG_FILE"
  docker start qdrant >> "$LOG_FILE" 2>&1 || {
    echo "[Qdrant] Failed to start container" >> "$LOG_FILE"
    exit 1
  }
  echo "[Qdrant] Container started." >> "$LOG_FILE"
else
  echo "[Qdrant] Container already running." >> "$LOG_FILE"
fi
