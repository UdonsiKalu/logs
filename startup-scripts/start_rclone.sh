#!/bin/bash
# Start Rclone sync service
# Syncs entire denials folder (including cms and denials) to Google Drive daily

# Configuration
LOCAL_FOLDER="/media/udonsi-kalu/New Volume/denials"
REMOTE_NAME="gdrive_backup"
LOG_FILE="$HOME/workspace/logs/rclone.log"

# Create logs directory if it doesn't exist
mkdir -p "$(dirname "$LOG_FILE")"

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Check if rclone is available
if ! command -v rclone &> /dev/null; then
    log "ERROR: rclone command not found"
    exit 1
fi

# Check if local folder exists
if [ ! -d "$LOCAL_FOLDER" ]; then
    log "ERROR: Local folder does not exist: $LOCAL_FOLDER"
    exit 1
fi

# Check if remote is configured
if ! rclone listremotes | grep -q "^${REMOTE_NAME}:$"; then
    log "ERROR: Remote '$REMOTE_NAME' not configured"
    exit 1
fi

log "Starting rclone sync service..."
log "Local: $LOCAL_FOLDER"
log "Remote: $REMOTE_NAME"

# Start the sync process in background
nohup rclone sync "$LOCAL_FOLDER" "$REMOTE_NAME:/denials-backup" \
    --progress \
    --log-file="$LOG_FILE" \
    --log-level=INFO \
    --stats=1m \
    --transfers=4 \
    --checkers=8 \
    --retries=3 \
    --low-level-retries=10 \
    --exclude="*.tmp" \
    --exclude="*.log" \
    --exclude="__pycache__/**" \
    --exclude=".git/**" \
    --exclude="node_modules/**" \
    --exclude="*.pyc" \
    --exclude=".DS_Store" \
    --exclude="Thumbs.db" \
    --exclude="faiss_gpu1/**" \
    --exclude="faiss_venv/**" \
    --exclude="venv/**" \
    --exclude="*.index" \
    --exclude="*.faiss" \
    > /dev/null 2>&1 &

# Get the PID
RCLONE_PID=$!
echo $RCLONE_PID > "$HOME/workspace/logs/rclone.pid"

log "Rclone sync started with PID: $RCLONE_PID"
log "Log file: $LOG_FILE"
log "PID file: $HOME/workspace/logs/rclone.pid"

# Wait a moment to check if it started successfully
sleep 2
if ps -p $RCLONE_PID > /dev/null; then
    log "✅ Rclone sync is running successfully"
else
    log "❌ Failed to start rclone sync"
    exit 1
fi

