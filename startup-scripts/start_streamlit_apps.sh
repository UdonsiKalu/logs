#!/bin/bash
# Start Streamlit Applications
# Runs your main data analysis applications

# Configuration
DENIALS_DIR="/media/udonsi-kalu/New Volume/denials"
RAG_DIR="$DENIALS_DIR/cms/manuals/rag"
VENV_PATH="/media/udonsi-kalu/New Volume/denials/denials/faiss_gpu1/bin/activate"
LOG_FILE="$HOME/workspace/logs/streamlit.log"

# Create logs directory if it doesn't exist
mkdir -p "$(dirname "$LOG_FILE")"

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Check if virtual environment exists
if [ ! -f "$VENV_PATH" ]; then
    log "ERROR: Virtual environment not found: $VENV_PATH"
    exit 1
fi

# Check if RAG directory exists
if [ ! -d "$RAG_DIR" ]; then
    log "ERROR: RAG directory not found: $RAG_DIR"
    exit 1
fi

log "Starting Streamlit applications..."

# Start Streamlit App 1 (streamlit_app4.py on port 8502)
log "Starting streamlit_app4.py on port 8502..."
cd "$RAG_DIR"
source "$VENV_PATH"
nohup streamlit run streamlit_app4.py --server.port 8502 --server.headless true > "$LOG_FILE.app4" 2>&1 &
STREAMLIT1_PID=$!
echo $STREAMLIT1_PID > "$HOME/workspace/logs/streamlit_app4.pid"
log "Streamlit App 4 started with PID: $STREAMLIT1_PID"

# Start Streamlit App 2 (complete_claim_analysis_app on port 8509)
log "Starting complete_claim_analysis_app on port 8509..."
nohup streamlit run complete_claim_analysis_app_cgpt3_update7.py --server.port 8509 --server.headless true > "$LOG_FILE.app2" 2>&1 &
STREAMLIT2_PID=$!
echo $STREAMLIT2_PID > "$HOME/workspace/logs/streamlit_app2.pid"
log "Streamlit App 2 started with PID: $STREAMLIT2_PID"

# Wait a moment to check if they started successfully
sleep 3

# Check if both apps are running
if ps -p $STREAMLIT1_PID > /dev/null && ps -p $STREAMLIT2_PID > /dev/null; then
    log " Both Streamlit applications started successfully"
    log "App 4 (streamlit_app4.py): http://localhost:8502"
    log "App 2 (complete_claim_analysis): http://localhost:8509"
    log "Log files: $LOG_FILE.app4, $LOG_FILE.app2"
else
    log " Some Streamlit applications failed to start"
    log "Check logs: $LOG_FILE.app4, $LOG_FILE.app2"
    exit 1
fi

