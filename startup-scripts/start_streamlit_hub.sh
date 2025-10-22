#!/bin/bash
# Start Streamlit Hub Launcher

LOG_DIR="/home/udonsi-kalu/workspace/logs"
mkdir -p "$LOG_DIR"

HUB_LAUNCHER="/home/udonsi-kalu/workspace/streamlit-hub/launchers/streamlit_launcher.py"
LOG_FILE="$LOG_DIR/streamlit_hub.log"
PID_FILE="$LOG_DIR/streamlit_hub.pid"

# Check if the hub launcher is already running
if [ -f "$PID_FILE" ] && ps -p "$(cat "$PID_FILE")" > /dev/null; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Streamlit Hub launcher is already running on port 8500." >> "$LOG_FILE"
    echo "✅ Streamlit Hub launcher is already running on port 8500."
    exit 0
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting Streamlit Hub launcher on port 8500..." >> "$LOG_FILE"
echo "🚀 Starting Streamlit Hub launcher on port 8500..."

# Start the Streamlit Hub launcher
nohup streamlit run "$HUB_LAUNCHER" --server.port 8500 \
    --server.headless true \
    --browser.gatherUsageStats false \
    --logger.level info \
    >> "$LOG_FILE" 2>&1 & echo $! > "$PID_FILE"

sleep 3
if netstat -tlnp | grep -q ":8500 "; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ Streamlit Hub launcher started successfully on port 8500." >> "$LOG_FILE"
    echo "✅ Streamlit Hub launcher started successfully on port 8500."
    echo "🌐 Access at: http://localhost:8500/app"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ❌ Streamlit Hub launcher failed to start on port 8500." >> "$LOG_FILE"
    echo "❌ Streamlit Hub launcher failed to start on port 8500."
    return 1
fi

