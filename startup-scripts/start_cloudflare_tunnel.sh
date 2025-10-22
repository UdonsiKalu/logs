#!/bin/bash
# Start Cloudflare Tunnel for Streamlit App

LOG_DIR="/home/udonsi-kalu/workspace/logs"
LOG_FILE="$LOG_DIR/cloudflare_tunnel.log"
PID_FILE="$LOG_DIR/cloudflare_tunnel.pid"
URL_FILE="$LOG_DIR/cloudflare_tunnel_url.txt"

mkdir -p "$LOG_DIR"

# Check if tunnel is already running
if [ -f "$PID_FILE" ] && ps -p "$(cat "$PID_FILE")" > /dev/null 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Cloudflare tunnel is already running."
    if [ -f "$URL_FILE" ]; then
        echo "Current URL: $(cat "$URL_FILE")"
    fi
    exit 0
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting Cloudflare tunnel for port 8509..." | tee -a "$LOG_FILE"

# Start cloudflared tunnel
nohup cloudflared tunnel --url http://localhost:8509 > "$LOG_FILE" 2>&1 &
TUNNEL_PID=$!
echo $TUNNEL_PID > "$PID_FILE"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Cloudflare tunnel started with PID: $TUNNEL_PID" | tee -a "$LOG_FILE"

# Wait for tunnel to initialize and extract URL
echo "Waiting for tunnel URL..."
sleep 5

# Extract URL from log
TUNNEL_URL=$(grep -oP 'https://[a-zA-Z0-9-]+\.trycloudflare\.com' "$LOG_FILE" | tail -1)

if [ -n "$TUNNEL_URL" ]; then
    echo "$TUNNEL_URL" > "$URL_FILE"
    echo "" | tee -a "$LOG_FILE"
    echo "============================================" | tee -a "$LOG_FILE"
    echo "✅ Cloudflare Tunnel is LIVE!" | tee -a "$LOG_FILE"
    echo "============================================" | tee -a "$LOG_FILE"
    echo "🌐 Public URL: $TUNNEL_URL" | tee -a "$LOG_FILE"
    echo "📱 Share this link with anyone!" | tee -a "$LOG_FILE"
    echo "============================================" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] URL saved to: $URL_FILE" | tee -a "$LOG_FILE"
else
    echo "⚠️  Could not extract URL yet. Check log: $LOG_FILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Warning: Could not extract tunnel URL" >> "$LOG_FILE"
fi

echo ""
echo "📋 Tunnel Info:"
echo "   PID: $TUNNEL_PID"
echo "   Log: $LOG_FILE"
echo "   URL file: $URL_FILE"
echo ""
echo "💡 To get the URL anytime, run:"
echo "   cat $URL_FILE"




