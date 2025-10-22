#!/bin/bash
# Stop Cloudflare Tunnel

PID_FILE="/home/udonsi-kalu/workspace/logs/cloudflare_tunnel.pid"
LOG_FILE="/home/udonsi-kalu/workspace/logs/cloudflare_tunnel.log"

echo "🛑 Stopping Cloudflare tunnel..."

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        kill $PID
        echo "✅ Tunnel stopped (PID: $PID)"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Tunnel stopped" >> "$LOG_FILE"
        rm -f "$PID_FILE"
    else
        echo "⚠️  Tunnel was not running (stale PID)"
        rm -f "$PID_FILE"
    fi
else
    echo "❌ No tunnel PID file found"
    
    # Try to kill any running cloudflared processes for port 8509
    PIDS=$(ps aux | grep "cloudflared tunnel --url http://localhost:8509" | grep -v grep | awk '{print $2}')
    if [ -n "$PIDS" ]; then
        echo "Found cloudflared processes: $PIDS"
        echo "$PIDS" | xargs kill
        echo "✅ Killed cloudflared processes"
    fi
fi

echo "Done."




