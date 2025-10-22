#!/bin/bash
# Quick script to get the current Cloudflare tunnel URL

URL_FILE="/home/udonsi-kalu/workspace/logs/cloudflare_tunnel_url.txt"
LOG_FILE="/home/udonsi-kalu/workspace/logs/cloudflare_tunnel.log"
PID_FILE="/home/udonsi-kalu/workspace/logs/cloudflare_tunnel.pid"

echo "🔍 Checking Cloudflare Tunnel Status..."
echo ""

# Check if tunnel is running
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "✅ Tunnel is RUNNING (PID: $PID)"
        echo ""
        
        # Check for URL
        if [ -f "$URL_FILE" ]; then
            URL=$(cat "$URL_FILE")
            echo "============================================"
            echo "🌐 PUBLIC STREAMLIT URL:"
            echo "   $URL/app"
            echo ""
            echo "   (Base URL: $URL)"
            echo "============================================"
            echo ""
            echo "📋 Copy and share this link!"
        else
            echo "⚠️  URL file not found. Extracting from log..."
            TUNNEL_URL=$(grep -oP 'https://[a-zA-Z0-9-]+\.trycloudflare\.com' "$LOG_FILE" | tail -1)
            if [ -n "$TUNNEL_URL" ]; then
                echo "$TUNNEL_URL" > "$URL_FILE"
                echo "============================================"
                echo "🌐 PUBLIC URL:"
                echo "   $TUNNEL_URL"
                echo "============================================"
            else
                echo "❌ Could not find URL in log"
            fi
        fi
    else
        echo "❌ Tunnel is NOT running (stale PID file)"
        echo "   Run: ~/workspace/startup-scripts/start_cloudflare_tunnel.sh"
    fi
else
    echo "❌ Tunnel is NOT running"
    echo "   Run: ~/workspace/startup-scripts/start_cloudflare_tunnel.sh"
fi

echo ""
echo "💡 Tip: To stop the tunnel, run:"
echo "   ~/workspace/startup-scripts/stop_cloudflare_tunnel.sh"

