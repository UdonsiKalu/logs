#!/bin/bash
# Start Cloudflare Tunnel for udonsik.com and streamlit-app.udonsik.com

LOG_FILE="/home/udonsi-kalu/workspace/logs/streamlit_tunnel.log"
PID_FILE="/home/udonsi-kalu/workspace/logs/cloudflare_main_tunnel.pid"

# Check if tunnel is already running
if [ -f "$PID_FILE" ] && ps -p "$(cat "$PID_FILE")" > /dev/null 2>&1; then
    echo " Cloudflare tunnel is already running"
    exit 0
fi

echo " Starting Cloudflare tunnel for udonsik.com..."

# Start tunnel
cloudflared --config /home/udonsi-kalu/.cloudflared/streamlit-config.yml tunnel run streamlit-app > "$LOG_FILE" 2>&1 &
TUNNEL_PID=$!
echo $TUNNEL_PID > "$PID_FILE"

sleep 5

if ps -p $TUNNEL_PID > /dev/null; then
    echo " Cloudflare tunnel started successfully"
    echo ""
    echo " Serving:"
    echo "   - https://udonsik.com"
    echo "   - https://www.udonsik.com"
    echo "   - https://streamlit-app.udonsik.com"
    echo ""
    echo " If domains don't work, update DNS in Cloudflare dashboard:"
    echo "   Change A records to CNAME  c7f006b3-3f9a-4c49-91aa-1620352cb61c.cfargotunnel.com"
else
    echo " Tunnel failed to start. Check log: $LOG_FILE"
    exit 1
fi




