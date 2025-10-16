#!/bin/bash
# Simple Service Monitor - ONE Terminal with Tabs
# Monitors your existing services without starting new ones

echo "🔍 Opening simple monitoring terminal..."

# Create ONE terminal with tabs
gnome-terminal --title="Service Monitor" \
    --tab --title="Qdrant" \
    -- bash -c "echo '=== QDRANT MONITOR ==='; while true; do clear; echo 'Qdrant Container:'; docker ps | grep qdrant; echo; echo 'Qdrant Logs (last 10 lines):'; docker logs --tail=10 \$(docker ps -q --filter ancestor=qdrant/qdrant:latest) 2>/dev/null || echo 'No logs available'; echo; echo 'Updated:' \$(date); sleep 5; done; exec bash" \
    --tab --title="Rclone" \
    -- bash -c "echo '=== RCLONE MONITOR ==='; while true; do clear; echo 'Rclone Process:'; ps aux | grep rclone | grep -v grep; echo; echo 'Rclone Logs:'; tail -5 ~/workspace/logs/rclone.log 2>/dev/null || echo 'No logs found'; echo; echo 'Updated:' \$(date); sleep 5; done; exec bash" \
    --tab --title="Streamlit" \
    -- bash -c "echo '=== STREAMLIT MONITOR ==='; while true; do clear; echo 'Streamlit Processes:'; ps aux | grep streamlit | grep -v grep; echo; echo 'Ports 8502 & 8509:'; netstat -tlnp | grep -E ':(8502|8509)'; echo; echo 'Updated:' \$(date); sleep 5; done; exec bash" \
    --tab --title="GPU" \
    -- bash -c "echo '=== GPU MONITOR ==='; nvtop; exec bash" \
    --tab --title="System" \
    -- bash -c "echo '=== SYSTEM MONITOR ==='; while true; do clear; echo 'CPU & Memory:'; top -bn1 | head -5; echo; echo 'Disk Usage:'; df -h / | tail -1; echo; echo 'All Services:'; ps aux | grep -E '(qdrant|ollama|rclone|streamlit|nginx)' | grep -v grep; echo; echo 'Updated:' \$(date); sleep 3; done; exec bash" \
    --tab --title="Logs" \
    -- bash -c "echo '=== ALL LOGS ==='; tail -f ~/workspace/logs/*.log 2>/dev/null || echo 'No log files found'; exec bash" \
    --tab --title="Shell" \
    -- bash -c "echo '=== INTERACTIVE SHELL ==='; cd '/media/udonsi-kalu/New Volume/denials/cms/manuals/rag'; source '/media/udonsi-kalu/New Volume/denials/faiss_gpu1/bin/activate' 2>/dev/null && echo 'Virtual environment activated' || echo 'Virtual environment not found'; echo 'Ready to work!'; exec bash"

echo "✅ Monitoring terminal opened with 7 tabs!"
echo ""
echo "🌐 Your Web Applications:"
echo "   Streamlit App 1: http://localhost:8502"
echo "   Streamlit App 2: http://localhost:8509"
echo "   Qdrant Dashboard: http://localhost:6333/dashboard"
