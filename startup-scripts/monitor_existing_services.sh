#!/bin/bash
# Monitor Existing Services in ONE Terminal with Tabs
# This shows the status of your already running services without starting new ones

# Configuration
DENIALS_DIR="/media/udonsi-kalu/New Volume/denials"
RAG_DIR="$DENIALS_DIR/cms/manuals/rag"
VENV_PATH="$DENIALS_DIR/faiss_gpu1/bin/activate"

echo "🔍 Opening monitoring terminal for your existing services..."

# Create ONE terminal with tabs to monitor existing services
gnome-terminal --title="Service Monitor - Existing Services" \
    --tab --title="Qdrant Monitor" --working-directory="$HOME" \
    -- bash -c "echo 'Monitoring Qdrant...'; echo 'Qdrant Status:'; docker ps | grep qdrant; echo; echo 'Qdrant Logs:'; docker logs --tail=20 \$(docker ps --format '{{.Names}}' | grep qdrant | head -1) 2>/dev/null || echo 'No Qdrant container found'; echo; echo 'Press Ctrl+C to exit'; sleep infinity" \
    --tab --title="Rclone Monitor" --working-directory="$HOME" \
    -- bash -c "echo 'Monitoring Rclone...'; echo 'Rclone Status:'; ps aux | grep rclone | grep -v grep; echo; echo 'Rclone Logs:'; tail -f ~/workspace/logs/rclone.log 2>/dev/null || echo 'No rclone logs found'; exec bash" \
    --tab --title="Streamlit Monitor" --working-directory="$HOME" \
    -- bash -c "echo 'Monitoring Streamlit Apps...'; echo 'Streamlit Status:'; ps aux | grep streamlit | grep -v grep; echo; echo 'Port Status:'; netstat -tlnp | grep -E ':(8502|8509)'; echo; echo 'Press Ctrl+C to exit'; sleep infinity" \
    --tab --title="GPU Monitor" --working-directory="$HOME" \
    -- bash -c "echo 'GPU Status:'; nvidia-smi --query-gpu=index,name,utilization.gpu,memory.used,memory.total,temperature.gpu --format=csv,noheader,nounits 2>/dev/null || echo 'nvidia-smi not available'; echo; nvtop; exec bash" \
    --tab --title="System Health" --working-directory="$HOME" \
    -- bash -c "echo 'System Health Monitor:'; while true; do clear; echo '=== SYSTEM HEALTH ==='; echo; echo 'CPU Usage:'; top -bn1 | grep 'Cpu(s)' | awk '{print \$2}'; echo; echo 'Memory Usage:'; free -h; echo; echo 'Disk Usage:'; df -h / | tail -1; echo; echo 'Running Services:'; ps aux | grep -E '(qdrant|ollama|rclone|streamlit|nginx)' | grep -v grep; echo; echo 'Updated:' \$(date); sleep 5; done; exec bash" \
    --tab --title="All Logs" --working-directory="$HOME" \
    -- bash -c "echo 'All Service Logs:'; echo 'Available log files:'; ls -la ~/workspace/logs/ 2>/dev/null || echo 'No logs directory found'; echo; echo 'Monitoring all logs:'; tail -f ~/workspace/logs/*.log 2>/dev/null || echo 'No log files found'; exec bash" \
    --tab --title="Database Status" --working-directory="$HOME" \
    -- bash -c "echo 'Database Status:'; ~/workspace/startup-scripts/start_database.sh; echo; echo 'Press Ctrl+C to exit'; sleep infinity" \
    --tab --title="Port Monitor" --working-directory="$HOME" \
    -- bash -c "echo 'Port Status Monitor:'; while true; do clear; echo '=== PORT STATUS ==='; echo; netstat -tlnp | grep -E ':(6333|6334|8502|8509|11434|80|1433)' | head -20; echo; echo 'Updated:' \$(date); sleep 3; done; exec bash" \
    --tab --title="Interactive Shell" --working-directory="$RAG_DIR" \
    -- bash -c "echo 'Interactive Shell - Ready!'; source '$VENV_PATH' 2>/dev/null && echo 'Virtual environment activated' || echo 'Virtual environment not found'; echo 'Ready to work!'; exec bash"

echo "✅ Monitoring terminal opened with 9 tabs!"
echo ""
echo "🌐 Your Running Web Applications:"
echo "   Streamlit App 1: http://localhost:8502"
echo "   Streamlit App 2: http://localhost:8509"
echo "   Qdrant Dashboard: http://localhost:6333/dashboard"
echo ""
echo "📋 Each tab monitors a different aspect of your running services"
echo "   - No new services are started"
echo "   - Just monitoring what's already running"
