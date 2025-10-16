#!/bin/bash
# Start All Services in Screen Sessions
# Creates multiple screen sessions for monitoring all services

# Configuration
DENIALS_DIR="/media/udonsi-kalu/New Volume/denials"
RAG_DIR="$DENIALS_DIR/cms/manuals/rag"
VENV_PATH="$DENIALS_DIR/faiss_gpu1/bin/activate"

echo "🚀 Starting Screen Workspace with all services..."

# Kill existing screen sessions if they exist
screen -S qdrant_monitor -X quit 2>/dev/null
screen -S rclone_monitor -X quit 2>/dev/null
screen -S streamlit_monitor -X quit 2>/dev/null
screen -S gpu_monitor -X quit 2>/dev/null
screen -S system_monitor -X quit 2>/dev/null
screen -S logs_monitor -X quit 2>/dev/null
screen -S shell_session -X quit 2>/dev/null

# Create screen sessions for each service
echo "Creating screen sessions..."

# Screen 1: Qdrant Monitor
screen -dmS qdrant_monitor bash -c "
echo '=== QDRANT MONITOR ===';
while true; do
    clear;
    echo 'Qdrant Container:';
    docker ps | grep qdrant;
    echo;
    echo 'Qdrant Logs (last 10 lines):';
    docker logs --tail=10 \$(docker ps -q --filter ancestor=qdrant/qdrant:latest) 2>/dev/null || echo 'No logs available';
    echo;
    echo 'Updated:' \$(date);
    sleep 5;
done
"

# Screen 2: Rclone Monitor
screen -dmS rclone_monitor bash -c "
echo '=== RCLONE MONITOR ===';
while true; do
    clear;
    echo 'Rclone Process:';
    ps aux | grep rclone | grep -v grep;
    echo;
    echo 'Rclone Logs:';
    tail -5 ~/workspace/logs/rclone.log 2>/dev/null || echo 'No logs found';
    echo;
    echo 'Updated:' \$(date);
    sleep 5;
done
"

# Screen 3: Streamlit Monitor
screen -dmS streamlit_monitor bash -c "
echo '=== STREAMLIT MONITOR ===';
while true; do
    clear;
    echo 'Streamlit Processes:';
    ps aux | grep streamlit | grep -v grep;
    echo;
    echo 'Ports 8502 & 8509:';
    netstat -tlnp | grep -E ':(8502|8509)';
    echo;
    echo 'Updated:' \$(date);
    sleep 5;
done
"

# Screen 4: GPU Monitor
screen -dmS gpu_monitor bash -c "
echo '=== GPU MONITOR ===';
nvtop
"

# Screen 5: System Monitor
screen -dmS system_monitor bash -c "
echo '=== SYSTEM MONITOR ===';
while true; do
    clear;
    echo 'CPU & Memory:';
    top -bn1 | head -5;
    echo;
    echo 'Disk Usage:';
    df -h / | tail -1;
    echo;
    echo 'All Services:';
    ps aux | grep -E '(qdrant|ollama|rclone|streamlit|nginx)' | grep -v grep;
    echo;
    echo 'Updated:' \$(date);
    sleep 3;
done
"

# Screen 6: Logs Monitor
screen -dmS logs_monitor bash -c "
echo '=== ALL LOGS ===';
tail -f ~/workspace/logs/*.log 2>/dev/null || echo 'No log files found'
"

# Screen 7: Interactive Shell
screen -dmS shell_session bash -c "
echo '=== INTERACTIVE SHELL ===';
cd '$RAG_DIR';
source '$VENV_PATH' 2>/dev/null && echo 'Virtual environment activated' || echo 'Virtual environment not found';
echo 'Ready to work!';
bash
"

echo "✅ Screen workspace created with 7 sessions!"
echo ""
echo "📋 Available Screen Sessions:"
echo "   1. qdrant_monitor    - Qdrant container and logs"
echo "   2. rclone_monitor    - Rclone process and sync logs"
echo "   3. streamlit_monitor - Streamlit apps and ports"
echo "   4. gpu_monitor       - GPU monitoring with nvtop"
echo "   5. system_monitor    - System health and resources"
echo "   6. logs_monitor      - All service logs"
echo "   7. shell_session     - Interactive shell ready for work"
echo ""
echo "🔧 How to Use:"
echo "   List sessions:    screen -list"
echo "   Attach to session: screen -r <session_name>"
echo "   Detach from session: Ctrl+A, then D"
echo "   Kill session:     screen -S <session_name> -X quit"
echo ""
echo "🌐 Your Web Applications:"
echo "   Streamlit App 1: http://localhost:8502"
echo "   Streamlit App 2: http://localhost:8509"
echo "   Qdrant Dashboard: http://localhost:6333/dashboard"
echo ""
echo "🚀 Quick Start:"
echo "   screen -r qdrant_monitor    # Monitor Qdrant"
echo "   screen -r system_monitor    # Monitor system"
echo "   screen -r shell_session     # Interactive shell"
