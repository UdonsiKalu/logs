#!/bin/bash
# Start Monitoring in Separate Terminal Windows
# Each service gets its own terminal window, then you can use + to add tabs

# Configuration
DENIALS_DIR="/media/udonsi-kalu/New Volume/denials"
RAG_DIR="$DENIALS_DIR/cms/manuals/rag"
VENV_PATH="$DENIALS_DIR/faiss_gpu1/bin/activate"

echo " Opening monitoring terminals for each service..."

# Terminal 1: Qdrant Monitor
gnome-terminal --title="Qdrant Monitor" --working-directory="$HOME" \
    -- bash -c "
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
" &

# Terminal 2: System Monitor
gnome-terminal --title="System Monitor" --working-directory="$HOME" \
    -- bash -c "
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
" &

# Terminal 3: GPU Monitor
gnome-terminal --title="GPU Monitor" --working-directory="$HOME" \
    -- bash -c "
echo '=== GPU MONITOR ===';
nvtop
" &

# Terminal 4: Rclone Monitor
gnome-terminal --title="Rclone Monitor" --working-directory="$HOME" \
    -- bash -c "
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
" &

# Terminal 5: Streamlit Monitor
gnome-terminal --title="Streamlit Monitor" --working-directory="$HOME" \
    -- bash -c "
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
" &

# Terminal 6: Logs Monitor
gnome-terminal --title="Logs Monitor" --working-directory="$HOME" \
    -- bash -c "
echo '=== ALL LOGS ===';
tail -f ~/workspace/logs/*.log 2>/dev/null || echo 'No log files found'
" &

# Terminal 7: Interactive Shell
gnome-terminal --title="Work Shell" --working-directory="$RAG_DIR" \
    -- bash -c "
echo '=== INTERACTIVE SHELL ===';
source '$VENV_PATH' 2>/dev/null && echo 'Virtual environment activated' || echo 'Virtual environment not found';
echo 'Ready to work!';
bash
" &

echo " Opened 7 terminal windows!"
echo ""
echo " Each terminal monitors a different service:"
echo "   1. Qdrant Monitor    - Container status and logs"
echo "   2. System Monitor    - CPU, memory, disk, services"
echo "   3. GPU Monitor       - Live GPU stats with nvtop"
echo "   4. Rclone Monitor    - Sync status and logs"
echo "   5. Streamlit Monitor - Apps and port status"
echo "   6. Logs Monitor      - All service logs"
echo "   7. Work Shell        - Interactive shell ready for work"
echo ""
echo " Now you can:"
echo "   - Use the + button to add new tabs to any terminal"
echo "   - Click between tabs to see different services"
echo "   - Organize tabs however you want"
echo "   - No need to detach/attach - just click!"
echo ""
echo " Your Web Applications:"
echo "   Streamlit App 1: http://localhost:8502"
echo "   Streamlit App 2: http://localhost:8509"
echo "   Qdrant Dashboard: http://localhost:6333/dashboard"

