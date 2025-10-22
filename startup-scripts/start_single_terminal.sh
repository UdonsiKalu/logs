#!/bin/bash
# Start All Services in ONE Terminal with Multiple Tabs
# This ensures all services run in a single terminal window with tabs

# Configuration
DENIALS_DIR="/media/udonsi-kalu/New Volume/denials"
RAG_DIR="$DENIALS_DIR/cms/manuals/rag"
VENV_PATH="$DENIALS_DIR/faiss_gpu1/bin/activate"

echo " Starting ALL services in ONE terminal with tabs..."

# Start the first tab with Qdrant and then add other tabs
gnome-terminal --title="Workspace - All Services" \
    --tab --title="Qdrant" --working-directory="$HOME" \
    -- bash -c "echo 'Starting Qdrant...'; ~/workspace/startup-scripts/start_qdrant.sh; echo 'Qdrant running! Press Ctrl+C to stop.'; sleep infinity" \
    --tab --title="Rclone" --working-directory="$HOME" \
    -- bash -c "echo 'Starting Rclone...'; ~/workspace/startup-scripts/start_rclone.sh; echo 'Rclone running! Press Ctrl+C to stop.'; sleep infinity" \
    --tab --title="Streamlit-1" --working-directory="$RAG_DIR" \
    -- bash -c "echo 'Starting Streamlit App 1...'; source '$VENV_PATH'; streamlit run streamlit_app4.py --server.port 8502; exec bash" \
    --tab --title="Streamlit-2" --working-directory="$RAG_DIR" \
    -- bash -c "echo 'Starting Streamlit App 2...'; source '$VENV_PATH'; streamlit run complete_claim_analysis_app_cgpt3_update7.py --server.port 8509; exec bash" \
    --tab --title="GPU Monitor" --working-directory="$HOME" \
    -- bash -c "echo 'Starting GPU Monitor...'; nvtop; exec bash" \
    --tab --title="Health Monitor" --working-directory="$HOME" \
    -- bash -c "echo 'Starting Health Monitor...'; ~/workspace/startup-scripts/start_health_monitor.sh; echo 'Health monitoring active!'; sleep infinity" \
    --tab --title="Log Viewer" --working-directory="$HOME" \
    -- bash -c "echo 'Log Viewer - All logs'; tail -f ~/workspace/logs/*.log; exec bash" \
    --tab --title="Database" --working-directory="$HOME" \
    -- bash -c "echo 'Database Monitor...'; ~/workspace/startup-scripts/start_database.sh; echo 'Database monitoring active!'; sleep infinity" \
    --tab --title="System Overview" --working-directory="$HOME" \
    -- bash -c "echo 'System Overview - Live stats'; while true; do clear; echo '=== SYSTEM OVERVIEW ==='; echo; echo 'Services:'; ps aux | grep -E '(qdrant|ollama|rclone|streamlit|nginx)' | grep -v grep; echo; echo 'Ports:'; netstat -tlnp | grep -E ':(6333|6334|8502|8509|11434|80)' | head -10; echo; echo 'GPU:'; nvidia-smi --query-gpu=index,name,utilization.gpu,memory.used,memory.total,temperature.gpu --format=csv,noheader,nounits 2>/dev/null || echo 'nvidia-smi not available'; echo; echo 'Updated:' \$(date); sleep 5; done; exec bash" \
    --tab --title="Shell" --working-directory="$RAG_DIR" \
    -- bash -c "echo 'Interactive Shell - Ready!'; source '$VENV_PATH'; echo 'Virtual environment activated. Ready to work!'; exec bash"

echo " All services started in ONE terminal with 10 tabs!"
echo ""
echo " Web Applications:"
echo "   Streamlit App 1: http://localhost:8502"
echo "   Streamlit App 2: http://localhost:8509"
echo "   Qdrant Dashboard: http://localhost:6333/dashboard"
echo ""
echo " Tab Navigation:"
echo "   - Click on tab names to switch"
echo "   - Or use Ctrl+PageUp/PageDown"
echo "   - Each tab runs a different service"

