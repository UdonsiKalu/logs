#!/bin/bash
# Start All Services in Terminal Tabs
# Opens all services in separate tabs within a single terminal window

# Configuration
DENIALS_DIR="/media/udonsi-kalu/New Volume/denials"
RAG_DIR="$DENIALS_DIR/cms/manuals/rag"
VENV_PATH="$DENIALS_DIR/faiss_gpu1/bin/activate"
TERMINAL_CMD="gnome-terminal"

# Function to create a new tab with a command
create_tab() {
    local title="$1"
    local command="$2"
    local working_dir="$3"
    
    if [ -n "$working_dir" ]; then
        $TERMINAL_CMD --tab --title="$title" --working-directory="$working_dir" -- bash -c "$command; exec bash"
    else
        $TERMINAL_CMD --tab --title="$title" -- bash -c "$command; exec bash"
    fi
}

echo " Starting Terminal Workspace with all services..."

# Wait a moment for the first tab to be ready
sleep 1

# Tab 1: Qdrant (Docker)
create_tab "Qdrant" "echo 'Starting Qdrant...' && ~/workspace/startup-scripts/start_qdrant.sh && echo 'Qdrant started! Press Ctrl+C to stop.' && sleep infinity"

# Tab 2: Rclone Sync
create_tab "Rclone" "echo 'Starting Rclone sync...' && ~/workspace/startup-scripts/start_rclone.sh && echo 'Rclone started! Press Ctrl+C to stop.' && sleep infinity"

# Tab 3: Streamlit App 1 (streamlit_app4.py)
create_tab "Streamlit App 1" "echo 'Starting Streamlit App 1...' && cd '$RAG_DIR' && source '$VENV_PATH' && streamlit run streamlit_app4.py --server.port 8502 --server.headless false"

# Tab 4: Streamlit App 2 (complete_claim_analysis)
create_tab "Streamlit App 2" "echo 'Starting Streamlit App 2...' && cd '$RAG_DIR' && source '$VENV_PATH' && streamlit run complete_claim_analysis_app_cgpt3_update7.py --server.port 8509 --server.headless false"

# Tab 5: GPU Monitoring (nvtop)
create_tab "GPU Monitor" "echo 'Starting GPU Monitor...' && nvtop"

# Tab 6: System Health Monitor
create_tab "Health Monitor" "echo 'Starting Health Monitor...' && ~/workspace/startup-scripts/start_health_monitor.sh && echo 'Health monitoring active. Press Ctrl+C to stop.' && sleep infinity"

# Tab 7: Log Viewer
create_tab "Log Viewer" "echo 'Log Viewer - Press Ctrl+C to exit' && tail -f ~/workspace/logs/*.log"

# Tab 8: Database Monitor
create_tab "Database" "echo 'Database Status...' && ~/workspace/startup-scripts/start_database.sh && echo 'Database monitoring active. Press Ctrl+C to stop.' && sleep infinity"

# Tab 9: System Overview
create_tab "System Overview" "echo 'System Overview - Press Ctrl+C to exit' && while true; do clear; echo '=== SYSTEM OVERVIEW ==='; echo; echo 'Services:'; ps aux | grep -E '(qdrant|ollama|rclone|streamlit|nginx)' | grep -v grep; echo; echo 'Ports:'; netstat -tlnp | grep -E ':(6333|6334|8502|8509|11434|80)' | head -10; echo; echo 'GPU:'; nvidia-smi --query-gpu=index,name,utilization.gpu,memory.used,memory.total,temperature.gpu --format=csv,noheader,nounits 2>/dev/null || echo 'nvidia-smi not available'; echo; echo 'Updated:' $(date); sleep 5; done"

# Tab 10: Interactive Shell
create_tab "Shell" "echo 'Interactive Shell - Ready for commands' && cd '$RAG_DIR' && source '$VENV_PATH' && echo 'Virtual environment activated. Ready to work!'"

echo " Terminal workspace created with 10 tabs:"
echo "   Tab 1: Qdrant (Docker container)"
echo "   Tab 2: Rclone (Cloud sync)"
echo "   Tab 3: Streamlit App 1 (Port 8502)"
echo "   Tab 4: Streamlit App 2 (Port 8509)"
echo "   Tab 5: GPU Monitor (nvtop)"
echo "   Tab 6: Health Monitor"
echo "   Tab 7: Log Viewer (all logs)"
echo "   Tab 8: Database Monitor"
echo "   Tab 9: System Overview"
echo "   Tab 10: Interactive Shell"
echo ""
echo " Web Applications:"
echo "   Streamlit App 1: http://localhost:8502"
echo "   Streamlit App 2: http://localhost:8509"
echo "   Qdrant Dashboard: http://localhost:6333/dashboard"
echo ""
echo " All services are now running in separate terminal tabs!"

