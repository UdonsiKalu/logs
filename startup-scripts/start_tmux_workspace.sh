#!/bin/bash
# Start All Services in TMUX Session
# Creates a tmux session with multiple windows for all services

# Configuration
DENIALS_DIR="/media/udonsi-kalu/New Volume/denials"
RAG_DIR="$DENIALS_DIR/cms/manuals/rag"
VENV_PATH="$DENIALS_DIR/faiss_gpu1/bin/activate"
SESSION_NAME="workspace"

echo "🚀 Starting TMUX Workspace with all services..."

# Kill existing session if it exists
tmux kill-session -t $SESSION_NAME 2>/dev/null

# Create new tmux session
tmux new-session -d -s $SESSION_NAME -n "main"

# Window 1: Qdrant
tmux new-window -t $SESSION_NAME:1 -n "Qdrant"
tmux send-keys -t $SESSION_NAME:1 "echo 'Starting Qdrant...'" C-m
tmux send-keys -t $SESSION_NAME:1 "~/workspace/startup-scripts/start_qdrant.sh" C-m
tmux send-keys -t $SESSION_NAME:1 "echo 'Qdrant started! Press Ctrl+C to stop.'" C-m

# Window 2: Rclone
tmux new-window -t $SESSION_NAME:2 -n "Rclone"
tmux send-keys -t $SESSION_NAME:2 "echo 'Starting Rclone...'" C-m
tmux send-keys -t $SESSION_NAME:2 "~/workspace/startup-scripts/start_rclone.sh" C-m
tmux send-keys -t $SESSION_NAME:2 "echo 'Rclone started! Press Ctrl+C to stop.'" C-m

# Window 3: Streamlit App 1
tmux new-window -t $SESSION_NAME:3 -n "Streamlit-1"
tmux send-keys -t $SESSION_NAME:3 "echo 'Starting Streamlit App 1...'" C-m
tmux send-keys -t $SESSION_NAME:3 "cd '$RAG_DIR'" C-m
tmux send-keys -t $SESSION_NAME:3 "source '$VENV_PATH'" C-m
tmux send-keys -t $SESSION_NAME:3 "streamlit run streamlit_app4.py --server.port 8502" C-m

# Window 4: Streamlit App 2
tmux new-window -t $SESSION_NAME:4 -n "Streamlit-2"
tmux send-keys -t $SESSION_NAME:4 "echo 'Starting Streamlit App 2...'" C-m
tmux send-keys -t $SESSION_NAME:4 "cd '$RAG_DIR'" C-m
tmux send-keys -t $SESSION_NAME:4 "source '$VENV_PATH'" C-m
tmux send-keys -t $SESSION_NAME:4 "streamlit run complete_claim_analysis_app_cgpt3_update7.py --server.port 8509" C-m

# Window 5: GPU Monitor
tmux new-window -t $SESSION_NAME:5 -n "GPU"
tmux send-keys -t $SESSION_NAME:5 "echo 'Starting GPU Monitor...'" C-m
tmux send-keys -t $SESSION_NAME:5 "nvtop" C-m

# Window 6: Health Monitor
tmux new-window -t $SESSION_NAME:6 -n "Health"
tmux send-keys -t $SESSION_NAME:6 "echo 'Starting Health Monitor...'" C-m
tmux send-keys -t $SESSION_NAME:6 "~/workspace/startup-scripts/start_health_monitor.sh" C-m
tmux send-keys -t $SESSION_NAME:6 "echo 'Health monitoring active.'" C-m

# Window 7: Log Viewer
tmux new-window -t $SESSION_NAME:7 -n "Logs"
tmux send-keys -t $SESSION_NAME:7 "echo 'Log Viewer - Press Ctrl+C to exit'" C-m
tmux send-keys -t $SESSION_NAME:7 "tail -f ~/workspace/logs/*.log" C-m

# Window 8: Database Monitor
tmux new-window -t $SESSION_NAME:8 -n "Database"
tmux send-keys -t $SESSION_NAME:8 "echo 'Database Status...'" C-m
tmux send-keys -t $SESSION_NAME:8 "~/workspace/startup-scripts/start_database.sh" C-m
tmux send-keys -t $SESSION_NAME:8 "echo 'Database monitoring active.'" C-m

# Window 9: System Overview
tmux new-window -t $SESSION_NAME:9 -n "Overview"
tmux send-keys -t $SESSION_NAME:9 "echo 'System Overview - Press Ctrl+C to exit'" C-m
tmux send-keys -t $SESSION_NAME:9 "while true; do clear; echo '=== SYSTEM OVERVIEW ==='; echo; echo 'Services:'; ps aux | grep -E '(qdrant|ollama|rclone|streamlit|nginx)' | grep -v grep; echo; echo 'Ports:'; netstat -tlnp | grep -E ':(6333|6334|8502|8509|11434|80)' | head -10; echo; echo 'GPU:'; nvidia-smi --query-gpu=index,name,utilization.gpu,memory.used,memory.total,temperature.gpu --format=csv,noheader,nounits 2>/dev/null || echo 'nvidia-smi not available'; echo; echo 'Updated:' \$(date); sleep 5; done" C-m

# Window 10: Interactive Shell
tmux new-window -t $SESSION_NAME:10 -n "Shell"
tmux send-keys -t $SESSION_NAME:10 "echo 'Interactive Shell - Ready for commands'" C-m
tmux send-keys -t $SESSION_NAME:10 "cd '$RAG_DIR'" C-m
tmux send-keys -t $SESSION_NAME:10 "source '$VENV_PATH'" C-m
tmux send-keys -t $SESSION_NAME:10 "echo 'Virtual environment activated. Ready to work!'" C-m

# Select the first window and attach
tmux select-window -t $SESSION_NAME:1

echo "✅ TMUX workspace created with 10 windows:"
echo "   Window 1: Qdrant (Docker container)"
echo "   Window 2: Rclone (Cloud sync)"
echo "   Window 3: Streamlit App 1 (Port 8502)"
echo "   Window 4: Streamlit App 2 (Port 8509)"
echo "   Window 5: GPU Monitor (nvtop)"
echo "   Window 6: Health Monitor"
echo "   Window 7: Log Viewer (all logs)"
echo "   Window 8: Database Monitor"
echo "   Window 9: System Overview"
echo "   Window 10: Interactive Shell"
echo ""
echo "🌐 Web Applications:"
echo "   Streamlit App 1: http://localhost:8502"
echo "   Streamlit App 2: http://localhost:8509"
echo "   Qdrant Dashboard: http://localhost:6333/dashboard"
echo ""
echo "📋 TMUX Commands:"
echo "   Attach to session: tmux attach-session -t $SESSION_NAME"
echo "   List windows: Ctrl+b, w"
echo "   Switch windows: Ctrl+b, 0-9"
echo "   Detach: Ctrl+b, d"
echo ""
echo "🚀 Attaching to TMUX session..."

# Attach to the session
tmux attach-session -t $SESSION_NAME

