#!/bin/bash
# Master startup script for all services
# Usage: ~/workspace/startup-scripts/start_all.sh

echo " Starting all services..."
echo "================================"

# Start Qdrant
echo " Starting Qdrant..."
~/workspace/startup-scripts/start_qdrant.sh

# Start Rclone
echo " Starting Rclone..."
~/workspace/startup-scripts/start_rclone.sh

# Start Streamlit Apps
echo " Starting Streamlit Apps..."
~/workspace/startup-scripts/start_streamlit_apps.sh

# Start Streamlit Hub Launcher
echo " Starting Streamlit Hub Launcher..."
~/workspace/startup-scripts/start_streamlit_hub.sh

# Start Database Services
echo " Starting Database Services..."
~/workspace/startup-scripts/start_database.sh

# Start GPU Monitoring
echo " Starting GPU Monitoring..."
~/workspace/startup-scripts/start_gpu_monitor.sh

# Start Log Management
echo " Starting Log Management..."
~/workspace/startup-scripts/start_log_management.sh

# Start Health Monitoring
echo " Starting Health Monitoring..."
~/workspace/startup-scripts/start_health_monitor.sh

# Add more services here as needed
# echo " Starting Claims Analyzer..."
# ~/workspace/startup-scripts/start_claims_analyzer.sh

# echo " Starting Retrieval Studio..."
# ~/workspace/startup-scripts/start_retrieval_studio.sh

echo "================================"
echo " All services started!"
echo " Check logs: ~/workspace/logs/"
echo ""
echo "  Terminal Workspace Options:"
echo "   Terminal Tabs: ~/workspace/startup-scripts/start_terminal_workspace.sh"
echo "   TMUX Session:  ~/workspace/startup-scripts/start_tmux_workspace.sh"
echo ""
echo " Web Applications:"
echo "   Streamlit Hub: http://localhost:8500/app"
echo "   Streamlit App 1: http://localhost:8502"
echo "   Streamlit App 2: http://localhost:8509"
echo "   Qdrant Dashboard: http://localhost:6333/dashboard"
