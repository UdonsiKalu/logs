#!/bin/bash
# Auto-start monitoring workspace with screen sessions
# This script will be called at login to create monitoring windows

LOG_DIR="$HOME/workspace/logs"
mkdir -p "$LOG_DIR"

# Function to check if screen session exists
session_exists() {
    screen -list | grep -q "monitor_workspace"
}

# Function to create a new screen session with monitoring windows
create_monitor_session() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Creating monitor workspace with screen..."
    
    # Create the main screen session
    screen -dmS monitor_workspace
    
    # Window 1: Qdrant Container Monitor
    screen -S monitor_workspace -X screen -t "Qdrant" bash -c "
        echo ' Qdrant Container Monitor'
        echo '========================'
        while true; do
            clear
            echo ' Qdrant Container Monitor - $(date)'
            echo '====================================='
            echo ''
            echo 'Container Status:'
            docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' | grep -E '(qdrant|NAMES)'
            echo ''
            echo 'Recent Logs (last 10 lines):'
            docker logs --tail 10 qdrant 2>/dev/null || echo 'Container not found or not running'
            echo ''
            echo 'Press Ctrl+C to exit this monitor'
            sleep 5
        done
    "
    
    # Window 2: Rclone Monitor
    screen -S monitor_workspace -X screen -t "Rclone" bash -c "
        echo '  Rclone Sync Monitor'
        echo '====================='
        while true; do
            clear
            echo '  Rclone Sync Monitor - $(date)'
            echo '================================='
            echo ''
            if [ -f $LOG_DIR/rclone.pid ]; then
                PID=\$(cat $LOG_DIR/rclone.pid)
                if ps -p \$PID > /dev/null; then
                    echo 'Status:  Running (PID: '\$PID')'
                    echo 'Recent Activity:'
                    tail -5 $LOG_DIR/rclone.log 2>/dev/null || echo 'No recent logs'
                else
                    echo 'Status:  Not running (stale PID file)'
                fi
            else
                echo 'Status:  Not running (no PID file)'
            fi
            echo ''
            echo 'Press Ctrl+C to exit this monitor'
            sleep 10
        done
    "
    
    # Window 3: Streamlit Apps Monitor
    screen -S monitor_workspace -X screen -t "Streamlit" bash -c "
        echo ' Streamlit Apps Monitor'
        echo '========================'
        while true; do
            clear
            echo ' Streamlit Apps Monitor - $(date)'
            echo '=================================='
            echo ''
            echo 'App 1 (Port 8509):'
            if [ -f $LOG_DIR/streamlit_app1.pid ]; then
                PID=\$(cat $LOG_DIR/streamlit_app1.pid)
                if ps -p \$PID > /dev/null; then
                    echo '  Status:  Running (PID: '\$PID')'
                    echo '  URL: http://localhost:8509'
                else
                    echo '  Status:  Not running'
                fi
            else
                echo '  Status:  Not running'
            fi
            echo ''
            echo 'App 2 (Port 8502):'
            if [ -f $LOG_DIR/streamlit_app2.pid ]; then
                PID=\$(cat $LOG_DIR/streamlit_app2.pid)
                if ps -p \$PID > /dev/null; then
                    echo '  Status:  Running (PID: '\$PID')'
                    echo '  URL: http://localhost:8502'
                else
                    echo '  Status:  Not running'
                fi
            else
                echo '  Status:  Not running'
            fi
            echo ''
            echo 'Press Ctrl+C to exit this monitor'
            sleep 10
        done
    "
    
    # Window 4: GPU Monitor (nvidia-smi)
    screen -S monitor_workspace -X screen -t "GPU" bash -c "
        echo ' GPU Monitor (nvidia-smi)'
        echo '==========================='
        while true; do
            clear
            echo ' GPU Monitor (nvidia-smi) - $(date)'
            echo '====================================='
            echo ''
            if command -v nvidia-smi >/dev/null 2>&1; then
                nvidia-smi
            else
                echo 'nvidia-smi not found. Installing NVIDIA drivers...'
                echo 'Please run: sudo apt update && sudo apt install nvidia-driver-525'
            fi
            echo ''
            echo 'Press Ctrl+C to exit this monitor'
            sleep 5
        done
    "
    
    # Window 5: GPU Monitor (nvtop)
    screen -S monitor_workspace -X screen -t "nvtop" bash -c "
        echo ' GPU Monitor (nvtop)'
        echo '======================'
        if command -v nvtop >/dev/null 2>&1; then
            nvtop
        else
            echo 'nvtop not installed. Installing...'
            sudo apt update && sudo apt install -y nvtop
            nvtop
        fi
    "
    
    # Window 6: System Health Monitor
    screen -S monitor_workspace -X screen -t "Health" bash -c "
        echo ' System Health Monitor'
        echo '========================'
        while true; do
            clear
            echo ' System Health Monitor - $(date)'
            echo '=================================='
            echo ''
            echo 'CPU Usage:'
            top -bn1 | grep 'Cpu(s)' | awk '{print \$2}' | sed 's/%us,//'
            echo ''
            echo 'Memory Usage:'
            free -h | grep -E '(Mem|Swap)'
            echo ''
            echo 'Disk Usage:'
            df -h | grep -E '^/dev/'
            echo ''
            echo 'Running Services:'
            systemctl --user is-active qdrant.service rclone.service streamlit.service health_monitor.service 2>/dev/null | grep -E '(active|inactive)'
            echo ''
            echo 'Press Ctrl+C to exit this monitor'
            sleep 5
        done
    "
    
    # Window 7: Log Viewer
    screen -S monitor_workspace -X screen -t "Logs" bash -c "
        echo ' Log Viewer'
        echo '============='
        while true; do
            clear
            echo ' Log Viewer - $(date)'
            echo '======================'
            echo ''
            echo 'Recent logs from all services:'
            echo '=============================='
            find $LOG_DIR -name '*.log' -type f -exec basename {} \; | head -10
            echo ''
            echo 'Latest log entries:'
            find $LOG_DIR -name '*.log' -type f -exec tail -2 {} \; 2>/dev/null | head -20
            echo ''
            echo 'Press Ctrl+C to exit this monitor'
            sleep 10
        done
    "
    
    # Window 8: Interactive Shell
    screen -S monitor_workspace -X screen -t "Shell" bash -c "
        echo ' Interactive Shell'
        echo '==================='
        echo 'Welcome to the monitoring workspace!'
        echo ''
        echo 'Available commands:'
        echo '  screen -r monitor_workspace  # Attach to this session'
        echo '  screen -list                 # List all sessions'
        echo '  Ctrl+A, 0-7                  # Switch between windows'
        echo '  Ctrl+A, d                    # Detach from session'
        echo ''
        echo 'Web Applications:'
        echo '  Streamlit App 1: http://localhost:8509'
        echo '  Streamlit App 2: http://localhost:8502'
        echo '  Qdrant Dashboard: http://localhost:6333/dashboard'
        echo ''
        bash
    "
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')]  Monitor workspace created successfully!"
    echo ""
    echo " Web Applications:"
    echo "   Streamlit App 1: http://localhost:8509"
    echo "   Streamlit App 2: http://localhost:8502"
    echo "   Qdrant Dashboard: http://localhost:6333/dashboard"
    echo ""
    echo " Screen Commands:"
    echo "   Attach to session: screen -r monitor_workspace"
    echo "   List windows: Ctrl+A, w"
    echo "   Switch windows: Ctrl+A, 0-7"
    echo "   Detach: Ctrl+A, d"
    echo ""
    echo " Attaching to monitor workspace..."
    sleep 2
    screen -r monitor_workspace
}

# Main execution
if session_exists; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Monitor workspace already exists. Attaching..."
    screen -r monitor_workspace
else
    create_monitor_session
fi
