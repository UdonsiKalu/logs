#!/bin/bash
# Start GPU Monitoring Service
# Monitors GPU usage and performance for ML workloads

LOG_FILE="$HOME/workspace/logs/gpu_monitor.log"

# Create logs directory if it doesn't exist
mkdir -p "$(dirname "$LOG_FILE")"

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "Starting GPU monitoring service..."

# Check if nvidia-smi is available
if ! command -v nvidia-smi &> /dev/null; then
    log " nvidia-smi not found - GPU monitoring limited"
else
    # Get GPU information
    GPU_COUNT=$(nvidia-smi --list-gpus | wc -l)
    log " Found $GPU_COUNT GPU(s)"
    
    # Display GPU status
    nvidia-smi --query-gpu=index,name,memory.used,memory.total,utilization.gpu --format=csv,noheader,nounits | while IFS=',' read -r gpu_id name mem_used mem_total gpu_util; do
        log "GPU $gpu_id: $name - Memory: ${mem_used}MB/${mem_total}MB - Utilization: ${gpu_util}%"
    done
fi

# Check if nvtop is available and start it
if command -v nvtop &> /dev/null; then
    log "Starting nvtop GPU monitor..."
    nohup nvtop > "$LOG_FILE.nvtop" 2>&1 &
    NVTOP_PID=$!
    echo $NVTOP_PID > "$HOME/workspace/logs/nvtop.pid"
    log " nvtop started with PID: $NVTOP_PID"
else
    log " nvtop not installed - install with: sudo apt install nvtop"
fi

# Check if nvidia-ml-py is available for Python monitoring
if python3 -c "import pynvml" 2>/dev/null; then
    log " nvidia-ml-py available for Python GPU monitoring"
else
    log "ℹ️ nvidia-ml-py not installed - install with: pip install nvidia-ml-py3"
fi

# Start a simple GPU monitoring loop (optional)
if command -v nvidia-smi &> /dev/null; then
    log "Starting background GPU monitoring..."
    nohup bash -c '
    while true; do
        echo "[$(date "+%Y-%m-%d %H:%M:%S")] GPU Status:" >> '"$LOG_FILE"'
        nvidia-smi --query-gpu=index,utilization.gpu,memory.used,memory.total,temperature.gpu --format=csv,noheader,nounits >> '"$LOG_FILE"' 2>&1
        sleep 300  # Check every 5 minutes
    done
    ' > /dev/null 2>&1 &
    MONITOR_PID=$!
    echo $MONITOR_PID > "$HOME/workspace/logs/gpu_monitor.pid"
    log " Background GPU monitoring started with PID: $MONITOR_PID"
fi

log "GPU monitoring service started successfully"

