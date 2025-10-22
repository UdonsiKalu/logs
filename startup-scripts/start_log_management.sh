#!/bin/bash
# Start Log Management Service
# Handles log rotation, cleanup, and monitoring

LOG_FILE="$HOME/workspace/logs/log_management.log"
LOGS_DIR="$HOME/workspace/logs"

# Create logs directory if it doesn't exist
mkdir -p "$LOGS_DIR"

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "Starting log management service..."

# Function to rotate logs
rotate_logs() {
    local log_file="$1"
    local max_size_mb=100  # 100MB max size
    
    if [ -f "$log_file" ]; then
        local size_mb=$(du -m "$log_file" | cut -f1)
        if [ "$size_mb" -gt "$max_size_mb" ]; then
            log "Rotating large log file: $log_file (${size_mb}MB)"
            mv "$log_file" "${log_file}.$(date +%Y%m%d_%H%M%S)"
            touch "$log_file"
        fi
    fi
}

# Function to clean old logs
cleanup_old_logs() {
    local days_to_keep=30
    log "Cleaning up logs older than $days_to_keep days..."
    
    find "$LOGS_DIR" -name "*.log.*" -type f -mtime +$days_to_keep -delete 2>/dev/null
    find "$LOGS_DIR" -name "*.pid" -type f -mtime +7 -delete 2>/dev/null
    
    log "✅ Old log cleanup completed"
}

# Function to check disk space
check_disk_space() {
    local usage=$(df "$LOGS_DIR" | tail -1 | awk '{print $5}' | sed 's/%//')
    local threshold=80
    
    if [ "$usage" -gt "$threshold" ]; then
        log "⚠️ WARNING: Disk usage is ${usage}% (threshold: ${threshold}%)"
        log "Consider cleaning up logs or increasing storage"
    else
        log "✅ Disk usage is ${usage}% (OK)"
    fi
}

# Function to monitor log sizes
monitor_log_sizes() {
    log "Current log file sizes:"
    find "$LOGS_DIR" -name "*.log" -type f -exec ls -lh {} \; | while read -r line; do
        log "  $line"
    done
}

# Initial cleanup and setup
log "Performing initial log management setup..."
cleanup_old_logs
check_disk_space
monitor_log_sizes

# Rotate any large existing logs
rotate_logs "$LOGS_DIR/qdrant.log"
rotate_logs "$LOGS_DIR/rclone.log"
rotate_logs "$LOGS_DIR/streamlit.log"
rotate_logs "$LOGS_DIR/database.log"
rotate_logs "$LOGS_DIR/gpu_monitor.log"

# Start log monitoring service
log "Starting log monitoring service..."
nohup bash -c '
while true; do
    # Check every hour
    sleep 3600
    
    # Rotate large logs
    for log_file in '"$LOGS_DIR"'/*.log; do
        if [ -f "$log_file" ]; then
            size_mb=$(du -m "$log_file" | cut -f1)
            if [ "$size_mb" -gt 100 ]; then
                echo "[$(date "+%Y-%m-%d %H:%M:%S")] Rotating large log: $log_file (${size_mb}MB)" >> '"$LOG_FILE"'
                mv "$log_file" "${log_file}.$(date +%Y%m%d_%H%M%S)"
                touch "$log_file"
            fi
        fi
    done
    
    # Clean up old logs weekly
    if [ $(date +%u) -eq 1 ]; then  # Monday
        echo "[$(date "+%Y-%m-%d %H:%M:%S")] Weekly log cleanup..." >> '"$LOG_FILE"'
        find '"$LOGS_DIR"' -name "*.log.*" -type f -mtime +30 -delete 2>/dev/null
    fi
    
    # Check disk space
    usage=$(df '"$LOGS_DIR"' | tail -1 | awk "{print \$5}" | sed "s/%//")
    if [ "$usage" -gt 85 ]; then
        echo "[$(date "+%Y-%m-%d %H:%M:%S")] WARNING: High disk usage: ${usage}%" >> '"$LOG_FILE"'
    fi
done
' > /dev/null 2>&1 &

MONITOR_PID=$!
echo $MONITOR_PID > "$LOGS_DIR/log_management.pid"
log "✅ Log monitoring service started with PID: $MONITOR_PID"

# Set up logrotate configuration
log "Setting up logrotate configuration..."
cat > "$LOGS_DIR/logrotate.conf" << EOF
$LOGS_DIR/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 $(whoami) $(whoami)
    postrotate
        # Restart services if needed
        echo "Log rotated: $(date)" >> $LOGS_DIR/log_management.log
    endscript
}
EOF

log "✅ Log management service started successfully"
log "Log files location: $LOGS_DIR"
log "Log rotation: Daily, keep 30 days"
log "Max log size: 100MB before rotation"

