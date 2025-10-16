#!/bin/bash
# Start System Health Monitoring Service
# Monitors system health, performance, and alerts on issues

LOG_FILE="$HOME/workspace/logs/health_monitor.log"
ALERT_FILE="$HOME/workspace/logs/health_alerts.log"

# Create logs directory if it doesn't exist
mkdir -p "$(dirname "$LOG_FILE")"

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to log alerts
alert() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ALERT: $1" | tee -a "$ALERT_FILE"
    log "ALERT: $1"
}

log "Starting system health monitoring service..."

# Function to check system resources
check_resources() {
    # CPU usage
    local cpu_usage=$(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | sed 's/%us,//')
    local cpu_int=$(echo $cpu_usage | cut -d. -f1)
    
    if [ "$cpu_int" -gt 80 ]; then
        alert "High CPU usage: ${cpu_usage}%"
    else
        log "CPU usage: ${cpu_usage}% (OK)"
    fi
    
    # Memory usage
    local mem_info=$(free | grep Mem)
    local mem_total=$(echo $mem_info | awk '{print $2}')
    local mem_used=$(echo $mem_info | awk '{print $3}')
    local mem_percent=$((mem_used * 100 / mem_total))
    
    if [ "$mem_percent" -gt 85 ]; then
        alert "High memory usage: ${mem_percent}%"
    else
        log "Memory usage: ${mem_percent}% (OK)"
    fi
    
    # Disk usage
    local disk_usage=$(df / | tail -1 | awk '{print $5}' | sed 's/%//')
    if [ "$disk_usage" -gt 90 ]; then
        alert "High disk usage: ${disk_usage}%"
    else
        log "Disk usage: ${disk_usage}% (OK)"
    fi
}

# Function to check service health
check_services() {
    local services=("qdrant" "ollama" "nginx" "rclone")
    
    for service in "${services[@]}"; do
        if pgrep -f "$service" > /dev/null; then
            log "✅ $service is running"
        else
            alert "$service is not running"
        fi
    done
    
    # Check Docker containers
    if command -v docker &> /dev/null; then
        local running_containers=$(docker ps --format "{{.Names}}" | wc -l)
        log "Docker containers running: $running_containers"
        
        if [ "$running_containers" -eq 0 ]; then
            alert "No Docker containers running"
        fi
    fi
}

# Function to check network connectivity
check_network() {
    # Check internet connectivity
    if ping -c 1 8.8.8.8 &> /dev/null; then
        log "✅ Internet connectivity OK"
    else
        alert "No internet connectivity"
    fi
    
    # Check local services
    local ports=("6333" "6334" "8502" "8509" "11434")
    for port in "${ports[@]}"; do
        if nc -z localhost "$port" 2>/dev/null; then
            log "✅ Port $port is accessible"
        else
            alert "Port $port is not accessible"
        fi
    done
}

# Function to check log file sizes
check_logs() {
    local logs_dir="$HOME/workspace/logs"
    local max_size_mb=500  # 500MB max total logs
    
    if [ -d "$logs_dir" ]; then
        local total_size_mb=$(du -sm "$logs_dir" | cut -f1)
        if [ "$total_size_mb" -gt "$max_size_mb" ]; then
            alert "Log directory size is large: ${total_size_mb}MB"
        else
            log "Log directory size: ${total_size_mb}MB (OK)"
        fi
    fi
}

# Function to check GPU health
check_gpu() {
    if command -v nvidia-smi &> /dev/null; then
        local gpu_temp=$(nvidia-smi --query-gpu=temperature.gpu --format=csv,noheader,nounits | head -1)
        if [ "$gpu_temp" -gt 80 ]; then
            alert "High GPU temperature: ${gpu_temp}°C"
        else
            log "GPU temperature: ${gpu_temp}°C (OK)"
        fi
        
        local gpu_mem=$(nvidia-smi --query-gpu=memory.used,memory.total --format=csv,noheader,nounits | head -1)
        log "GPU memory: $gpu_mem"
    else
        log "nvidia-smi not available - skipping GPU checks"
    fi
}

# Initial health check
log "Performing initial system health check..."
check_resources
check_services
check_network
check_logs
check_gpu

# Start continuous monitoring
log "Starting continuous health monitoring..."
nohup bash -c '
while true; do
    # Check every 5 minutes
    sleep 300
    
    echo "[$(date "+%Y-%m-%d %H:%M:%S")] Health check cycle..." >> '"$LOG_FILE"'
    
    # Check resources
    cpu_usage=$(top -bn1 | grep "Cpu(s)" | awk "{print \$2}" | sed "s/%us,//")
    cpu_int=$(echo $cpu_usage | cut -d. -f1)
    if [ "$cpu_int" -gt 80 ]; then
        echo "[$(date "+%Y-%m-%d %H:%M:%S")] ALERT: High CPU usage: ${cpu_usage}%" >> '"$ALERT_FILE"'
    fi
    
    # Check memory
    mem_percent=$(free | grep Mem | awk "{mem_used=\$3; mem_total=\$2; print int(mem_used*100/mem_total)}")
    if [ "$mem_percent" -gt 85 ]; then
        echo "[$(date "+%Y-%m-%d %H:%M:%S")] ALERT: High memory usage: ${mem_percent}%" >> '"$ALERT_FILE"'
    fi
    
    # Check disk
    disk_usage=$(df / | tail -1 | awk "{print \$5}" | sed "s/%//")
    if [ "$disk_usage" -gt 90 ]; then
        echo "[$(date "+%Y-%m-%d %H:%M:%S")] ALERT: High disk usage: ${disk_usage}%" >> '"$ALERT_FILE"'
    fi
    
    # Check services
    if ! pgrep -f "qdrant" > /dev/null; then
        echo "[$(date "+%Y-%m-%d %H:%M:%S")] ALERT: Qdrant not running" >> '"$ALERT_FILE"'
    fi
    
    if ! pgrep -f "ollama" > /dev/null; then
        echo "[$(date "+%Y-%m-%d %H:%M:%S")] ALERT: Ollama not running" >> '"$ALERT_FILE"'
    fi
    
    # Check network
    if ! ping -c 1 8.8.8.8 &> /dev/null; then
        echo "[$(date "+%Y-%m-%d %H:%M:%S")] ALERT: No internet connectivity" >> '"$ALERT_FILE"'
    fi
    
done
' > /dev/null 2>&1 &

MONITOR_PID=$!
echo $MONITOR_PID > "$HOME/workspace/logs/health_monitor.pid"
log "✅ Health monitoring service started with PID: $MONITOR_PID"

log "✅ System health monitoring service started successfully"
log "Health logs: $LOG_FILE"
log "Alerts: $ALERT_FILE"
log "Monitoring: CPU, Memory, Disk, Services, Network, GPU"
