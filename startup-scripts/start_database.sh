#!/bin/bash
# Start Database Services
# Ensures database services are running for your applications

LOG_FILE="$HOME/workspace/logs/database.log"

# Create logs directory if it doesn't exist
mkdir -p "$(dirname "$LOG_FILE")"

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "Starting database services..."

# Check if SQL Server is running (based on your connection string)
if pgrep -f "sqlservr" > /dev/null; then
    log " SQL Server is already running"
else
    log " SQL Server not detected - you may need to start it manually"
    log "   Connection string: localhost,1433"
    log "   Database: _reporting"
fi

# Check if any other database services are running
if pgrep -f "postgres" > /dev/null; then
    log " PostgreSQL is running"
fi

if pgrep -f "mysql" > /dev/null; then
    log " MySQL is running"
fi

if pgrep -f "mongod" > /dev/null; then
    log " MongoDB is running"
fi

# Check if database ports are accessible
log "Checking database connectivity..."

# Test SQL Server port 1433
if nc -z localhost 1433 2>/dev/null; then
    log " SQL Server port 1433 is accessible"
else
    log " SQL Server port 1433 is not accessible"
fi

# Test PostgreSQL port 5432
if nc -z localhost 5432 2>/dev/null; then
    log " PostgreSQL port 5432 is accessible"
fi

# Test MySQL port 3306
if nc -z localhost 3306 2>/dev/null; then
    log " MySQL port 3306 is accessible"
fi

log "Database service check completed"

