#!/bin/bash
# Setup script for Automated Repository Sync

REPO_DIR="/home/udonsi-kalu/workspace"
LOG_FILE="/home/udonsi-kalu/workspace/logs/sync_setup.log"

echo "🚀 Setting up Automated Repository Sync"
echo "======================================"

# Ensure log directory exists
mkdir -p "$(dirname "$LOG_FILE")"

# Function to log with timestamp
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log_message "Starting automated sync setup..."

# Check if git is installed
if ! command -v git &> /dev/null; then
    log_message "❌ Git is not installed. Please install git first."
    exit 1
fi

# Initialize git repository if not already done
if [ ! -d "$REPO_DIR/.git" ]; then
    log_message "Initializing git repository..."
    cd "$REPO_DIR"
    git init
    git branch -m main
    git config user.name "Auto Sync Bot"
    git config user.email "sync-bot@localhost"
    log_message "✅ Git repository initialized"
else
    log_message "✅ Git repository already exists"
fi

# Check if remote is configured
cd "$REPO_DIR"
if git remote -v | grep -q origin; then
    log_message "✅ GitHub remote already configured"
    git remote -v
else
    log_message "⚠️  No GitHub remote configured"
    echo ""
    echo "To complete the setup, you need to:"
    echo "1. Create a new repository on GitHub (or use an existing one)"
    echo "2. Add the remote URL:"
    echo "   git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git"
    echo "3. Push your initial commit:"
    echo "   git push -u origin main"
    echo ""
    echo "Example commands:"
    echo "   git remote add origin https://github.com/udonsi-kalu/daily-logs.git"
    echo "   git push -u origin main"
fi

# Show current cron jobs
log_message "Current cron jobs:"
crontab -l | grep -E "(auto_sync|daily_sync)" || log_message "No automated sync cron job found"

# Show setup status
echo ""
echo "📋 Setup Status:"
echo "================="
echo "✅ Git repository: $REPO_DIR"
echo "✅ Sync script: /home/udonsi-kalu/workspace/startup-scripts/auto_sync.sh"
echo "✅ Cron job: 6:00 AM daily"
echo "✅ Log file: $LOG_FILE"
echo ""

if ! git remote -v | grep -q origin; then
    echo "⚠️  Action Required:"
    echo "   Add your GitHub remote URL to complete setup"
    echo "   Run: git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git"
else
    echo "✅ Ready to go! Your automated sync will start tomorrow at 6:00 AM"
fi

log_message "Automated sync setup completed"

