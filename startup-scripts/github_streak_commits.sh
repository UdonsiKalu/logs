#!/bin/bash
# GitHub Streak Automation Script
# Creates 5 commits every morning to maintain green streak

# Configuration
REPO_DIR="/home/udonsi-kalu/workspace"
LOG_FILE="/home/udonsi-kalu/workspace/logs/github_streak.log"
COMMIT_FILE="$REPO_DIR/daily_commits.txt"

# Ensure log directory exists
mkdir -p "$(dirname "$LOG_FILE")"

# Function to log with timestamp
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

# Function to create a commit
create_commit() {
    local commit_number=$1
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    # Create or update the commit file
    echo "Daily commit #$commit_number - $timestamp" >> "$COMMIT_FILE"
    echo "Automated commit for GitHub streak maintenance" >> "$COMMIT_FILE"
    echo "Commit time: $(date)" >> "$COMMIT_FILE"
    echo "System: $(uname -a)" >> "$COMMIT_FILE"
    echo "---" >> "$COMMIT_FILE"
    
    # Add to git
    cd "$REPO_DIR"
    git add daily_commits.txt
    
    # Create commit with different messages
    local messages=(
        "Daily commit #$commit_number - System maintenance and updates"
        "Morning commit #$commit_number - Automated development workflow"
        "Daily update #$commit_number - Code repository maintenance"
        "Commit #$commit_number - Development environment sync"
        "Daily sync #$commit_number - Project status update"
    )
    
    local message_index=$((commit_number - 1))
    local commit_message="${messages[$message_index]}"
    
    git commit -m "$commit_message" >> "$LOG_FILE" 2>&1
    
    if [ $? -eq 0 ]; then
        log_message "✅ Commit #$commit_number created successfully: $commit_message"
        return 0
    else
        log_message "❌ Failed to create commit #$commit_number"
        return 1
    fi
}

# Function to push commits
push_commits() {
    cd "$REPO_DIR"
    git push origin main >> "$LOG_FILE" 2>&1
    
    if [ $? -eq 0 ]; then
        log_message "✅ All commits pushed to GitHub successfully"
        return 0
    else
        log_message "❌ Failed to push commits to GitHub"
        return 1
    fi
}

# Main execution
log_message "🚀 Starting GitHub streak automation - 5 commits"
echo "🚀 Starting GitHub streak automation - 5 commits"

# Check if we're in a git repository
if [ ! -d "$REPO_DIR/.git" ]; then
    log_message "❌ Not in a git repository. Initializing..."
    cd "$REPO_DIR"
    git init
    git remote add origin https://github.com/yourusername/your-repo.git  # Update this URL
    log_message "⚠️  Please update the remote URL in this script"
fi

# Create 5 commits with small delays
successful_commits=0
for i in {1..5}; do
    log_message "Creating commit #$i..."
    if create_commit $i; then
        ((successful_commits++))
    fi
    
    # Small delay between commits
    sleep 2
done

# Push all commits
log_message "Pushing $successful_commits commits to GitHub..."
if push_commits; then
    log_message "✅ GitHub streak automation completed successfully"
    echo "✅ GitHub streak automation completed - $successful_commits commits pushed"
else
    log_message "❌ GitHub streak automation completed with push errors"
    echo "❌ GitHub streak automation completed with push errors - $successful_commits commits created locally"
fi

log_message "GitHub streak automation finished"
