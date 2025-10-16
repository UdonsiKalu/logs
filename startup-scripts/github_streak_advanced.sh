#!/bin/bash
# Advanced GitHub Streak Automation Script
# Creates 5 meaningful commits every morning to maintain green streak

# Configuration
REPO_DIR="/home/udonsi-kalu/workspace"
LOG_FILE="/home/udonsi-kalu/workspace/logs/github_streak.log"
COMMIT_FILE="$REPO_DIR/daily_commits.txt"
PROGRESS_FILE="$REPO_DIR/streak_progress.json"

# Ensure log directory exists
mkdir -p "$(dirname "$LOG_FILE")"

# Function to log with timestamp
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

# Function to create meaningful commit content
create_commit_content() {
    local commit_number=$1
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local day_of_year=$(date '+%j')
    local week_of_year=$(date '+%V')
    
    # Create different types of content based on commit number
    case $commit_number in
        1)
            echo "# Daily Development Log - $(date '+%Y-%m-%d')" > "$COMMIT_FILE"
            echo "" >> "$COMMIT_FILE"
            echo "## System Status" >> "$COMMIT_FILE"
            echo "- Date: $timestamp" >> "$COMMIT_FILE"
            echo "- Day of Year: $day_of_year" >> "$COMMIT_FILE"
            echo "- Week: $week_of_year" >> "$COMMIT_FILE"
            echo "- System: $(uname -s) $(uname -r)" >> "$COMMIT_FILE"
            echo "" >> "$COMMIT_FILE"
            echo "## Development Progress" >> "$COMMIT_FILE"
            echo "- Automated commit #$commit_number" >> "$COMMIT_FILE"
            echo "- Repository maintenance" >> "$COMMIT_FILE"
            echo "- Code organization" >> "$COMMIT_FILE"
            ;;
        2)
            echo "# Code Quality Improvements - $(date '+%Y-%m-%d')" > "$COMMIT_FILE"
            echo "" >> "$COMMIT_FILE"
            echo "## Updates" >> "$COMMIT_FILE"
            echo "- Code formatting improvements" >> "$COMMIT_FILE"
            echo "- Documentation updates" >> "$COMMIT_FILE"
            echo "- Comment enhancements" >> "$COMMIT_FILE"
            echo "" >> "$COMMIT_FILE"
            echo "## Technical Details" >> "$COMMIT_FILE"
            echo "- Commit timestamp: $timestamp" >> "$COMMIT_FILE"
            echo "- Automated maintenance" >> "$COMMIT_FILE"
            ;;
        3)
            echo "# Project Documentation - $(date '+%Y-%m-%d')" > "$COMMIT_FILE"
            echo "" >> "$COMMIT_FILE"
            echo "## Documentation Updates" >> "$COMMIT_FILE"
            echo "- README improvements" >> "$COMMIT_FILE"
            echo "- Code comments added" >> "$COMMIT_FILE"
            echo "- Process documentation" >> "$COMMIT_FILE"
            echo "" >> "$COMMIT_FILE"
            echo "## Files Modified" >> "$COMMIT_FILE"
            echo "- daily_commits.txt (this file)" >> "$COMMIT_FILE"
            echo "- Documentation files" >> "$COMMIT_FILE"
            ;;
        4)
            echo "# Performance Optimization - $(date '+%Y-%m-%d')" > "$COMMIT_FILE"
            echo "" >> "$COMMIT_FILE"
            echo "## Optimizations" >> "$COMMIT_FILE"
            echo "- Code efficiency improvements" >> "$COMMIT_FILE"
            echo "- Resource usage optimization" >> "$COMMIT_FILE"
            echo "- Performance monitoring" >> "$COMMIT_FILE"
            echo "" >> "$COMMIT_FILE"
            echo "## Metrics" >> "$COMMIT_FILE"
            echo "- Commit #$commit_number of 5" >> "$COMMIT_FILE"
            echo "- Time: $timestamp" >> "$COMMIT_FILE"
            ;;
        5)
            echo "# Final Daily Commit - $(date '+%Y-%m-%d')" > "$COMMIT_FILE"
            echo "" >> "$COMMIT_FILE"
            echo "## Daily Summary" >> "$COMMIT_FILE"
            echo "- Total commits today: 5" >> "$COMMIT_FILE"
            echo "- GitHub streak maintained" >> "$COMMIT_FILE"
            echo "- Development workflow active" >> "$COMMIT_FILE"
            echo "" >> "$COMMIT_FILE"
            echo "## Next Steps" >> "$COMMIT_FILE"
            echo "- Continue development tomorrow" >> "$COMMIT_FILE"
            echo "- Maintain consistent commit schedule" >> "$COMMIT_FILE"
            echo "- Monitor project progress" >> "$COMMIT_FILE"
            ;;
    esac
}

# Function to create a commit
create_commit() {
    local commit_number=$1
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    # Create meaningful content
    create_commit_content $commit_number
    
    # Add to git
    cd "$REPO_DIR"
    git add daily_commits.txt
    
    # Create commit with different messages
    local messages=(
        "docs: Daily development log and system status update"
        "refactor: Code quality improvements and formatting"
        "docs: Project documentation and README updates"
        "perf: Performance optimization and monitoring"
        "feat: Daily development workflow completion"
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

# Function to update progress tracking
update_progress() {
    local successful_commits=$1
    local date=$(date '+%Y-%m-%d')
    
    # Create or update progress file
    if [ ! -f "$PROGRESS_FILE" ]; then
        echo "{\"streak_days\": 0, \"total_commits\": 0, \"last_update\": \"$date\"}" > "$PROGRESS_FILE"
    fi
    
    # Update progress (simple JSON update)
    local current_total=$(grep -o '"total_commits": [0-9]*' "$PROGRESS_FILE" | grep -o '[0-9]*')
    local new_total=$((current_total + successful_commits))
    
    # Simple JSON update
    sed -i "s/\"total_commits\": [0-9]*/\"total_commits\": $new_total/" "$PROGRESS_FILE"
    sed -i "s/\"last_update\": \"[^\"]*\"/\"last_update\": \"$date\"/" "$PROGRESS_FILE"
}

# Main execution
log_message "🚀 Starting Advanced GitHub streak automation - 5 commits"
echo "🚀 Starting Advanced GitHub streak automation - 5 commits"

# Check if we're in a git repository
if [ ! -d "$REPO_DIR/.git" ]; then
    log_message "❌ Not in a git repository. Initializing..."
    cd "$REPO_DIR"
    git init
    git config user.name "GitHub Streak Bot"
    git config user.email "streak-bot@localhost"
    log_message "⚠️  Please add your GitHub remote: git remote add origin https://github.com/yourusername/your-repo.git"
fi

# Create 5 commits with small delays
successful_commits=0
for i in {1..5}; do
    log_message "Creating commit #$i..."
    if create_commit $i; then
        ((successful_commits++))
    fi
    
    # Small delay between commits
    sleep 3
done

# Update progress tracking
update_progress $successful_commits

# Push all commits
log_message "Pushing $successful_commits commits to GitHub..."
if push_commits; then
    log_message "✅ GitHub streak automation completed successfully"
    echo "✅ GitHub streak automation completed - $successful_commits commits pushed"
    echo "📊 Progress saved to: $PROGRESS_FILE"
else
    log_message "❌ GitHub streak automation completed with push errors"
    echo "❌ GitHub streak automation completed with push errors - $successful_commits commits created locally"
    echo "📊 Progress saved to: $PROGRESS_FILE"
fi

log_message "Advanced GitHub streak automation finished"
