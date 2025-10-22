#!/bin/bash
# Daily Repository Sync Script

# Run the automated sync script
/home/udonsi-kalu/workspace/startup-scripts/auto_sync.sh

# Force push to ensure commits are uploaded
cd /home/udonsi-kalu/workspace
git push origin main >> /home/udonsi-kalu/workspace/logs/auto_sync.log 2>&1

if [ $? -eq 0 ]; then
    echo "✅ All commits pushed to GitHub successfully!"
else
    echo "❌ Some commits may not have been pushed"
fi