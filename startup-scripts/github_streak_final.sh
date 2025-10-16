#!/bin/bash
# Final GitHub Streak Script with Clean Files

# Run the original streak script
/home/udonsi-kalu/workspace/startup-scripts/github_streak_advanced.sh

# Force push to ensure commits are uploaded
cd /home/udonsi-kalu/workspace
git push origin main >> /home/udonsi-kalu/workspace/logs/github_streak.log 2>&1

if [ $? -eq 0 ]; then
    echo "✅ All commits pushed to GitHub successfully!"
else
    echo "❌ Some commits may not have been pushed"
fi