#!/usr/bin/env bash
# checks if there are any uncommitted changes or unstaged/staged changes in the git repo
# if there are no changes, checks out the 'pages' branch
# supports DRY_RUN=1 environment variable to only print actions

set -euo pipefail

DRY_RUN=${DRY_RUN:-0}
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || echo "")"

if [ -z "$REPO_ROOT" ]; then
  echo "Not a git repository (or git not installed). Exiting with code 2." >&2
  exit 2
fi

cd "$REPO_ROOT"

# Check for uncommitted changes, untracked files, or differences from HEAD
# We'll treat any output from git status --porcelain as "there are changes"

STATUS_OUTPUT=$(git status --porcelain)

if [ -n "$STATUS_OUTPUT" ]; then
  echo "Repository has changes. Not checking out 'pages' branch."
  echo "Changes (git status --porcelain):"
  echo "$STATUS_OUTPUT"
  exit 1
fi

git checkout pages
git merge main
node examples/gym/query.js
node examples/sensors/query.js

mv gym.html temp_sensors.html pages

git add pages/gym.html 
git add pages/temp_sensors.html

git commit -m "- Update pages"
git push

git checkout main