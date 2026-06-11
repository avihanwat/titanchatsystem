#!/bin/bash
# deploy.sh — runs ON the VM during CI/CD deployment
set -euo pipefail

APP_DIR="/opt/titanchat"
REPO_DIR="$APP_DIR/repo"

echo "==> Pulling latest code..."
cd "$REPO_DIR"
git fetch origin main
git reset --hard origin/main

echo "==> Syncing code to app directory..."
rsync -a --delete \
  --exclude='venv' \
  --exclude='.env' \
  --exclude='__pycache__' \
  --exclude='.git' \
  --exclude='.pytest_cache' \
  "$REPO_DIR/" "$APP_DIR/"

echo "==> Installing/updating dependencies..."
cd "$APP_DIR"
source venv/bin/activate
pip install -r requirements.txt --quiet

echo "==> Restarting services..."
sudo systemctl daemon-reload
sudo systemctl restart titanchat-gateway
sudo systemctl restart titanchat-consumer
sudo systemctl restart titanchat-api

echo "==> Waiting for services to stabilize..."
sleep 3

echo "==> Service status:"
systemctl is-active titanchat-gateway titanchat-consumer titanchat-api

echo "==> Deploy complete!"
