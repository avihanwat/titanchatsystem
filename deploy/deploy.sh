#!/bin/bash
# deploy.sh — runs ON the VM during CI/CD deployment
set -euo pipefail

APP_DIR="/opt/titanchat"

echo "==> Extracting uploaded code..."
sudo -u titanchat tar xzf /tmp/titanchat.tar.gz -C "$APP_DIR/" --overwrite
rm -f /tmp/titanchat.tar.gz

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
