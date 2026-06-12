#!/bin/bash
# deploy.sh — runs ON the VM during CI/CD deployment
set -euo pipefail

APP_DIR="/opt/titanchat"

echo "==> Setting up directories..."
mkdir -p /var/log/titanchat
chown titanchat:titanchat /var/log/titanchat

echo "==> Installing systemd services..."
cp "$APP_DIR/deploy/systemd/"*.service /etc/systemd/system/

echo "==> Creating .env if missing..."
if [ ! -f "$APP_DIR/.env" ]; then
  cat > "$APP_DIR/.env" << 'EOF'
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
SERVER_ID=gateway-1
GATEWAY_INTERNAL_URL=http://127.0.0.1:8000
CASSANDRA_HOSTS=127.0.0.1
CASSANDRA_KEYSPACE=titanchat
CASSANDRA_USERNAME=
CASSANDRA_PASSWORD=
JWT_SECRET=CHANGE_ME_IN_PRODUCTION
POSTGRES_HOST=127.0.0.1
POSTGRES_DB=titanchat_accounts
POSTGRES_USER=titanchat
POSTGRES_PASSWORD=CHANGE_ME
EOF
  chown titanchat:titanchat "$APP_DIR/.env"
  chmod 600 "$APP_DIR/.env"
fi

echo "==> Installing/updating dependencies..."
cd "$APP_DIR"
if [ ! -d "venv" ]; then
  echo "==> Creating Python venv..."
  python3 -m venv venv
  chown -R titanchat:titanchat venv
fi
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
