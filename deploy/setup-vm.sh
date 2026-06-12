#!/bin/bash
# setup-vm.sh — Run ONCE on the VM to prepare it for deployments.
# Usage: ssh into VM, then: sudo bash setup-vm.sh
set -euo pipefail

APP_DIR="/opt/titanchat"
REPO_URL="git@github.com:avihanwat/titanchatsystem.git"

echo "==> Creating titanchat user..."
id -u titanchat &>/dev/null || useradd -r -m -s /bin/bash titanchat

echo "==> Creating app directory..."
mkdir -p "$APP_DIR"
chown titanchat:titanchat "$APP_DIR"

echo "==> Cloning repo..."
if [ ! -d "$APP_DIR/repo" ]; then
  sudo -u titanchat git clone "$REPO_URL" "$APP_DIR/repo"
fi

echo "==> Installing system dependencies..."
apt-get update && apt-get install -y python3.12 python3.12-venv python3-pip rsync

echo "==> Verifying local services are installed..."
for svc in redis kafka cassandra postgresql; do
  if systemctl list-unit-files | grep -q "^${svc}"; then
    echo "  ✓ $svc found"
  else
    echo "  ⚠ $svc not found — install it manually"
  fi
done

echo "==> Setting up Python venv..."
sudo -u titanchat python3.12 -m venv "$APP_DIR/venv"
sudo -u titanchat "$APP_DIR/venv/bin/pip" install -r "$APP_DIR/repo/requirements.txt"

echo "==> Creating .env file (edit with your values)..."
if [ ! -f "$APP_DIR/.env" ]; then
  cat > "$APP_DIR/.env" << 'EOF'
# All services on localhost (single server)
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

echo "==> Creating log directory..."
mkdir -p /var/log/titanchat
chown titanchat:titanchat /var/log/titanchat

echo "==> Setting up logrotate..."
cat > /etc/logrotate.d/titanchat << 'EOF'
/var/log/titanchat/*.log {
    daily
    rotate 14
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
    su titanchat titanchat
}
EOF

echo "==> Installing systemd services..."
cp "$APP_DIR/repo/deploy/systemd/"*.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable titanchat-gateway titanchat-consumer titanchat-api

echo "==> Allowing titanchat user to restart services without password..."
cat > /etc/sudoers.d/titanchat << 'EOF'
titanchat ALL=(ALL) NOPASSWD: /bin/systemctl daemon-reload, /bin/systemctl restart titanchat-*, /bin/systemctl start titanchat-*, /bin/systemctl stop titanchat-*
EOF

echo "==> Initial deploy..."
sudo -u titanchat bash "$APP_DIR/repo/deploy/deploy.sh"

echo "==> Setup complete! Edit /opt/titanchat/.env with real values, then:"
echo "    sudo systemctl restart titanchat-gateway titanchat-consumer titanchat-api"
