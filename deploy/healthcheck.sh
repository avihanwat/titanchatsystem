#!/bin/bash
# healthcheck.sh — Verify all services are healthy
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

FAILURES=0

check_systemd() {
  local svc="$1"
  if systemctl is-active --quiet "$svc"; then
    echo -e "${GREEN}✓${NC} $svc is running"
  else
    echo -e "${RED}✗${NC} $svc is NOT running"
    FAILURES=$((FAILURES + 1))
  fi
}

check_port() {
  local name="$1" port="$2"
  if ss -tlnp | grep -q ":${port} "; then
    echo -e "${GREEN}✓${NC} $name listening on port $port"
  else
    echo -e "${RED}✗${NC} $name NOT listening on port $port"
    FAILURES=$((FAILURES + 1))
  fi
}

check_http() {
  local name="$1" url="$2"
  if curl -sf --max-time 5 "$url" > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} $name HTTP OK ($url)"
  else
    echo -e "${RED}✗${NC} $name HTTP FAILED ($url)"
    FAILURES=$((FAILURES + 1))
  fi
}

echo "====== Infrastructure Services ======"
check_systemd "redis"
check_systemd "kafka"
check_systemd "cassandra"
check_systemd "postgresql"

echo ""
echo "====== Infrastructure Ports ======"
check_port "Redis"      6379
check_port "Kafka"      9092
check_port "Cassandra"  9042
check_port "PostgreSQL" 5432

echo ""
echo "====== App Services ======"
check_systemd "titanchat-gateway"
check_systemd "titanchat-consumer"
check_systemd "titanchat-api"

echo ""
echo "====== App HTTP Endpoints ======"
check_http "Gateway" "http://localhost:8000/health"
check_http "API"     "http://localhost:8001/health"

echo ""
if [ $FAILURES -eq 0 ]; then
  echo -e "${GREEN}All checks passed!${NC}"
  exit 0
else
  echo -e "${RED}${FAILURES} check(s) failed!${NC}"
  exit 1
fi
