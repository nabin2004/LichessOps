#!/usr/bin/env bash
# Start Prometheus + Grafana (monitoring profile) and wait until healthy.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "Starting Prometheus and Grafana (profile: monitoring)..."
docker compose --profile monitoring up -d

wait_for() {
  local name="$1"
  local url="$2"
  local max_attempts="${3:-30}"
  local attempt=0

  echo -n "Waiting for ${name}"
  while [ "$attempt" -lt "$max_attempts" ]; do
    if curl -sf "$url" >/dev/null 2>&1; then
      echo " — ready"
      return 0
    fi
    attempt=$((attempt + 1))
    echo -n "."
    sleep 2
  done
  echo " — timed out"
  return 1
}

wait_for "Prometheus" "http://localhost:9090/-/healthy"
wait_for "Grafana" "http://localhost:3000/api/health"

echo ""
echo "Monitoring stack is up:"
echo "  Prometheus: http://localhost:9090"
echo "  Grafana:    http://localhost:3000  (default admin / changeme — see services/.env.example)"
echo ""
echo "Grafana dashboards (auto-provisioned):"
echo "  - Lichess Serving  (requires lichess-serving on :8082)"
echo "  - Lichess System   (node-exporter host metrics)"
echo ""
echo "Start the serving API so Prometheus can scrape it:"
echo "  uv run lichess-serving --port 8082"
