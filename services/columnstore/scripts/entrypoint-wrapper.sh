#!/bin/bash
# Start ColumnStore, provision single-node cluster once, apply schema DDL.
set -euo pipefail

ADMIN_PASSWORD="${MARIADB_ROOT_PASSWORD:-C0lumnStore!}"
HOSTNAME="${PM1:-mcs1}"
SCHEMA_MARKER="/etc/columnstore/.lichess_schema_applied"

/usr/bin/tini -- /docker-entrypoint.sh start-services &
MAIN_PID=$!

wait_for_mysql() {
  for _ in $(seq 1 120); do
    if mysqladmin ping -h 127.0.0.1 -u admin -p"$ADMIN_PASSWORD" --silent 2>/dev/null; then
      return 0
    fi
    sleep 2
  done
  return 1
}

provision_cluster() {
  if [ -f /etc/columnstore/container-provisioned ]; then
    return 0
  fi
  echo "Provisioning ColumnStore node ${HOSTNAME}..."
  /usr/bin/provision "$HOSTNAME"
}

apply_schema() {
  if [ -f "$SCHEMA_MARKER" ]; then
    return 0
  fi
  for sql in /docker-entrypoint-initdb.d/*.sql; do
    [ -f "$sql" ] || continue
    echo "Applying schema: $sql"
    if ! mysql -h 127.0.0.1 -u admin -p"$ADMIN_PASSWORD" < "$sql"; then
      echo "Schema apply failed for $sql" >&2
      return 1
    fi
  done
  touch "$SCHEMA_MARKER"
}

if wait_for_mysql; then
  provision_cluster
  apply_schema
else
  echo "ColumnStore MySQL did not become ready in time" >&2
fi

wait "$MAIN_PID"
