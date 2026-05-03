#!/bin/bash
set -euo pipefail

CASSANDRA_HOST="${CASSANDRA_HOST:-cassandra}"
CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"
SCHEMA_FILE="${SCHEMA_FILE:-/init/schema.cql}"
MAX_RETRIES="${MAX_RETRIES:-30}"
SLEEP_SECONDS="${SLEEP_SECONDS:-5}"

echo "Esperando a que Cassandra este disponible en ${CASSANDRA_HOST}:${CASSANDRA_PORT}..."

for attempt in $(seq 1 "$MAX_RETRIES"); do
  if cqlsh "$CASSANDRA_HOST" "$CASSANDRA_PORT" -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; then
    echo "Cassandra disponible. Aplicando schema..."
    cqlsh "$CASSANDRA_HOST" "$CASSANDRA_PORT" -f "$SCHEMA_FILE"
    echo "Schema aplicado correctamente."
    exit 0
  fi

  echo "Intento ${attempt}/${MAX_RETRIES} fallido. Reintentando en ${SLEEP_SECONDS}s..."
  sleep "$SLEEP_SECONDS"
done

echo "No fue posible conectar con Cassandra para aplicar el schema."
exit 1