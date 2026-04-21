#!/bin/bash
set -e

echo "Creando topic de Wikimedia..."
docker exec kafka kafka-topics \
  --create \
  --topic wikimedia.recentchange \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

echo "Topic creado."