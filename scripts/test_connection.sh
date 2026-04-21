#!/bin/bash
set -e

echo "Verificando contenedores..."
docker ps

echo ""
echo "Probando conexión a Cassandra..."
docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES;"

echo ""
echo "Probando conexión a Kafka..."
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092