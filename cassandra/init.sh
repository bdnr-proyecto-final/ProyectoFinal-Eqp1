#!/bin/bash

echo "Esperando a que Cassandra esté disponible..."
sleep 20

echo "Creando esquema..."
cqlsh cassandra -f /schema.cql

echo "Creando roles..."
cqlsh cassandra -f /roles.cql

echo "Inicialización completada."