#!/bin/bash
set -e

echo "Configuración de ACLs para Kafka (modo documentativo)"

# NOTA:
# Este script muestra cómo se configurarían los permisos en Kafka
# en un entorno con seguridad habilitada (SASL/ACLs).
# En la configuración actual (PLAINTEXT), estos comandos NO se ejecutan.

echo ""
echo "Ejemplos de comandos:"

echo ""
echo "Permitir a un productor escribir en el topic:"
echo "kafka-acls --bootstrap-server localhost:9092 \\"
echo "  --add --allow-principal User:producer \\"
echo "  --operation WRITE --topic wikimedia.recentchange"

echo ""
echo "Permitir a un consumidor leer del topic:"
echo "kafka-acls --bootstrap-server localhost:9092 \\"
echo "  --add --allow-principal User:consumer \\"
echo "  --operation READ --topic wikimedia.recentchange"

echo ""
echo "NOTA:"
echo "Para que estos comandos funcionen, Kafka debe estar configurado con:"
echo "- Autenticación SASL"
echo "- Autorización habilitada (ACLs)"
echo "- Usuarios definidos"

echo ""
echo "Este archivo cumple como documentación de la estrategia de seguridad para la etapa 2."