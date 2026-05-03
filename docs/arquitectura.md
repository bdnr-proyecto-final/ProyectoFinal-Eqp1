## Descripción general
La arquitectura actual del proyecto implementa un flujo completo de ingesta, persistencia y análisis sobre el stream Wikimedia RecentChange en un entorno local reproducible.

El flujo real del repositorio es:

`Wikimedia EventStreams -> Kafka -> consumer Python -> Cassandra -> Spark -> CSV analítico`

## Flujo de datos
1. `consumers/wikimedia_to_kafka.py` consume eventos desde Wikimedia EventStreams.
2. Los eventos se publican en Kafka en el topic `wikimedia.recentchange`.
3. `consumers/kafka_to_cassandra.py` consume el topic y persiste los eventos en `wikimedia.recent_changes_raw`.
4. `spark/jobs/recent_changes_analytics.py` lee la tabla raw desde Cassandra.
5. Spark limpia, agrega y genera resultados analíticos en `spark/output/changes_by_wiki_hour`.

## Componentes

### Wikimedia EventStreams
Es la fuente externa de eventos en tiempo real.

### Kafka
Funciona como buffer local de ingesta y desacopla la recepción del stream de la persistencia en Cassandra.

### Consumer Python
El consumidor Kafka -> Cassandra implementa:

- commit manual de offsets
- inserción en `recent_changes_raw`
- prepared statements
- manejo explícito de mensajes inválidos y errores temporales de Cassandra

### Cassandra
Es la capa operativa de persistencia del proyecto. En el estado actual corre en un solo nodo local y almacena:

- `recent_changes` como tabla legacy temporal
- `recent_changes_raw` como tabla operativa del stream
- `changes_by_wiki_hour` como tabla analítica disponible para uso opcional

### Spark
Se usa como motor batch analítico sobre datos ya persistidos en Cassandra. No es el motor de streaming principal del pipeline.

## Artefactos principales

- `consumers/wikimedia_to_kafka.py`
- `consumers/kafka_to_cassandra.py`
- `cassandra/schema.cql`
- `spark/jobs/recent_changes_analytics.py`
- `scripts/run_analytics.sh`

## Justificación de la arquitectura
La arquitectura separa claramente:

- la capa de ingesta
- la capa operativa
- la capa analítica

Esto facilita la reproducibilidad del proyecto y mantiene el flujo entendible para fines académicos.

## Alcance real del entorno
El entorno actual está diseñado para desarrollo local y validación académica. No implementa:

- cluster multinodo real
- alta disponibilidad efectiva
- failover automático
- seguridad enterprise en ejecución
- despliegue productivo distribuido