## Descripción general
La arquitectura del proyecto se diseñó para soportar la ingesta, almacenamiento y análisis de eventos en tiempo real provenientes del stream de Wikimedia RecentChange.

## Flujo de datos
1. Los eventos son obtenidos desde Wikimedia EventStreams.
2. Kafka actúa como capa intermedia de mensajería y desacopla la recepción del procesamiento.
3. Cassandra almacena los eventos crudos como fuente operativa de verdad.
4. Spark toma los datos para procesos posteriores de limpieza, transformación y análisis.
5. Los resultados analíticos permitirán responder consultas de valor sobre tendencias y comportamiento del stream.

## Componentes

### Wikimedia EventStreams
Fuente de datos en tiempo real.

### Kafka
Permite la recepción continua de eventos y soporta el flujo asíncrono de datos.

### Cassandra
Almacena eventos de forma distribuida, con alta disponibilidad y enfoque en escritura rápida.

### Spark
Procesa y transforma los datos para fines analíticos.

## Justificación de la arquitectura
Se eligió esta arquitectura porque separa claramente:
- la capa de ingesta
- la capa operativa
- la capa analítica

Esto permite escalar de forma modular y cumplir con los requerimientos del proyecto.