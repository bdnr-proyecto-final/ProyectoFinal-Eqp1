# Decisiones basadas en el teorema CAP

## Prioridad elegida
La arquitectura prioriza:

- Disponibilidad (Availability)
- Tolerancia a particiones (Partition Tolerance)

## Justificación
Dado que el sistema recibe eventos en tiempo real desde una fuente externa, es importante mantener la disponibilidad del sistema incluso cuando existan fallos parciales de red o nodos dentro de la infraestructura.

En este contexto, Cassandra resulta una elección adecuada porque permite operar en entornos distribuidos con tolerancia a fallos, aceptando consistencia eventual cuando sea necesario.

## Implicaciones
- El sistema sigue recibiendo y almacenando eventos aunque existan fallos parciales.
- Se privilegia la continuidad operativa del stream.
- La consistencia fuerte no es el objetivo principal en la capa de ingesta.
- La consolidación y análisis más estructurado se realizará después en la capa analítica usando Spark.

## Conclusión
La decisión de priorizar AP se alinea con la naturaleza distribuida y en tiempo real del proyecto.