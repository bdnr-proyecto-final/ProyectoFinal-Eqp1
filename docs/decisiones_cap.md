# Decisiones basadas en el teorema CAP

## Prioridad elegida
La arquitectura prioriza conceptualmente:

- Disponibilidad (Availability)
- Tolerancia a particiones (Partition Tolerance)

## Justificación
Dado que el sistema recibe eventos en tiempo real desde una fuente externa, el diseño se orienta a mantener la continuidad de la ingesta y a procesar posteriormente los datos en la capa analítica.

En este contexto, Cassandra resulta una elección adecuada como base conceptual porque favorece escrituras simples, escalabilidad horizontal y consistencia eventual cuando el caso de uso lo permite.

## Implicaciones
- Kafka desacopla la recepción del stream del almacenamiento operativo.
- Cassandra concentra la persistencia de eventos del pipeline de ingesta.
- La consistencia fuerte no es el objetivo principal en esta solución académica.
- Spark consolida el análisis posteriormente sobre datos ya persistidos.

## Alcance y limitaciones
Esta decisión CAP debe leerse como una justificación académica del diseño, no como una garantía empírica del entorno actual.

Aunque Cassandra es una tecnología asociada frecuentemente a escenarios AP, el repositorio corre actualmente en un solo nodo local. Por ello:

- no se demuestra alta disponibilidad real
- no se valida tolerancia a particiones entre nodos
- no existe replicación multinodo efectiva en ejecución
- el entorno sirve para reproducir el flujo y argumentar el diseño, no para probar comportamiento de producción distribuida

## Conclusión
La decisión de priorizar AP se alinea con la naturaleza del problema y con la elección tecnológica del proyecto. Sin embargo, en este repositorio esa prioridad debe entenderse como criterio de diseño y no como una propiedad operativa plenamente demostrada del despliegue local.