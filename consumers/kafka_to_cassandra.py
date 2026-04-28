

import json
import os
import time
import uuid
from datetime import datetime, timezone

from cassandra.cluster import Cluster
from kafka import KafkaConsumer


KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "wikimedia.recentchange")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = os.getenv("KEYSPACE", "wikimedia")
TABLE_NAME = os.getenv("TABLE_NAME", "recent_changes")


INSERT_QUERY = f"""
INSERT INTO {KEYSPACE}.{TABLE_NAME} (
    id,
    title,
    user_name,
    type,
    wiki,
    bot,
    server_name,
    comment,
    timestamp_event,
    parsed_date
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


SELECT_COUNT_QUERY = f"SELECT COUNT(*) FROM {KEYSPACE}.{TABLE_NAME};"


def parse_timestamp(value):
    """Convierte el timestamp del evento a datetime UTC."""
    if value is None:
        return None

    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except (TypeError, ValueError):
        return None


def connect_to_cassandra(max_retries=10, wait_seconds=5):
    """Intenta conectarse a Cassandra con reintentos."""
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)

    for attempt in range(1, max_retries + 1):
        try:
            session = cluster.connect(KEYSPACE)
            print(f"[CASSANDRA] Conectado a keyspace '{KEYSPACE}'.")
            return cluster, session
        except Exception as exc:
            print(
                f"[CASSANDRA] Intento {attempt}/{max_retries} falló: {exc}. "
                f"Reintentando en {wait_seconds} segundos..."
            )
            time.sleep(wait_seconds)

    raise ConnectionError("No fue posible conectarse a Cassandra.")


def create_kafka_consumer():
    """Crea el consumidor de Kafka."""
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="wikimedia-cassandra-consumer",
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    print(f"[KAFKA] Escuchando topic '{TOPIC_NAME}' en {KAFKA_HOST}:{KAFKA_PORT}...")
    return consumer


def insert_event(session, event):
    """Inserta un evento de Kafka en Cassandra."""
    event_id = uuid.uuid4()
    title = event.get("title")
    user_name = event.get("user")
    change_type = event.get("type")
    wiki = event.get("wiki")
    bot = bool(event.get("bot", False))
    server_name = event.get("server_name")
    comment = event.get("comment")
    timestamp_event = parse_timestamp(event.get("timestamp"))
    parsed_date = (
        timestamp_event.strftime("%Y-%m-%d %H:%M:%S") if timestamp_event else None
    )

    session.execute(
        INSERT_QUERY,
        (
            event_id,
            title,
            user_name,
            change_type,
            wiki,
            bot,
            server_name,
            comment,
            timestamp_event,
            parsed_date,
        ),
    )

    print(
        f"[INSERT] Evento guardado | title={title!r} | user={user_name!r} | "
        f"type={change_type!r} | wiki={wiki!r}"
    )


def print_total_rows(session):
    """Imprime cuántos registros hay actualmente en la tabla."""
    try:
        result = session.execute(SELECT_COUNT_QUERY)
        total = result.one()[0]
        print(f"[CASSANDRA] Total de registros en {TABLE_NAME}: {total}")
    except Exception as exc:
        print(f"[CASSANDRA] No se pudo obtener el conteo de registros: {exc}")


def main():
    cluster = None
    consumer = None

    try:
        cluster, session = connect_to_cassandra()
        consumer = create_kafka_consumer()

        print("[PIPELINE] Iniciando consumo de Kafka hacia Cassandra...")

        while True:
            has_messages = False

            for message in consumer:
                has_messages = True
                event = message.value
                insert_event(session, event)

            if not has_messages:
                print("[KAFKA] Esperando nuevos mensajes...")
                time.sleep(2)
                print_total_rows(session)

    except KeyboardInterrupt:
        print("\n[PIPELINE] Proceso detenido manualmente.")
    except Exception as exc:
        print(f"[ERROR] Ocurrió un error en el pipeline Kafka → Cassandra: {exc}")
    finally:
        if consumer is not None:
            consumer.close()
            print("[KAFKA] Consumidor cerrado.")
        if cluster is not None:
            cluster.shutdown()
            print("[CASSANDRA] Conexión cerrada.")


if __name__ == "__main__":
    main()